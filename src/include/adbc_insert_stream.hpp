#pragma once

// Shared streaming-insert machinery used by both the `adbc_insert` table
// function (src/adbc_insert.cpp) and the storage/ATTACH write operator
// (src/storage/adbc_insert.cpp).
//
// The core problem both paths solve: stream DuckDB DataChunks into an ADBC
// driver's bulk-ingest call (AdbcStatementExecuteUpdate) without buffering the
// entire source result set in memory. The driver pulls Arrow batches via
// ArrowArrayStream::get_next from inside ExecuteUpdate, which we run on a
// dedicated consumer thread, while the DuckDB engine pushes batches from the
// producer side. A bounded queue with two condition variables provides
// backpressure in both directions so resident memory stays flat regardless of
// input size.

#include "adbc_connection.hpp"
#include <nanoarrow/nanoarrow.h>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <cstdlib>

namespace adbc_scanner {
using namespace duckdb;

// Default upper bound on the number of Arrow record batches buffered between the
// DuckDB producer and the ADBC driver consumer. Each DuckDB DataChunk is at most
// STANDARD_VECTOR_SIZE (2048) rows, so this caps resident memory at roughly
// N * 2048 * row_width rather than the full source size. Overridable via
// ADBC_INSERT_MAX_PENDING_BATCHES.
static constexpr size_t DEFAULT_MAX_PENDING_BATCHES = 32;

inline size_t ResolveMaxPendingBatches(int64_t override_value) {
    // Per-call named parameter wins, then the env override, then the default.
    if (override_value > 0) {
        return static_cast<size_t>(override_value);
    }
    const char *env = std::getenv("ADBC_INSERT_MAX_PENDING_BATCHES");
    if (env && *env) {
        char *end = nullptr;
        long v = std::strtol(env, &end, 10);
        if (end != env && v > 0) {
            return static_cast<size_t>(v);
        }
    }
    return DEFAULT_MAX_PENDING_BATCHES;
}

// Bounded, blocking ArrowArrayStream bridging the DuckDB producer and the ADBC
// driver consumer.
//
// The driver pulls batches via GetNext from inside AdbcStatement::ExecuteUpdate,
// which runs on a dedicated thread. The DuckDB execution engine pushes batches
// via AddBatch from the (single) producer. A bounded queue with two condition
// variables provides backpressure in both directions:
//   - AddBatch blocks when the queue is full until the consumer drains one
//     (this is what keeps RSS flat — DuckDB stops decoding ahead of the driver).
//   - GetNext blocks when the queue is empty until the producer pushes one or
//     signals completion.
// Abort()/consumer-stop signalling prevents either side from deadlocking when
// the other fails or the query is cancelled.
struct AdbcInsertStream {
    ArrowArrayStream stream;
    ArrowSchema schema;
    bool schema_set = false;

    std::mutex lock;
    std::condition_variable cv_not_empty;  // consumer waits for a batch
    std::condition_variable cv_not_full;   // producer waits for free space
    std::queue<ArrowArray> pending_batches;
    size_t max_batches;

    bool finished = false;          // producer: no more batches will arrive
    bool aborted = false;           // hard stop: consumer should error out
    bool consumer_stopped = false;  // consumer thread has exited (ok or error)
    string consumer_error;          // error message from the consumer side
    string last_error;              // surfaced to the driver via get_last_error

    explicit AdbcInsertStream(size_t max_batches_p) : max_batches(max_batches_p) {
        memset(&stream, 0, sizeof(stream));
        memset(&schema, 0, sizeof(schema));
        stream.private_data = this;
        stream.get_schema = GetSchema;
        stream.get_next = GetNext;
        stream.get_last_error = GetLastError;
        stream.release = Release;
    }

    ~AdbcInsertStream() {
        if (schema.release) {
            schema.release(&schema);
        }
        while (!pending_batches.empty()) {
            auto &batch = pending_batches.front();
            if (batch.release) {
                batch.release(&batch);
            }
            pending_batches.pop();
        }
    }

    void SetSchema(ArrowSchema *new_schema) {
        std::lock_guard<std::mutex> l(lock);
        if (schema.release) {
            schema.release(&schema);
        }
        schema = *new_schema;
        memset(new_schema, 0, sizeof(*new_schema));  // Transfer ownership
        schema_set = true;
    }

    // Producer side. Blocks while the queue is full to apply backpressure.
    // Returns false if the consumer has stopped/aborted and the batch could not
    // be handed off (the batch is released in that case).
    bool AddBatch(ArrowArray *batch) {
        std::unique_lock<std::mutex> l(lock);
        cv_not_full.wait(l, [&] {
            return pending_batches.size() < max_batches || consumer_stopped || aborted;
        });
        if (consumer_stopped || aborted) {
            if (batch->release) {
                batch->release(batch);
            }
            memset(batch, 0, sizeof(*batch));
            return false;
        }
        pending_batches.push(*batch);
        memset(batch, 0, sizeof(*batch));  // Transfer ownership
        cv_not_empty.notify_one();
        return true;
    }

    // Producer side: no more batches will be produced.
    void Finish() {
        std::lock_guard<std::mutex> l(lock);
        finished = true;
        cv_not_empty.notify_all();
    }

    // Hard abort (query cancelled or producer errored). Wakes both sides; the
    // next GetNext returns an error so ExecuteUpdate unwinds without ingesting a
    // partial result.
    void Abort(const string &reason) {
        std::lock_guard<std::mutex> l(lock);
        aborted = true;
        if (last_error.empty()) {
            last_error = reason;
        }
        cv_not_empty.notify_all();
        cv_not_full.notify_all();
    }

    // Consumer side: record that the ExecuteUpdate thread has exited so a blocked
    // producer can stop waiting.
    void MarkConsumerStopped(const string &error) {
        std::lock_guard<std::mutex> l(lock);
        consumer_stopped = true;
        if (!error.empty()) {
            consumer_error = error;
        }
        cv_not_full.notify_all();
    }

    string GetConsumerError() {
        std::lock_guard<std::mutex> l(lock);
        return consumer_error;
    }

    static int GetSchema(ArrowArrayStream *stream, ArrowSchema *out) {
        auto *self = static_cast<AdbcInsertStream *>(stream->private_data);
        std::lock_guard<std::mutex> l(self->lock);
        if (!self->schema_set) {
            self->last_error = "Schema not set";
            return EINVAL;
        }
        return ArrowSchemaDeepCopy(&self->schema, out);
    }

    static int GetNext(ArrowArrayStream *stream, ArrowArray *out) {
        auto *self = static_cast<AdbcInsertStream *>(stream->private_data);
        std::unique_lock<std::mutex> l(self->lock);
        self->cv_not_empty.wait(l, [&] {
            return !self->pending_batches.empty() || self->finished || self->aborted;
        });

        if (self->aborted && self->pending_batches.empty()) {
            self->last_error = "adbc_insert: ingestion aborted";
            return EIO;
        }

        if (self->pending_batches.empty()) {
            // finished and fully drained → end of stream
            memset(out, 0, sizeof(*out));
            return 0;
        }

        *out = self->pending_batches.front();
        self->pending_batches.pop();
        self->cv_not_full.notify_one();
        return 0;
    }

    static const char *GetLastError(ArrowArrayStream *stream) {
        auto *self = static_cast<AdbcInsertStream *>(stream->private_data);
        return self->last_error.empty() ? nullptr : self->last_error.c_str();
    }

    static void Release(ArrowArrayStream *stream) {
        // Lifetime managed externally (by the owning global state).
        stream->release = nullptr;
    }
};

// Owns the consumer side of a streaming insert: the bound ADBC statement, the
// bounded stream, and the background thread running ExecuteUpdate. Embed this in
// a DuckDB global state (table function or storage sink). The destructor aborts
// and joins so a partially-consumed stream can never leak the consumer thread.
struct AdbcStreamingInsertConsumer {
    shared_ptr<AdbcStatementWrapper> statement;
    unique_ptr<AdbcInsertStream> insert_stream;
    bool stream_bound = false;

    std::thread exec_thread;
    bool exec_ok = false;
    string exec_error;
    int64_t exec_rows_affected = -1;

    explicit AdbcStreamingInsertConsumer(size_t max_batches) {
        insert_stream = make_uniq<AdbcInsertStream>(max_batches);
    }

    ~AdbcStreamingInsertConsumer() {
        // Abnormal teardown (producer threw, query cancelled): make sure the
        // consumer thread can never block forever, then join it.
        if (insert_stream && exec_thread.joinable()) {
            insert_stream->Abort("adbc_insert: aborted before completion");
        }
        JoinConsumer();
    }

    void SetSchema(ArrowSchema *schema) {
        insert_stream->SetSchema(schema);
    }

    // Bind the stream to the statement (stores the stream; does not consume yet).
    void BindStream() {
        statement->BindStream(&insert_stream->stream);
        stream_bound = true;
    }

    // Start draining concurrently: ExecuteUpdate runs on its own thread and pulls
    // batches from the bound stream as the producer pushes them. Without this
    // overlap the queue would have to hold the entire source before ExecuteUpdate
    // ran (the OOM failure mode).
    void StartConsumer() {
        exec_thread = std::thread([this]() {
            try {
                statement->ExecuteUpdate(&exec_rows_affected);
                exec_ok = true;
                insert_stream->MarkConsumerStopped(string());
            } catch (std::exception &e) {
                exec_ok = false;
                exec_error = e.what();
                insert_stream->MarkConsumerStopped(exec_error);
            } catch (...) {
                exec_ok = false;
                exec_error = "unknown error during ExecuteUpdate";
                insert_stream->MarkConsumerStopped(exec_error);
            }
        });
    }

    // Producer side: hand a batch to the consumer (blocks for backpressure).
    // Returns false if the consumer already stopped (error/cancellation).
    bool AddBatch(ArrowArray *batch) {
        return insert_stream->AddBatch(batch);
    }

    string GetConsumerError() {
        return insert_stream->GetConsumerError();
    }

    void JoinConsumer() {
        if (exec_thread.joinable()) {
            exec_thread.join();
        }
    }

    // Signal end of input and wait for ExecuteUpdate to finish draining. Throws
    // an IOException if the consumer reported an error.
    void FinishAndJoin() {
        insert_stream->Finish();
        JoinConsumer();
        if (stream_bound && !exec_ok) {
            throw IOException("adbc_insert: Failed to execute insert: " + exec_error);
        }
    }
};

} // namespace adbc_scanner
