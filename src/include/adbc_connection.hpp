#pragma once

#include "duckdb.hpp"
#include "adbc_utils.hpp"
#include "adbc_options.hpp"
#include <arrow-adbc/adbc.h>
#include <arrow-adbc/adbc_driver_manager.h>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <atomic>

namespace adbc_scanner {
using namespace duckdb;

// Forward declaration
class AdbcConnectionWrapper;

// DuckDB can initialize and destroy execution state on different threads. A
// lease must therefore not hold a thread-owned mutex across those callbacks.
class AdbcOperationLease {
public:
    explicit AdbcOperationLease(std::atomic<bool> &active) : active(active) {
        if (active.exchange(true)) {
            throw InvalidInputException("ADBC connection already has an active operation; to run two adbc_* calls on one database at once, ATTACH it a second time");
        }
    }
    ~AdbcOperationLease() { active.store(false); }
    AdbcOperationLease(const AdbcOperationLease &) = delete;
    AdbcOperationLease &operator=(const AdbcOperationLease &) = delete;
private:
    std::atomic<bool> &active;
};

// Metadata streams retain exclusive use of the connection until released,
// including on errors while reading the stream.
struct AdbcMetadataStream {
    explicit AdbcMetadataStream(unique_ptr<AdbcOperationLease> lease) : lease(std::move(lease)) {}
    ~AdbcMetadataStream() {
        if (stream.release) {
            stream.release(&stream);
        }
    }
    ArrowArrayStream stream {};
    unique_ptr<AdbcOperationLease> lease;

    static AdbcMetadataStream &Get(ArrowArrayStream *out) {
        return *static_cast<AdbcMetadataStream *>(out->private_data);
    }
    static void Export(unique_ptr<AdbcMetadataStream> state, ArrowArrayStream *out) {
        out->get_schema = [](ArrowArrayStream *out, ArrowSchema *schema) {
            auto &stream = Get(out).stream;
            return stream.get_schema(&stream, schema);
        };
        out->get_next = [](ArrowArrayStream *out, ArrowArray *array) {
            auto &stream = Get(out).stream;
            return stream.get_next(&stream, array);
        };
        out->get_last_error = [](ArrowArrayStream *out) -> const char * {
            auto &stream = Get(out).stream;
            return stream.get_last_error ? stream.get_last_error(&stream) : nullptr;
        };
        out->release = [](ArrowArrayStream *out) {
            delete static_cast<AdbcMetadataStream *>(out->private_data);
            out->private_data = nullptr;
            out->release = nullptr;
        };
        out->private_data = state.release();
    }
};

// RAII wrapper for AdbcDatabase
class AdbcDatabaseWrapper {
public:
    AdbcDatabaseWrapper() : allocated(false), initialized(false) {
        memset(&database, 0, sizeof(database));
    }

    // Set driver name for error messages (call before Initialize)
    void SetDriverName(const string &driver_path) {
        driver_name = ExtractDriverName(driver_path);
    }

    const string &GetDriverName() const {
        return driver_name;
    }

    ~AdbcDatabaseWrapper() {
        Release();
    }

    void Init() {
        if (allocated) {
            return;
        }
        AdbcErrorGuard error;
        auto status = AdbcDatabaseNew(&database, error.Get());
        CheckAdbc(status, error.Get(), "Failed to create ADBC database");
        allocated = true;
    }

    void SetOption(const string &key, const string &value) {
        AdbcErrorGuard error;
        auto status = AdbcDatabaseSetOption(&database, key.c_str(), value.c_str(), error.Get());
        CheckAdbc(status, error.Get(), "Failed to set database option '" + key + "'", driver_name);
    }

    void SetOption(const string &key, const Value &value) {
        AdbcErrorGuard error;
        AdbcStatusCode status;
        auto type = value.type().id();
        if (type == LogicalTypeId::VARCHAR || type == LogicalTypeId::BOOLEAN) {
            SetOption(key, value.ToString());
            return;
        } else if (type == LogicalTypeId::BLOB) {
            auto &bytes = StringValue::Get(value);
            status = AdbcDatabaseSetOptionBytes(&database, key.c_str(),
                reinterpret_cast<const uint8_t *>(bytes.data()), bytes.size(), error.Get());
        } else if (value.type().IsIntegral()) {
            status = AdbcDatabaseSetOptionInt(&database, key.c_str(), value.GetValue<int64_t>(), error.Get());
        } else if (type == LogicalTypeId::FLOAT || type == LogicalTypeId::DOUBLE) {
            status = AdbcDatabaseSetOptionDouble(&database, key.c_str(), value.GetValue<double>(), error.Get());
        } else {
            throw InvalidInputException("Unsupported ADBC option type: " + value.type().ToString());
        }
        CheckAdbc(status, error.Get(), "Failed to set typed database option", driver_name);
    }

    // Set load flags for manifest-based driver discovery
    // Default enables all search paths (env, user, system) and relative paths
    void SetLoadFlags(AdbcLoadFlags flags = ADBC_LOAD_FLAG_DEFAULT) {
        AdbcErrorGuard error;
        auto status = AdbcDriverManagerDatabaseSetLoadFlags(&database, flags, error.Get());
        CheckAdbc(status, error.Get(), "Failed to set load flags", driver_name);
    }

    // Set additional search paths for driver manifests (colon-separated on Unix, semicolon on Windows)
    void SetAdditionalSearchPaths(const string &path_list) {
        AdbcErrorGuard error;
        auto status = AdbcDriverManagerDatabaseSetAdditionalSearchPathList(
            &database, path_list.c_str(), error.Get());
        CheckAdbc(status, error.Get(), "Failed to set additional search paths", driver_name);
    }

    void Initialize() {
        AdbcErrorGuard error;
        auto status = AdbcDatabaseInit(&database, error.Get());
        CheckAdbc(status, error.Get(), "Failed to initialize ADBC database", driver_name);
        initialized = true;
    }

    void Release() {
        if (allocated) {
            AdbcErrorGuard error;
            AdbcDatabaseRelease(&database, error.Get());
            allocated = false;
            initialized = false;
        }
    }

    AdbcDatabase *Get() {
        return &database;
    }

    bool IsInitialized() const {
        return initialized;
    }

    // Non-copyable
    AdbcDatabaseWrapper(const AdbcDatabaseWrapper &) = delete;
    AdbcDatabaseWrapper &operator=(const AdbcDatabaseWrapper &) = delete;

    // Movable
    AdbcDatabaseWrapper(AdbcDatabaseWrapper &&other) noexcept
        : database(other.database), allocated(other.allocated), initialized(other.initialized) {
        other.allocated = false;
        other.initialized = false;
        memset(&other.database, 0, sizeof(other.database));
    }

private:
    AdbcDatabase database;
    bool allocated;
    bool initialized;
    string driver_name;
};

// RAII wrapper for AdbcConnection
class AdbcConnectionWrapper {
public:
    AdbcConnectionWrapper(shared_ptr<AdbcDatabaseWrapper> db) : database(std::move(db)), allocated(false), initialized(false) {
        memset(&connection, 0, sizeof(connection));
    }

    ~AdbcConnectionWrapper() {
        Release();
    }

    void Init() {
        AdbcErrorGuard error;
        auto status = AdbcConnectionNew(&connection, error.Get());
        CheckAdbc(status, error.Get(), "Failed to create ADBC connection", GetDriverName());
        allocated = true;
    }

    void SetOption(const string &key, const string &value) {
        AdbcErrorGuard error;
        auto status = AdbcConnectionSetOption(&connection, key.c_str(), value.c_str(), error.Get());
        CheckAdbc(status, error.Get(), "Failed to set connection option '" + key + "'", GetDriverName());
    }

    // Read a string connection option (ADBC 1.1.0+). Returns false when the driver
    // does not report it (unknown option, pre-1.1 driver, or any other failure).
    bool TryGetOption(const string &key, string &value) {
        auto lock = AcquireOperation();
        AdbcErrorGuard error;
        char buffer[1024];
        size_t length = sizeof(buffer);
        auto status = AdbcConnectionGetOption(&connection, key.c_str(), buffer, &length, error.Get());
        if (status != ADBC_STATUS_OK || length == 0) {
            return false;
        }
        if (length <= sizeof(buffer)) {
            // length includes the null terminator
            value = string(buffer, length - 1);
            return true;
        }
        string large(length, '\0');
        status = AdbcConnectionGetOption(&connection, key.c_str(), &large[0], &length, error.Get());
        if (status != ADBC_STATUS_OK || length == 0 || length > large.size()) {
            return false;
        }
        value = large.substr(0, length - 1);
        return true;
    }

    void Initialize() {
        AdbcErrorGuard error;
        auto status = AdbcConnectionInit(&connection, database->Get(), error.Get());
        CheckAdbc(status, error.Get(), "Failed to initialize ADBC connection", GetDriverName());
        initialized = true;
    }

    void Release() {
        if (allocated) {
            AdbcErrorGuard error;
            AdbcConnectionRelease(&connection, error.Get());
            allocated = false;
            initialized = false;
        }
    }

    AdbcConnection *Get() {
        return &connection;
    }

    bool IsInitialized() const {
        return initialized;
    }

    shared_ptr<AdbcDatabaseWrapper> GetDatabase() {
        return database;
    }

    const string &GetDriverName() const {
        return database->GetDriverName();
    }

    // Get connection info (vendor name, driver version, etc.)
    // info_codes can be NULL to get all info, or an array of specific codes
    void GetInfo(const uint32_t *info_codes, size_t info_codes_length, ArrowArrayStream *out) {
        auto state = make_uniq<AdbcMetadataStream>(AcquireOperation());
        AdbcErrorGuard error;
        auto status = AdbcConnectionGetInfo(&connection, info_codes, info_codes_length, &state->stream, error.Get());
        CheckAdbc(status, error.Get(), "Failed to get connection info", GetDriverName());
        AdbcMetadataStream::Export(std::move(state), out);
    }

    // Get database objects (catalogs, schemas, tables, columns)
    // depth: 0=all, 1=catalogs, 2=schemas, 3=tables
    // Other parameters can be NULL for no filtering, or search patterns
    void GetObjects(int depth, const char *catalog, const char *db_schema,
                    const char *table_name, const char **table_types,
                    const char *column_name, ArrowArrayStream *out) {
        auto state = make_uniq<AdbcMetadataStream>(AcquireOperation());
        AdbcErrorGuard error;
        auto status = AdbcConnectionGetObjects(&connection, depth, catalog, db_schema,
                                                table_name, table_types, column_name, &state->stream, error.Get());
        CheckAdbc(status, error.Get(), "Failed to get database objects", GetDriverName());
        AdbcMetadataStream::Export(std::move(state), out);
    }

    // Get table types (e.g., "TABLE", "VIEW", etc.)
    void GetTableTypes(ArrowArrayStream *out) {
        auto state = make_uniq<AdbcMetadataStream>(AcquireOperation());
        AdbcErrorGuard error;
        auto status = AdbcConnectionGetTableTypes(&connection, &state->stream, error.Get());
        CheckAdbc(status, error.Get(), "Failed to get table types", GetDriverName());
        AdbcMetadataStream::Export(std::move(state), out);
    }

    // Get the Arrow schema for a specific table
    void GetTableSchema(const char *catalog, const char *db_schema,
                        const char *table_name, ArrowSchema *schema) {
        auto lease = AcquireOperation();
        AdbcErrorGuard error;
        auto status = AdbcConnectionGetTableSchema(&connection, catalog, db_schema,
                                                    table_name, schema, error.Get());
        CheckAdbc(status, error.Get(), "Failed to get table schema", GetDriverName());
    }

    // Get table statistics (ADBC 1.1.0+)
    // Returns true if successful, false if not supported by driver
    // approximate: if non-zero, allow approximate/cached values
    bool GetStatistics(const char *catalog, const char *db_schema, const char *table_name,
                       char approximate, ArrowArrayStream *out) {
        auto state = make_uniq<AdbcMetadataStream>(AcquireOperation());
        AdbcErrorGuard error;
        auto status = AdbcConnectionGetStatistics(&connection, catalog, db_schema, table_name,
                                                   approximate, &state->stream, error.Get());
        if (status == ADBC_STATUS_NOT_IMPLEMENTED) {
            return false;
        }
        CheckAdbc(status, error.Get(), "Failed to get table statistics", GetDriverName());
        AdbcMetadataStream::Export(std::move(state), out);
        return true;
    }

    unique_ptr<AdbcOperationLease> AcquireOperation() {
        auto lease = make_uniq<AdbcOperationLease>(operation_active);
        if (!initialized) {
            throw InvalidInputException("ADBC connection is closed");
        }
        return lease;
    }

    void Close() {
        auto lock = AcquireOperation();
        Release();
    }

    // Transaction support
    void Commit() {
        auto lock = AcquireOperation();
        AdbcErrorGuard error;
        auto status = AdbcConnectionCommit(&connection, error.Get());
        CheckAdbc(status, error.Get(), "Failed to commit transaction", GetDriverName());
    }

    void Rollback() {
        auto lock = AcquireOperation();
        AdbcErrorGuard error;
        auto status = AdbcConnectionRollback(&connection, error.Get());
        CheckAdbc(status, error.Get(), "Failed to rollback transaction", GetDriverName());
    }

    void SetAutocommit(bool enabled) {
        auto lock = AcquireOperation();
        AdbcErrorGuard error;
        const char *value = enabled ? ADBC_OPTION_VALUE_ENABLED : ADBC_OPTION_VALUE_DISABLED;
        auto status = AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT, value, error.Get());
        CheckAdbc(status, error.Get(), "Failed to set autocommit", GetDriverName());
    }

    // Non-copyable
    AdbcConnectionWrapper(const AdbcConnectionWrapper &) = delete;
    AdbcConnectionWrapper &operator=(const AdbcConnectionWrapper &) = delete;

private:
    shared_ptr<AdbcDatabaseWrapper> database;
    AdbcConnection connection;
    bool allocated;
    bool initialized;
    std::atomic<bool> operation_active {false};
};

// RAII wrapper for AdbcStatement
class AdbcStatementWrapper {
public:
    AdbcStatementWrapper(shared_ptr<AdbcConnectionWrapper> conn) : connection(std::move(conn)), initialized(false), operation(this->connection->AcquireOperation()) {
        memset(&statement, 0, sizeof(statement));
    }

    ~AdbcStatementWrapper() {
        Release();
    }

    void Init() {
        AdbcErrorGuard error;
        auto status = AdbcStatementNew(connection->Get(), &statement, error.Get());
        CheckAdbc(status, error.Get(), "Failed to create ADBC statement", GetDriverName());
        initialized = true;
    }

    void SetSqlQuery(const string &query) {
        AdbcErrorGuard error;
        auto status = AdbcStatementSetSqlQuery(&statement, query.c_str(), error.Get());
        CheckAdbc(status, error.Get(), "Failed to set SQL query", GetDriverName());
    }

    void SetOption(const string &key, const string &value) {
        AdbcErrorGuard error;
        auto status = AdbcStatementSetOption(&statement, key.c_str(), value.c_str(), error.Get());
        CheckAdbc(status, error.Get(), "Failed to set statement option '" + key + "'", GetDriverName());
    }

    void Prepare() {
        AdbcErrorGuard error;
        auto status = AdbcStatementPrepare(&statement, error.Get());
        CheckAdbc(status, error.Get(), "Failed to prepare statement", GetDriverName());
    }

    // Bind parameters to the statement (Arrow format)
    // The statement takes ownership of the values/schema
    void Bind(ArrowArray *values, ArrowSchema *schema) {
        AdbcErrorGuard error;
        auto status = AdbcStatementBind(&statement, values, schema, error.Get());
        CheckAdbc(status, error.Get(), "Failed to bind parameters", GetDriverName());
    }

    // Bind an Arrow stream for bulk ingestion
    // The statement takes ownership of the stream
    void BindStream(ArrowArrayStream *stream) {
        AdbcErrorGuard error;
        auto status = AdbcStatementBindStream(&statement, stream, error.Get());
        CheckAdbc(status, error.Get(), "Failed to bind stream", GetDriverName());
    }

    // Get the result schema without executing the query (requires Prepare first)
    // Returns true if successful, false if not supported by driver
    bool ExecuteSchema(ArrowSchema *schema) {
        AdbcErrorGuard error;
        auto status = AdbcStatementExecuteSchema(&statement, schema, error.Get());
        if (status == ADBC_STATUS_NOT_IMPLEMENTED) {
            return false;
        }
        CheckAdbc(status, error.Get(), "Failed to get result schema", GetDriverName());
        return true;
    }

    // Execute and return the ArrowArrayStream - caller takes ownership
    void ExecuteQuery(ArrowArrayStream *out, int64_t *rows_affected = nullptr) {
        AdbcErrorGuard error;
        auto status = AdbcStatementExecuteQuery(&statement, out, rows_affected, error.Get());
        CheckAdbc(status, error.Get(), "Failed to execute query", GetDriverName());
    }

    // Execute without expecting a result set (for bulk ingestion)
    void ExecuteUpdate(int64_t *rows_affected = nullptr) {
        AdbcErrorGuard error;
        auto status = AdbcStatementExecuteQuery(&statement, nullptr, rows_affected, error.Get());
        CheckAdbc(status, error.Get(), "Failed to execute update", GetDriverName());
    }

    // Cancel any in-progress query (best effort - ignores errors)
    void Cancel() {
        if (initialized) {
            AdbcErrorGuard error;
            AdbcStatementCancel(&statement, error.Get());
            // Ignore errors - cancel is best effort
        }
    }

    void Release() {
        if (initialized) {
            AdbcErrorGuard error;
            AdbcStatementRelease(&statement, error.Get());
            initialized = false;
        }
    }

    AdbcStatement *Get() {
        return &statement;
    }

    shared_ptr<AdbcConnectionWrapper> GetConnection() {
        return connection;
    }

    const string &GetDriverName() const {
        return connection->GetDriverName();
    }

    // Non-copyable
    AdbcStatementWrapper(const AdbcStatementWrapper &) = delete;
    AdbcStatementWrapper &operator=(const AdbcStatementWrapper &) = delete;

private:
    shared_ptr<AdbcConnectionWrapper> connection;
    AdbcStatement statement;
    bool initialized;
    unique_ptr<AdbcOperationLease> operation;
};

// Helper to create a connection from a vector of options
// Extracts driver, entrypoint, uri, search_paths, use_manifests and configures the connection
// Returns the initialized connection wrapper
shared_ptr<AdbcConnectionWrapper> CreateConnectionFromOptions(const AdbcOptions &options);

// The connection behind an attached ADBC database, named by its ATTACH alias
// (`ATTACH '…' AS pg (TYPE adbc, …)` → "pg"). Inside an explicit transaction,
// writes (and reads after the transaction has written) use the transaction's
// write connection, so they commit or roll back together with writes made
// through the catalog; otherwise the attachment's own connection, in
// autocommit. Writes to a read-only attachment are rejected. function_name
// prefixes error messages.
shared_ptr<AdbcConnectionWrapper> GetAttachedConnection(ClientContext &context, const Value &database,
                                                        const string &function_name, bool write);

// Helper to iterate over batches in an ArrowArrayStream
// Calls the callback for each batch, automatically handles errors and cleanup
// callback should return true to continue, false to stop early
// function_name is used in error messages
template <typename Callback>
void ForEachArrowBatch(ArrowArrayStream &stream, const string &function_name, Callback callback) {
	ArrowArray batch;
	while (true) {
		memset(&batch, 0, sizeof(batch));
		int ret = stream.get_next(&stream, &batch);
		if (ret != 0) {
			const char *error_msg = stream.get_last_error(&stream);
			string msg = function_name + ": Failed to get next batch";
			if (error_msg) {
				msg += ": ";
				msg += error_msg;
			}
			throw IOException(msg);
		}

		if (!batch.release) {
			break; // End of stream
		}

		bool should_continue = callback(&batch);

		if (batch.release) {
			batch.release(&batch);
		}

		if (!should_continue) {
			break;
		}
	}
}

} // namespace adbc_scanner
