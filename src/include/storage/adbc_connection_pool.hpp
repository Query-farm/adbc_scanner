//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/adbc_connection_pool.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "adbc_connection.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/vector.hpp"
#include <utility>

namespace adbc_scanner {
using namespace duckdb;

class AdbcConnectionPool;

// What a leased connection will be used for.
//
// Reads and writes must never share a connection. The PostgreSQL driver
// (arrow-adbc 24+) starts transactions lazily: SetOption(autocommit=false) only
// flips a flag, and the BEGIN is emitted later by the driver's EnsureTransaction(),
// which skips it when libpq still reports a command in progress. A COPY-based
// scan leaves exactly that state behind, so a write transaction reusing a
// scan's connection never gets its BEGIN — it silently runs in autocommit and
// ROLLBACK becomes a no-op. Keeping the two roles on disjoint connections is the
// only fix available above the driver, since COPY is the fast path for reads and
// no ADBC call resets the connection.
enum class AdbcConnectionRole { READ, WRITE };

// RAII lease over a connection checked out from an AdbcConnectionPool.
//
// ADBC connections are not safe for concurrent statement execution, so each
// concurrent operation (scan, catalog introspection, write) must use its own
// connection. A lease hands out one connection for the duration of an operation
// and returns it to the pool's idle list on destruction so it can be reused.
// Move-only; an empty/default-constructed lease owns nothing.
class AdbcPoolConnection {
public:
	AdbcPoolConnection() : pool(nullptr), role(AdbcConnectionRole::READ), pooled(false) {
	}
	AdbcPoolConnection(AdbcConnectionPool *pool, shared_ptr<AdbcConnectionWrapper> connection,
	                   AdbcConnectionRole role, bool pooled);
	~AdbcPoolConnection();

	// Non-copyable
	AdbcPoolConnection(const AdbcPoolConnection &) = delete;
	AdbcPoolConnection &operator=(const AdbcPoolConnection &) = delete;
	// Movable
	AdbcPoolConnection(AdbcPoolConnection &&other) noexcept {
		Swap(other);
	}
	AdbcPoolConnection &operator=(AdbcPoolConnection &&other) noexcept {
		Swap(other);
		return *this;
	}

	bool HasConnection() const {
		return connection != nullptr;
	}
	const shared_ptr<AdbcConnectionWrapper> &GetConnection() const {
		return connection;
	}

private:
	void Swap(AdbcPoolConnection &other) noexcept {
		std::swap(pool, other.pool);
		std::swap(connection, other.connection);
		std::swap(role, other.role);
		std::swap(pooled, other.pooled);
	}

	AdbcConnectionPool *pool;
	shared_ptr<AdbcConnectionWrapper> connection;
	// Which idle list this connection returns to; a connection never changes role.
	AdbcConnectionRole role;
	// Whether this connection should be returned to the pool (false for ephemeral
	// connections handed out when the pool is exhausted).
	bool pooled;
};

// Thread-safe pool of ADBC connections sharing a single AdbcDatabase. Modeled on
// duckdb-postgres' PostgresConnectionPool: active_connections counts checked-out
// pooled connections and is balanced by ReturnConnection, so it cannot underflow.
class AdbcConnectionPool {
public:
	static constexpr const idx_t DEFAULT_MAX_CONNECTIONS = 16;

	explicit AdbcConnectionPool(shared_ptr<AdbcDatabaseWrapper> database,
	                            idx_t maximum_connections = DEFAULT_MAX_CONNECTIONS);

	// Lease a connection for an operation. Reuses an idle connection of the same
	// role if available, otherwise opens a new one. If the pool is at capacity,
	// returns an ephemeral (non-pooled) connection so callers never block or fail.
	//
	// The role partitions only the idle lists — READ and WRITE draw on one shared
	// budget, so the total number of open connections is bounded by
	// maximum_connections regardless of the read/write mix.
	AdbcPoolConnection GetConnection(AdbcConnectionRole role = AdbcConnectionRole::READ);

	// Lease a connection whose lifetime follows a shared_ptr rather than a scoped
	// RAII object. When the last returned reference drops, the connection is
	// returned to the pool automatically. Used by the scan path, where the leased
	// connection must outlive the bind call and stay alive for the whole query
	// (held by the scan's bind data) — see AdbcTableEntry::GetScanFunction.
	shared_ptr<AdbcConnectionWrapper> GetConnectionShared();

	// Return a pooled connection to its role's idle list. Called by the lease
	// destructor.
	void ReturnConnection(shared_ptr<AdbcConnectionWrapper> connection, AdbcConnectionRole role);

	void SetMaximumConnections(idx_t new_max);

	shared_ptr<AdbcDatabaseWrapper> GetDatabase() const {
		return database;
	}

private:
	// Acquire a connection (reuse idle of this role, open new, or ephemeral if
	// exhausted). Sets pooled_out to whether it should be returned to the pool.
	shared_ptr<AdbcConnectionWrapper> AcquireRaw(AdbcConnectionRole role, bool &pooled_out);

	// Create and initialize a fresh connection from the shared database.
	shared_ptr<AdbcConnectionWrapper> OpenNewConnection();

	// Idle list for a role. Caller must hold `lock`.
	vector<shared_ptr<AdbcConnectionWrapper>> &IdleFor(AdbcConnectionRole role) {
		return role == AdbcConnectionRole::WRITE ? idle_write : idle_read;
	}

	// Total connections this pool has open, checked out or idle. Caller must hold `lock`.
	idx_t TotalOpen() const {
		return active_connections + idle_read.size() + idle_write.size();
	}

	shared_ptr<AdbcDatabaseWrapper> database;
	mutex lock;
	idx_t active_connections = 0;
	idx_t maximum_connections;
	// Idle connections, partitioned by role so a write never reuses a connection
	// that has served a scan (see AdbcConnectionRole).
	vector<shared_ptr<AdbcConnectionWrapper>> idle_read;
	vector<shared_ptr<AdbcConnectionWrapper>> idle_write;
};

} // namespace adbc_scanner
