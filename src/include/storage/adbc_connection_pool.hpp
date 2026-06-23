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

// RAII lease over a connection checked out from an AdbcConnectionPool.
//
// ADBC connections are not safe for concurrent statement execution, so each
// concurrent operation (scan, catalog introspection, write) must use its own
// connection. A lease hands out one connection for the duration of an operation
// and returns it to the pool's idle list on destruction so it can be reused.
// Move-only; an empty/default-constructed lease owns nothing.
class AdbcPoolConnection {
public:
	AdbcPoolConnection() : pool(nullptr), pooled(false) {
	}
	AdbcPoolConnection(AdbcConnectionPool *pool, shared_ptr<AdbcConnectionWrapper> connection, bool pooled);
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
		std::swap(pooled, other.pooled);
	}

	AdbcConnectionPool *pool;
	shared_ptr<AdbcConnectionWrapper> connection;
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

	// Lease a connection for an operation. Reuses an idle connection if available,
	// otherwise opens a new one. If the pool is at capacity, returns an ephemeral
	// (non-pooled) connection so callers never block or fail.
	AdbcPoolConnection GetConnection();

	// Lease a connection whose lifetime follows a shared_ptr rather than a scoped
	// RAII object. When the last returned reference drops, the connection is
	// returned to the pool automatically. Used by the scan path, where the leased
	// connection must outlive the bind call and stay alive for the whole query
	// (held by the scan's bind data) — see AdbcTableEntry::GetScanFunction.
	shared_ptr<AdbcConnectionWrapper> GetConnectionShared();

	// Return a pooled connection to the idle list. Called by the lease destructor.
	void ReturnConnection(shared_ptr<AdbcConnectionWrapper> connection);

	void SetMaximumConnections(idx_t new_max);

	shared_ptr<AdbcDatabaseWrapper> GetDatabase() const {
		return database;
	}

private:
	// Acquire a connection (reuse idle, open new, or ephemeral if exhausted).
	// Sets pooled_out to whether it should be returned to the pool.
	shared_ptr<AdbcConnectionWrapper> AcquireRaw(bool &pooled_out);

	// Create and initialize a fresh connection from the shared database.
	shared_ptr<AdbcConnectionWrapper> OpenNewConnection();

	shared_ptr<AdbcDatabaseWrapper> database;
	mutex lock;
	idx_t active_connections = 0;
	idx_t maximum_connections;
	vector<shared_ptr<AdbcConnectionWrapper>> idle;
};

} // namespace adbc_scanner
