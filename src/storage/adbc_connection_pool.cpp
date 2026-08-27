#include "storage/adbc_connection_pool.hpp"

namespace adbc_scanner {
using namespace duckdb;

//===--------------------------------------------------------------------===//
// AdbcPoolConnection (RAII lease)
//===--------------------------------------------------------------------===//

AdbcPoolConnection::AdbcPoolConnection(AdbcConnectionPool *pool_p, shared_ptr<AdbcConnectionWrapper> connection_p,
                                       AdbcConnectionRole role_p, bool pooled_p)
    : pool(pool_p), connection(std::move(connection_p)), role(role_p), pooled(pooled_p) {
}

AdbcPoolConnection::~AdbcPoolConnection() {
	if (pool && connection && pooled) {
		// Hand the connection back so it can be reused by the same role.
		pool->ReturnConnection(std::move(connection), role);
	}
	// Ephemeral (non-pooled) connections simply drop here: the AdbcConnectionWrapper
	// destructor releases the underlying ADBC connection.
}

//===--------------------------------------------------------------------===//
// AdbcConnectionPool
//===--------------------------------------------------------------------===//

AdbcConnectionPool::AdbcConnectionPool(shared_ptr<AdbcDatabaseWrapper> database_p, idx_t maximum_connections_p)
    : database(std::move(database_p)), maximum_connections(maximum_connections_p) {
}

shared_ptr<AdbcConnectionWrapper> AdbcConnectionPool::OpenNewConnection() {
	auto conn = make_shared_ptr<AdbcConnectionWrapper>(database);
	conn->Init();
	conn->Initialize();
	return conn;
}

shared_ptr<AdbcConnectionWrapper> AdbcConnectionPool::AcquireRaw(AdbcConnectionRole role, bool &pooled_out) {
	unique_lock<mutex> l(lock);
	// Reuse an idle connection of the same role if one is available. Roles never
	// borrow from each other — see AdbcConnectionRole.
	auto &idle = IdleFor(role);
	if (!idle.empty()) {
		auto conn = std::move(idle.back());
		idle.pop_back();
		active_connections++;
		pooled_out = true;
		return conn;
	}
	// No idle connection for this role. If the pool is at its overall budget but
	// the other role is holding idle connections, drop one of those to make room
	// rather than falling through to an untracked ephemeral connection.
	if (TotalOpen() >= maximum_connections) {
		auto &other = IdleFor(role == AdbcConnectionRole::WRITE ? AdbcConnectionRole::READ
		                                                       : AdbcConnectionRole::WRITE);
		if (!other.empty()) {
			other.pop_back();
		}
	}
	// Open a new pooled connection if there is room. Release the lock while
	// opening (driver connect can be slow / may throw) but reserve the slot first.
	if (TotalOpen() < maximum_connections) {
		active_connections++;
		l.unlock();
		try {
			pooled_out = true;
			return OpenNewConnection();
		} catch (...) {
			// Failed to open — give the reserved slot back.
			lock_guard<mutex> recover(lock);
			active_connections--;
			throw;
		}
	}
	// Pool exhausted: hand out an ephemeral connection that is not tracked and is
	// released (not returned) when the lease is dropped. Callers never block.
	l.unlock();
	pooled_out = false;
	return OpenNewConnection();
}

AdbcPoolConnection AdbcConnectionPool::GetConnection(AdbcConnectionRole role) {
	bool pooled = false;
	auto conn = AcquireRaw(role, pooled);
	return AdbcPoolConnection(this, std::move(conn), role, pooled);
}

shared_ptr<AdbcConnectionWrapper> AdbcConnectionPool::GetConnectionShared() {
	// Scans only — a scan's connection can never be reused for writes.
	const auto role = AdbcConnectionRole::READ;
	bool pooled = false;
	auto conn = AcquireRaw(role, pooled);
	if (!pooled) {
		// Ephemeral: a plain shared_ptr; releasing the ADBC connection on last ref
		// is the right behavior (it was never tracked by the pool).
		return conn;
	}
	// Pooled: hand out an aliasing shared_ptr that points at the same connection
	// but, on last reference, returns the connection to the pool instead of
	// destroying it. The captured `conn` is the real owner and is moved back into
	// the idle list by ReturnConnection. The pool outlives all leases (a catalog
	// cannot be detached while a query is still using it).
	auto *self = this;
	auto *raw = conn.get();
	return shared_ptr<AdbcConnectionWrapper>(raw, [self, conn, role](AdbcConnectionWrapper *) mutable noexcept {
		// A shared_ptr deleter must never throw: an escaping exception calls
		// std::terminate(). ReturnConnection locks a mutex and pushes to a vector,
		// either of which could throw (e.g. std::system_error, bad_alloc). Swallow
		// anything; on failure the captured `conn` is destroyed here instead, which
		// releases the underlying ADBC connection rather than returning it to the pool.
		try {
			self->ReturnConnection(std::move(conn), role);
		} catch (...) {
			// Intentionally ignored — see above.
		}
	});
}

void AdbcConnectionPool::ReturnConnection(shared_ptr<AdbcConnectionWrapper> connection, AdbcConnectionRole role) {
	lock_guard<mutex> l(lock);
	if (active_connections == 0) {
		// Defensive: should never happen since every pooled lease was counted.
		return;
	}
	active_connections--;
	// If the maximum was lowered while this connection was checked out, drop it
	// instead of caching beyond the new capacity. TotalOpen() spans both idle
	// lists, so the two roles stay within one shared budget.
	if (TotalOpen() >= maximum_connections) {
		return;
	}
	IdleFor(role).push_back(std::move(connection));
}

void AdbcConnectionPool::SetMaximumConnections(idx_t new_max) {
	lock_guard<mutex> l(lock);
	if (new_max < maximum_connections) {
		// Reclaim idle connections down to the new cap (checked-out connections are
		// reclaimed lazily on return). Drain the read list first: writes are rarer,
		// so a cached write connection is more expensive to lose.
		while (TotalOpen() > new_max && (!idle_read.empty() || !idle_write.empty())) {
			if (!idle_read.empty()) {
				idle_read.pop_back();
			} else {
				idle_write.pop_back();
			}
		}
	}
	maximum_connections = new_max;
}

} // namespace adbc_scanner
