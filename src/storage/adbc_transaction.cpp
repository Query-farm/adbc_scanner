#include "storage/adbc_transaction.hpp"
#include "storage/adbc_catalog.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace adbc_scanner {
using namespace duckdb;

AdbcTransaction::AdbcTransaction(AdbcCatalog &adbc_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), adbc_catalog(adbc_catalog),
      transaction_state(AdbcTransactionState::TRANSACTION_NOT_YET_STARTED) {
}

AdbcTransaction::~AdbcTransaction() = default;

void AdbcTransaction::Start() {
	transaction_state = AdbcTransactionState::TRANSACTION_STARTED;
}

void AdbcTransaction::Commit() {
	transaction_state = AdbcTransactionState::TRANSACTION_FINISHED;
	if (write_started) {
		// Commit the ADBC transaction, then restore autocommit so the pooled
		// connection is clean when the lease returns it to the pool.
		auto conn = write_lease.GetConnection();
		conn->Commit();
		conn->SetAutocommit(true);
		write_started = false;
	}
}

void AdbcTransaction::Rollback() {
	transaction_state = AdbcTransactionState::TRANSACTION_FINISHED;
	if (write_started) {
		auto conn = write_lease.GetConnection();
		conn->Rollback();
		conn->SetAutocommit(true);
		write_started = false;
	}
}

shared_ptr<AdbcConnectionWrapper> AdbcTransaction::GetConnection() {
	return adbc_catalog.GetConnection();
}

ClientContext &AdbcTransaction::GetContext() {
	return *context.lock();
}

shared_ptr<AdbcConnectionWrapper> AdbcTransaction::GetWriteConnection() {
	if (adbc_catalog.access_mode == AccessMode::READ_ONLY) {
		throw PermissionException("Cannot write to ADBC database \"%s\" — it is attached read-only",
		                          adbc_catalog.GetName());
	}
	if (!write_lease.HasConnection()) {
		// Pin a dedicated connection for the lifetime of this transaction so all
		// writes share one connection and commit/roll back together. It must be a
		// WRITE-role connection: one that has served a scan would silently skip
		// BEGIN and autocommit, making ROLLBACK a no-op (see AdbcConnectionRole).
		write_lease = adbc_catalog.GetPool().GetConnection(AdbcConnectionRole::WRITE);
	}
	auto conn = write_lease.GetConnection();
	if (!write_started) {
		try {
			conn->SetAutocommit(false);
		} catch (std::exception &e) {
			throw NotImplementedException(
			    "ADBC driver does not support multi-statement transactions (could not disable "
			    "autocommit): %s",
			    e.what());
		}
		write_started = true;
	}
	return conn;
}

AdbcTransaction &AdbcTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<AdbcTransaction>();
}

} // namespace adbc_scanner
