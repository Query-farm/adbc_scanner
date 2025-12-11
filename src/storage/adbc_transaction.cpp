#include "storage/adbc_transaction.hpp"
#include "storage/adbc_catalog.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

AdbcTransaction::AdbcTransaction(AdbcCatalog &adbc_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), adbc_catalog(adbc_catalog),
      transaction_state(AdbcTransactionState::TRANSACTION_NOT_YET_STARTED), access_mode(adbc_catalog.access_mode) {
}

AdbcTransaction::~AdbcTransaction() = default;

void AdbcTransaction::Start() {
	transaction_state = AdbcTransactionState::TRANSACTION_STARTED;
}

void AdbcTransaction::Commit() {
	if (transaction_state == AdbcTransactionState::TRANSACTION_STARTED) {
		transaction_state = AdbcTransactionState::TRANSACTION_FINISHED;
		// ADBC transactions are auto-commit by default, so nothing to do here
	}
}

void AdbcTransaction::Rollback() {
	if (transaction_state == AdbcTransactionState::TRANSACTION_STARTED) {
		transaction_state = AdbcTransactionState::TRANSACTION_FINISHED;
	}
}

shared_ptr<adbc::AdbcConnectionWrapper> AdbcTransaction::GetConnection() {
	return adbc_catalog.GetConnection();
}

AdbcTransaction &AdbcTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<AdbcTransaction>();
}

} // namespace duckdb
