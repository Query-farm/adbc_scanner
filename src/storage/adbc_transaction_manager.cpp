#include "storage/adbc_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

AdbcTransactionManager::AdbcTransactionManager(AttachedDatabase &db_p, AdbcCatalog &adbc_catalog)
    : TransactionManager(db_p), adbc_catalog(adbc_catalog) {
}

Transaction &AdbcTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<AdbcTransaction>(adbc_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData AdbcTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &adbc_transaction = transaction.Cast<AdbcTransaction>();
	adbc_transaction.Commit();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void AdbcTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &adbc_transaction = transaction.Cast<AdbcTransaction>();
	adbc_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void AdbcTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// ADBC doesn't have a checkpoint operation
}

} // namespace duckdb
