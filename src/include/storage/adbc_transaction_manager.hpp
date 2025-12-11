//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/adbc_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "storage/adbc_catalog.hpp"
#include "storage/adbc_transaction.hpp"
#include "duckdb/common/reference_map.hpp"

namespace duckdb {

class AdbcTransactionManager : public TransactionManager {
public:
	AdbcTransactionManager(AttachedDatabase &db_p, AdbcCatalog &adbc_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	AdbcCatalog &adbc_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<AdbcTransaction>> transactions;
};

} // namespace duckdb
