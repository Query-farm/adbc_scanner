//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/adbc_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "adbc_connection.hpp"

namespace adbc_scanner {
using namespace duckdb;
class AdbcCatalog;
class AdbcSchemaEntry;
class AdbcTableEntry;

enum class AdbcTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class AdbcTransaction : public Transaction {
public:
	AdbcTransaction(AdbcCatalog &adbc_catalog, TransactionManager &manager, ClientContext &context);
	~AdbcTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	shared_ptr<AdbcConnectionWrapper> GetConnection();

	static AdbcTransaction &Get(ClientContext &context, Catalog &catalog);

	AdbcCatalog &GetCatalog() {
		return adbc_catalog;
	}

private:
	AdbcCatalog &adbc_catalog;
	AdbcTransactionState transaction_state;
	AccessMode access_mode;
};

} // namespace adbc_scanner
