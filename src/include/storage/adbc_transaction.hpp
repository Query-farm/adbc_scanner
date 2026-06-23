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
#include "storage/adbc_connection_pool.hpp"

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

	//! The client context that owns this transaction (used for Arrow type mapping
	//! during catalog introspection).
	ClientContext &GetContext();

	//! Connection used for writes in this transaction. On first use it leases a
	//! connection and disables ADBC autocommit so every write in the transaction
	//! commits or rolls back together (driven by Commit/Rollback below). Throws if
	//! the catalog is read-only, or NotImplemented if the driver cannot disable
	//! autocommit (fail loud rather than silently auto-committing).
	shared_ptr<AdbcConnectionWrapper> GetWriteConnection();

	static AdbcTransaction &Get(ClientContext &context, Catalog &catalog);

	AdbcCatalog &GetCatalog() {
		return adbc_catalog;
	}

private:
	AdbcCatalog &adbc_catalog;
	AdbcTransactionState transaction_state;
	//! Lazily-leased connection for writes; empty for read-only transactions.
	AdbcPoolConnection write_lease;
	//! Whether autocommit has been disabled on the write connection.
	bool write_started = false;
};

} // namespace adbc_scanner
