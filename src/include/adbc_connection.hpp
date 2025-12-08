#pragma once

#include "duckdb.hpp"
#include "adbc_utils.hpp"
#include <arrow-adbc/adbc.h>
#include <arrow-adbc/adbc_driver_manager.h>
#include <memory>
#include <mutex>
#include <unordered_set>

namespace duckdb {
namespace adbc {

// Forward declaration
class AdbcConnectionWrapper;

// RAII wrapper for AdbcDatabase
class AdbcDatabaseWrapper {
public:
    AdbcDatabaseWrapper() : initialized(false) {
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
        if (initialized) {
            return;
        }
        AdbcErrorGuard error;
        auto status = AdbcDatabaseNew(&database, error.Get());
        CheckAdbc(status, error.Get(), "Failed to create ADBC database");
    }

    void SetOption(const string &key, const string &value) {
        AdbcErrorGuard error;
        auto status = AdbcDatabaseSetOption(&database, key.c_str(), value.c_str(), error.Get());
        CheckAdbc(status, error.Get(), "Failed to set database option '" + key + "'", driver_name);
    }

    void Initialize() {
        AdbcErrorGuard error;
        auto status = AdbcDatabaseInit(&database, error.Get());
        CheckAdbc(status, error.Get(), "Failed to initialize ADBC database", driver_name);
        initialized = true;
    }

    void Release() {
        if (initialized) {
            AdbcErrorGuard error;
            AdbcDatabaseRelease(&database, error.Get());
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
        : database(other.database), initialized(other.initialized) {
        other.initialized = false;
        memset(&other.database, 0, sizeof(other.database));
    }

private:
    AdbcDatabase database;
    bool initialized;
    string driver_name;
};

// RAII wrapper for AdbcConnection
class AdbcConnectionWrapper {
public:
    AdbcConnectionWrapper(shared_ptr<AdbcDatabaseWrapper> db) : database(std::move(db)), initialized(false) {
        memset(&connection, 0, sizeof(connection));
    }

    ~AdbcConnectionWrapper() {
        Release();
    }

    void Init() {
        AdbcErrorGuard error;
        auto status = AdbcConnectionNew(&connection, error.Get());
        CheckAdbc(status, error.Get(), "Failed to create ADBC connection", GetDriverName());
    }

    void SetOption(const string &key, const string &value) {
        AdbcErrorGuard error;
        auto status = AdbcConnectionSetOption(&connection, key.c_str(), value.c_str(), error.Get());
        CheckAdbc(status, error.Get(), "Failed to set connection option '" + key + "'", GetDriverName());
    }

    void Initialize() {
        AdbcErrorGuard error;
        auto status = AdbcConnectionInit(&connection, database->Get(), error.Get());
        CheckAdbc(status, error.Get(), "Failed to initialize ADBC connection", GetDriverName());
        initialized = true;
    }

    void Release() {
        if (initialized) {
            AdbcErrorGuard error;
            AdbcConnectionRelease(&connection, error.Get());
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
        AdbcErrorGuard error;
        auto status = AdbcConnectionGetInfo(&connection, info_codes, info_codes_length, out, error.Get());
        CheckAdbc(status, error.Get(), "Failed to get connection info", GetDriverName());
    }

    // Get database objects (catalogs, schemas, tables, columns)
    // depth: 0=all, 1=catalogs, 2=schemas, 3=tables
    // Other parameters can be NULL for no filtering, or search patterns
    void GetObjects(int depth, const char *catalog, const char *db_schema,
                    const char *table_name, const char **table_types,
                    const char *column_name, ArrowArrayStream *out) {
        AdbcErrorGuard error;
        auto status = AdbcConnectionGetObjects(&connection, depth, catalog, db_schema,
                                                table_name, table_types, column_name, out, error.Get());
        CheckAdbc(status, error.Get(), "Failed to get database objects", GetDriverName());
    }

    // Get table types (e.g., "TABLE", "VIEW", etc.)
    void GetTableTypes(ArrowArrayStream *out) {
        AdbcErrorGuard error;
        auto status = AdbcConnectionGetTableTypes(&connection, out, error.Get());
        CheckAdbc(status, error.Get(), "Failed to get table types", GetDriverName());
    }

    // Get the Arrow schema for a specific table
    void GetTableSchema(const char *catalog, const char *db_schema,
                        const char *table_name, ArrowSchema *schema) {
        AdbcErrorGuard error;
        auto status = AdbcConnectionGetTableSchema(&connection, catalog, db_schema,
                                                    table_name, schema, error.Get());
        CheckAdbc(status, error.Get(), "Failed to get table schema", GetDriverName());
    }

    // Non-copyable
    AdbcConnectionWrapper(const AdbcConnectionWrapper &) = delete;
    AdbcConnectionWrapper &operator=(const AdbcConnectionWrapper &) = delete;

private:
    shared_ptr<AdbcDatabaseWrapper> database;
    AdbcConnection connection;
    bool initialized;
};

// RAII wrapper for AdbcStatement
class AdbcStatementWrapper {
public:
    AdbcStatementWrapper(shared_ptr<AdbcConnectionWrapper> conn) : connection(std::move(conn)), initialized(false) {
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
};

// Thread-safe connection registry
class ConnectionRegistry {
public:
    static ConnectionRegistry &Get() {
        static ConnectionRegistry instance;
        return instance;
    }

    // Add a connection and return its handle
    int64_t Add(shared_ptr<AdbcConnectionWrapper> connection) {
        lock_guard<mutex> lock(mutex_);
        int64_t handle = reinterpret_cast<int64_t>(connection.get());
        connections_[handle] = std::move(connection);
        return handle;
    }

    // Get a connection by handle (returns nullptr if not found)
    shared_ptr<AdbcConnectionWrapper> Get(int64_t handle) {
        lock_guard<mutex> lock(mutex_);
        auto it = connections_.find(handle);
        if (it == connections_.end()) {
            return nullptr;
        }
        return it->second;
    }

    // Remove and return a connection (for cleanup)
    shared_ptr<AdbcConnectionWrapper> Remove(int64_t handle) {
        lock_guard<mutex> lock(mutex_);
        auto it = connections_.find(handle);
        if (it == connections_.end()) {
            return nullptr;
        }
        auto conn = std::move(it->second);
        connections_.erase(it);
        return conn;
    }

    // Check if a handle exists
    bool Contains(int64_t handle) {
        lock_guard<mutex> lock(mutex_);
        return connections_.find(handle) != connections_.end();
    }

    // Get count of active connections
    size_t Count() {
        lock_guard<mutex> lock(mutex_);
        return connections_.size();
    }

private:
    ConnectionRegistry() = default;
    ~ConnectionRegistry() = default;

    // Non-copyable
    ConnectionRegistry(const ConnectionRegistry &) = delete;
    ConnectionRegistry &operator=(const ConnectionRegistry &) = delete;

    mutex mutex_;
    unordered_map<int64_t, shared_ptr<AdbcConnectionWrapper>> connections_;
};

} // namespace adbc
} // namespace duckdb
