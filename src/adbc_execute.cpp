#include "adbc_connection.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace adbc_scanner {
using namespace duckdb;

// Helper to format error messages with query context
static string FormatError(const string &message, const string &query) {
    string result = message;
    // Truncate query if too long for error message
    if (query.length() > 100) {
        result += " [Query: " + query.substr(0, 100) + "...]";
    } else {
        result += " [Query: " + query + "]";
    }
    return result;
}

// Bind data for adbc_execute
struct AdbcExecuteBindData : public FunctionData {
    int64_t connection_id;
    string query;
    shared_ptr<AdbcConnectionWrapper> connection;
    vector<Value> params;
    vector<LogicalType> param_types;
    bool has_params = false;

    unique_ptr<FunctionData> Copy() const override {
        auto copy = make_uniq<AdbcExecuteBindData>();
        copy->connection_id = connection_id;
        copy->query = query;
        copy->connection = connection;
        copy->params = params;
        copy->param_types = param_types;
        copy->has_params = has_params;
        return std::move(copy);
    }

    bool Equals(const FunctionData &other_p) const override {
        auto &other = other_p.Cast<AdbcExecuteBindData>();
        return connection_id == other.connection_id && query == other.query;
    }
};

// Bind function for adbc_execute
static unique_ptr<FunctionData> AdbcExecuteBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
    auto bind_data = make_uniq<AdbcExecuteBindData>();
    return std::move(bind_data);
}

// Helper to execute a single DDL/DML statement and return rows affected
static int64_t ExecuteStatement(int64_t connection_id, const string &query) {
    // Look up and validate connection
    auto connection = GetValidatedConnection(connection_id, "adbc_execute");

    // Create and prepare statement
    auto statement = make_shared_ptr<AdbcStatementWrapper>(connection);
    statement->Init();
    statement->SetSqlQuery(query);

    try {
        statement->Prepare();
    } catch (Exception &e) {
        throw InvalidInputException(FormatError("adbc_execute: Failed to prepare statement: " + string(e.what()), query));
    }

    // Execute the statement
    ArrowArrayStream stream;
    memset(&stream, 0, sizeof(stream));
    int64_t rows_affected = -1;

    try {
        statement->ExecuteQuery(&stream, &rows_affected);
    } catch (Exception &e) {
        throw IOException(FormatError("adbc_execute: Failed to execute statement: " + string(e.what()), query));
    }

    // Release the stream if it was created (DDL/DML may or may not create one)
    if (stream.release) {
        stream.release(&stream);
    }

    // Return rows affected (or 0 if not available)
    return rows_affected >= 0 ? rows_affected : 0;
}

// Execute function - runs DDL/DML and returns rows affected
static void AdbcExecuteFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &conn_vector = args.data[0];
    auto &query_vector = args.data[1];
    auto count = args.size();

    // Handle constant input (for constant folding optimization)
    if (conn_vector.GetVectorType() == VectorType::CONSTANT_VECTOR &&
        query_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
        if (ConstantVector::IsNull(conn_vector)) {
            throw InvalidInputException("adbc_execute: Connection handle cannot be NULL");
        }
        if (ConstantVector::IsNull(query_vector)) {
            throw InvalidInputException("adbc_execute: Query cannot be NULL");
        }
        auto connection_id = conn_vector.GetValue(0).GetValue<int64_t>();
        auto query = query_vector.GetValue(0).GetValue<string>();
        auto rows_affected = ExecuteStatement(connection_id, query);
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
        ConstantVector::GetData<int64_t>(result)[0] = rows_affected;
        return;
    }

    // Handle flat/dictionary vectors
    result.SetVectorType(VectorType::FLAT_VECTOR);
    auto result_data = FlatVector::GetData<int64_t>(result);
    auto &validity = FlatVector::Validity(result);

    for (idx_t row_idx = 0; row_idx < count; row_idx++) {
        auto conn_value = conn_vector.GetValue(row_idx);
        auto query_value = query_vector.GetValue(row_idx);

        if (conn_value.IsNull()) {
            throw InvalidInputException("adbc_execute: Connection handle cannot be NULL");
        }
        if (query_value.IsNull()) {
            throw InvalidInputException("adbc_execute: Query cannot be NULL");
        }

        auto connection_id = conn_value.GetValue<int64_t>();
        auto query = query_value.GetValue<string>();
        result_data[row_idx] = ExecuteStatement(connection_id, query);
    }
}

// Register adbc_execute scalar function
void RegisterAdbcExecuteFunction(DatabaseInstance &db) {
    ExtensionLoader loader(db, "adbc");

    ScalarFunction adbc_execute_function(
        "adbc_execute",
        {LogicalType::BIGINT, LogicalType::VARCHAR},
        LogicalType::BIGINT,
        AdbcExecuteFunction,
        AdbcExecuteBind
    );
    // Disable automatic NULL propagation so we can throw a meaningful error
    adbc_execute_function.null_handling = FunctionNullHandling::SPECIAL_HANDLING;

    CreateScalarFunctionInfo info(adbc_execute_function);
    FunctionDescription desc;
    desc.description = "Execute DDL/DML statements (CREATE, INSERT, UPDATE, DELETE) on an ADBC connection";
    desc.parameter_names = {"connection_handle", "query"};
    desc.parameter_types = {LogicalType::BIGINT, LogicalType::VARCHAR};
    desc.examples = {"SELECT adbc_execute(conn, 'CREATE TABLE test (id INTEGER)')",
                     "SELECT adbc_execute(conn, 'INSERT INTO test VALUES (1)')",
                     "SELECT adbc_execute(conn, 'DELETE FROM test WHERE id = 1')"};
    desc.categories = {"adbc"};
    info.descriptions.push_back(std::move(desc));
    loader.RegisterFunction(info);
}

} // namespace adbc_scanner
