#include "adbc_connection.hpp"
#include "adbc_secrets.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace adbc_scanner {
using namespace duckdb;

// Helper to extract key-value pairs from either a STRUCT or MAP
static vector<pair<string, string>> ExtractOptions(Vector &options_vector, idx_t row_idx) {
	vector<pair<string, string>> options;
	auto value = options_vector.GetValue(row_idx);
	auto &type = value.type();

	if (type.id() == LogicalTypeId::STRUCT) {
		// Handle STRUCT - iterate over named fields
		auto &children = StructValue::GetChildren(value);
		for (idx_t i = 0; i < children.size(); i++) {
			auto key = StructType::GetChildName(type, i);
			auto &child_value = children[i];
			if (!child_value.IsNull()) {
				options.emplace_back(key, child_value.ToString());
			}
		}
	} else if (type.id() == LogicalTypeId::MAP) {
		// Handle MAP - iterate over key-value pairs
		auto &map_children = MapValue::GetChildren(value);
		for (auto &entry : map_children) {
			auto &entry_children = StructValue::GetChildren(entry);
			if (entry_children.size() == 2 && !entry_children[0].IsNull()) {
				auto key = entry_children[0].ToString();
				auto val = entry_children[1].IsNull() ? "" : entry_children[1].ToString();
				options.emplace_back(key, val);
			}
		}
	} else {
		throw InvalidInputException("adbc_connect: options must be a STRUCT or MAP, got " + type.ToString());
	}

	return options;
}

// Helper to create a connection from options
// If context is provided, secrets will be looked up and merged with explicit options
static int64_t CreateConnection(const vector<pair<string, string>> &explicit_options,
                                ClientContext *context = nullptr) {
	// Merge with secrets if context is available
	vector<pair<string, string>> options;
	if (context) {
		options = MergeSecretOptions(*context, explicit_options);
	} else {
		options = explicit_options;
	}

	// Create connection using shared helper
	auto connection = CreateConnectionFromOptions(options);

	// Register connection and return handle
	auto &registry = ConnectionRegistry::Get();
	return registry.Add(std::move(connection));
}

// adbc_connect(options STRUCT or MAP) -> BIGINT
// Returns a connection handle that can be used with other ADBC functions
// Secrets are automatically looked up based on the 'uri' option or explicit 'secret' name
static void AdbcConnectFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &options_vector = args.data[0];
	auto count = args.size();
	auto &context = state.GetContext();

	// Handle constant input (for constant folding optimization)
	if (options_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(options_vector)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			auto options = ExtractOptions(options_vector, 0);
			auto conn_id = CreateConnection(options, &context);
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::GetData<int64_t>(result)[0] = conn_id;
		}
		return;
	}

	// Handle flat/dictionary vectors
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<int64_t>(result);

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto options = ExtractOptions(options_vector, row_idx);
		result_data[row_idx] = CreateConnection(options, &context);
	}
}

// adbc_disconnect(connection_id BIGINT) -> BOOLEAN
// Disconnects and removes a connection from the registry
static void AdbcDisconnectFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &connection_vector = args.data[0];

	UnaryExecutor::Execute<int64_t, bool>(connection_vector, result, args.size(), [&](int64_t connection_id) {
		auto &registry = ConnectionRegistry::Get();
		auto connection = registry.Remove(connection_id);
		if (!connection) {
			throw InvalidInputException("adbc_disconnect: Invalid connection handle: " + to_string(connection_id));
		}
		// Connection is automatically released when shared_ptr goes out of scope
		return true;
	});
}

// adbc_commit(connection_id BIGINT) -> BOOLEAN
// Commits the current transaction
static void AdbcCommitFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &connection_vector = args.data[0];

	UnaryExecutor::Execute<int64_t, bool>(connection_vector, result, args.size(), [&](int64_t connection_id) {
		auto connection = GetValidatedConnection(connection_id, "adbc_commit");
		connection->Commit();
		return true;
	});
}

// adbc_rollback(connection_id BIGINT) -> BOOLEAN
// Rolls back the current transaction
static void AdbcRollbackFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &connection_vector = args.data[0];

	UnaryExecutor::Execute<int64_t, bool>(connection_vector, result, args.size(), [&](int64_t connection_id) {
		auto connection = GetValidatedConnection(connection_id, "adbc_rollback");
		connection->Rollback();
		return true;
	});
}

// adbc_set_autocommit(connection_id BIGINT, enabled BOOLEAN) -> BOOLEAN
// Sets the autocommit mode for the connection
static void AdbcSetAutocommitFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &connection_vector = args.data[0];
	auto &enabled_vector = args.data[1];

	BinaryExecutor::Execute<int64_t, bool, bool>(
	    connection_vector, enabled_vector, result, args.size(), [&](int64_t connection_id, bool enabled) {
		    auto connection = GetValidatedConnection(connection_id, "adbc_set_autocommit");
		    connection->SetAutocommit(enabled);
		    return true;
	    });
}

// Register the ADBC scalar functions using ExtensionLoader
void RegisterAdbcScalarFunctions(DatabaseInstance &db) {
	ExtensionLoader loader(db, "adbc");

	// adbc_connect: Create a new ADBC connection
	{
		auto adbc_connect_function =
		    ScalarFunction("adbc_connect", {LogicalType::ANY}, LogicalType::BIGINT, AdbcConnectFunction);
		CreateScalarFunctionInfo info(adbc_connect_function);
		FunctionDescription desc;
		desc.description = "Connect to an ADBC data source and return a connection handle";
		desc.parameter_names = {"options"};
		desc.parameter_types = {LogicalType::ANY};
		desc.examples = {"SELECT adbc_connect({'driver': 'sqlite', 'uri': ':memory:'})",
		                 "SELECT adbc_connect({'driver': '/path/to/driver.so', 'uri': 'connection_string'})"};
		desc.categories = {"adbc"};
		info.descriptions.push_back(std::move(desc));
		loader.RegisterFunction(info);
	}

	// adbc_disconnect: Close an ADBC connection
	{
		auto adbc_disconnect_function =
		    ScalarFunction("adbc_disconnect", {LogicalType::BIGINT}, LogicalType::BOOLEAN, AdbcDisconnectFunction);
		CreateScalarFunctionInfo info(adbc_disconnect_function);
		FunctionDescription desc;
		desc.description = "Disconnect and close an ADBC connection";
		desc.parameter_names = {"connection_handle"};
		desc.parameter_types = {LogicalType::BIGINT};
		desc.examples = {"SELECT adbc_disconnect(connection_handle)"};
		desc.categories = {"adbc"};
		info.descriptions.push_back(std::move(desc));
		loader.RegisterFunction(info);
	}

	// adbc_commit: Commit the current transaction
	{
		auto adbc_commit_function =
		    ScalarFunction("adbc_commit", {LogicalType::BIGINT}, LogicalType::BOOLEAN, AdbcCommitFunction);
		CreateScalarFunctionInfo info(adbc_commit_function);
		FunctionDescription desc;
		desc.description = "Commit the current transaction on an ADBC connection";
		desc.parameter_names = {"connection_handle"};
		desc.parameter_types = {LogicalType::BIGINT};
		desc.examples = {"SELECT adbc_commit(connection_handle)"};
		desc.categories = {"adbc"};
		info.descriptions.push_back(std::move(desc));
		loader.RegisterFunction(info);
	}

	// adbc_rollback: Rollback the current transaction
	{
		auto adbc_rollback_function =
		    ScalarFunction("adbc_rollback", {LogicalType::BIGINT}, LogicalType::BOOLEAN, AdbcRollbackFunction);
		CreateScalarFunctionInfo info(adbc_rollback_function);
		FunctionDescription desc;
		desc.description = "Rollback the current transaction on an ADBC connection";
		desc.parameter_names = {"connection_handle"};
		desc.parameter_types = {LogicalType::BIGINT};
		desc.examples = {"SELECT adbc_rollback(connection_handle)"};
		desc.categories = {"adbc"};
		info.descriptions.push_back(std::move(desc));
		loader.RegisterFunction(info);
	}

	// adbc_set_autocommit: Set autocommit mode
	{
		auto adbc_set_autocommit_function =
		    ScalarFunction("adbc_set_autocommit", {LogicalType::BIGINT, LogicalType::BOOLEAN}, LogicalType::BOOLEAN,
		                   AdbcSetAutocommitFunction);
		CreateScalarFunctionInfo info(adbc_set_autocommit_function);
		FunctionDescription desc;
		desc.description = "Enable or disable autocommit mode on an ADBC connection";
		desc.parameter_names = {"connection_handle", "enabled"};
		desc.parameter_types = {LogicalType::BIGINT, LogicalType::BOOLEAN};
		desc.examples = {"SELECT adbc_set_autocommit(connection_handle, false)",
		                 "SELECT adbc_set_autocommit(connection_handle, true)"};
		desc.categories = {"adbc"};
		info.descriptions.push_back(std::move(desc));
		loader.RegisterFunction(info);
	}
}

} // namespace adbc_scanner
