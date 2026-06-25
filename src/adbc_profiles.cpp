#include "adbc_connection.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/string_util.hpp"

#include <cstdlib>
#include <filesystem>

#include <toml++/toml.hpp>

namespace adbc_scanner {
using namespace duckdb;

//===--------------------------------------------------------------------===//
// adbc_profiles - Enumerate discoverable ADBC connection profiles
//
// Mirrors the search order of the ADBC driver manager's filesystem profile
// provider (AdbcProfileProviderFilesystem):
//   1. caller-provided search_paths (the 'search_paths' named parameter)
//   2. the ADBC_PROFILE_PATH environment variable
//   3. the per-OS user configuration directory
// Each *.toml file found is parsed (profile_version, driver) and returned as a
// row. The driver manager itself exposes no enumeration API, so we replicate
// the discovery here using the same tomlplusplus parser it uses internally.
//===--------------------------------------------------------------------===//

namespace {

#if defined(_WIN32)
constexpr char kPathSeparator = ';';
#else
constexpr char kPathSeparator = ':';
#endif

// Per-OS user configuration directory, matching the driver manager's
// InternalAdbcUserConfigDir(). Returns empty when it cannot be determined.
std::filesystem::path UserConfigDir() {
#if defined(_WIN32)
	const char *appdata = std::getenv("LOCALAPPDATA");
	if (appdata && *appdata) {
		return std::filesystem::path(appdata) / "ADBC";
	}
	return {};
#elif defined(__APPLE__)
	const char *home = std::getenv("HOME");
	if (home && *home) {
		return std::filesystem::path(home) / "Library" / "Application Support" / "ADBC";
	}
	return {};
#else
	const char *xdg = std::getenv("XDG_CONFIG_HOME");
	if (xdg && *xdg) {
		return std::filesystem::path(xdg) / "adbc";
	}
	const char *home = std::getenv("HOME");
	if (home && *home) {
		return std::filesystem::path(home) / ".config" / "adbc";
	}
	return {};
#endif
}

// The leaf directory name for profiles differs by platform, matching the
// driver manager's GetProfileSearchPaths().
const char *ProfilesDirName() {
#if defined(_WIN32) || defined(__APPLE__)
	return "Profiles";
#else
	return "profiles";
#endif
}

struct SearchDir {
	std::filesystem::path path;
	string source;
};

vector<SearchDir> ProfileSearchDirs(const string &extra_search_paths) {
	vector<SearchDir> dirs;

	auto add_list = [&](const string &list, const string &source) {
		if (list.empty()) {
			return;
		}
		for (auto &p : StringUtil::Split(list, kPathSeparator)) {
			if (!p.empty()) {
				dirs.push_back({std::filesystem::path(p), source});
			}
		}
	};

	// 1. Caller-provided search paths
	add_list(extra_search_paths, "additional");

	// 2. ADBC_PROFILE_PATH environment variable
	const char *env_path = std::getenv("ADBC_PROFILE_PATH");
	if (env_path) {
		add_list(env_path, "env");
	}

	// 3. User configuration directory
	auto user_dir = UserConfigDir();
	if (!user_dir.empty()) {
		dirs.push_back({user_dir / ProfilesDirName(), "user"});
	}

	return dirs;
}

struct ProfileRow {
	string name;
	string driver;
	string path;
	string source;
	int64_t profile_version;
};

struct AdbcProfilesBindData : public TableFunctionData {
	vector<ProfileRow> rows;
};

struct AdbcProfilesGlobalState : public GlobalTableFunctionState {
	idx_t offset = 0;
	idx_t MaxThreads() const override {
		return 1;
	}
};

unique_ptr<FunctionData> AdbcProfilesBind(ClientContext &, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<AdbcProfilesBindData>();

	string extra_search_paths;
	auto entry = input.named_parameters.find("search_paths");
	if (entry != input.named_parameters.end() && !entry->second.IsNull()) {
		extra_search_paths = entry->second.ToString();
	}

	for (auto &dir : ProfileSearchDirs(extra_search_paths)) {
		std::error_code ec;
		if (!std::filesystem::is_directory(dir.path, ec)) {
			continue;
		}
		for (auto it = std::filesystem::directory_iterator(dir.path, ec);
		     !ec && it != std::filesystem::directory_iterator(); it.increment(ec)) {
			const auto &dir_entry = *it;
			if (!dir_entry.is_regular_file(ec)) {
				continue;
			}
			const auto &file_path = dir_entry.path();
			if (file_path.extension() != ".toml") {
				continue;
			}

			ProfileRow row;
			row.name = file_path.stem().string();
			row.path = file_path.string();
			row.source = dir.source;
			row.profile_version = 0;

			// Best-effort parse: a malformed profile is still listed (with empty
			// driver / version 0) rather than failing the whole enumeration.
			try {
				auto config = toml::parse_file(file_path.native());
				row.profile_version = config["profile_version"].value_or<int64_t>(0);
				row.driver = config["driver"].value_or<std::string>("");
			} catch (const toml::parse_error &) {
				// Leave defaults; the file exists but could not be parsed.
			}

			bind_data->rows.push_back(std::move(row));
		}
	}

	names = {"name", "driver", "path", "source", "profile_version"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::BIGINT};

	return std::move(bind_data);
}

unique_ptr<GlobalTableFunctionState> AdbcProfilesInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<AdbcProfilesGlobalState>();
}

void AdbcProfilesFunction(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<AdbcProfilesBindData>();
	auto &state = data_p.global_state->Cast<AdbcProfilesGlobalState>();

	idx_t count = 0;
	while (state.offset < bind_data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = bind_data.rows[state.offset];
		output.SetValue(0, count, Value(row.name));
		output.SetValue(1, count, row.driver.empty() ? Value(LogicalType::VARCHAR) : Value(row.driver));
		output.SetValue(2, count, Value(row.path));
		output.SetValue(3, count, Value(row.source));
		output.SetValue(4, count, Value::BIGINT(row.profile_version));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

} // namespace

void RegisterAdbcProfilesFunction(DatabaseInstance &db) {
	ExtensionLoader loader(db, "adbc");

	TableFunction profiles("adbc_profiles", {}, AdbcProfilesFunction, AdbcProfilesBind, AdbcProfilesInit);
	profiles.named_parameters["search_paths"] = LogicalType::VARCHAR;

	CreateTableFunctionInfo info(profiles);
	FunctionDescription desc;
	desc.description = "List discoverable ADBC connection profiles from the standard search paths";
	desc.examples = {"SELECT * FROM adbc_profiles()",
	                 "SELECT * FROM adbc_profiles(search_paths := '/opt/adbc/profiles')"};
	desc.categories = {"adbc"};
	info.descriptions.push_back(std::move(desc));
	loader.RegisterFunction(info);
}

} // namespace adbc_scanner
