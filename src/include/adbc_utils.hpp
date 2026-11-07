#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include <nanoarrow/nanoarrow.h>
#include <arrow-adbc/adbc.h>
#include <arrow-adbc/adbc_driver_manager.h>
#include <string>

namespace adbc_scanner {
using namespace duckdb;

// Convert ADBC status code to a human-readable string
inline const char *StatusCodeToString(AdbcStatusCode code) {
    return AdbcStatusCodeMessage(code);
}

// Exempt a function from AddressSanitizer instrumentation. Used when reading
// buffers owned by a non-ASan-instrumented ADBC driver, where ASan can raise a
// false-positive "container-overflow" (the bytes are valid; only the container
// annotations are missing). No effect in release builds.
#if defined(__clang__) || defined(__GNUC__)
#define ADBC_NO_SANITIZE_ADDRESS __attribute__((no_sanitize_address))
#else
#define ADBC_NO_SANITIZE_ADDRESS
#endif

// Copy `len` bytes from a driver-owned buffer into a std::string using a plain
// uninstrumented loop. Reading the (non-instrumented) driver buffer element by
// element here avoids tripping ASan's container-overflow check that fires on
// instrumented loads / the memcpy interceptor when crossing the instrumentation
// boundary. The returned copy is a normal instrumented string, safe to inspect.
ADBC_NO_SANITIZE_ADDRESS
inline string CopyDriverBytes(const uint8_t *data, size_t len) {
    string out(len, '\0');
    for (size_t j = 0; j < len; j++) {
        out[j] = static_cast<char>(data[j]);
    }
    return out;
}

// Mask credentials in text that is shown to users (error messages, the
// attach path in duckdb_databases()). Two forms are masked:
//   - the password in a URI's userinfo: scheme://user:secret@host -> scheme://user:***@host
//   - the value of credential-like key=value pairs (password=..., token=...,
//     in URI query strings or 'k=v;k=v' option strings)
// Best effort: it never fails and leaves everything else unchanged.
inline string ScrubCredentials(const string &text) {
    string result = text;
    auto is_value_end = [](char c) {
        return c == ';' || c == '&' || c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '"' || c == '\'' ||
               c == ',' || c == ')';
    };

    // 1. URI userinfo passwords
    size_t search = 0;
    while (true) {
        auto scheme_end = result.find("://", search);
        if (scheme_end == string::npos) {
            break;
        }
        auto start = scheme_end + 3;
        auto end = start;
        while (end < result.size()) {
            char c = result[end];
            if (c == '/' || c == '?' || c == '#' || c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '"' ||
                c == '\'' || c == '<' || c == '>') {
                break;
            }
            end++;
        }
        auto at = result.rfind('@', end == 0 ? 0 : end - 1);
        if (at != string::npos && at >= start && at < end) {
            auto colon = result.find(':', start);
            if (colon != string::npos && colon < at) {
                result.replace(colon + 1, at - colon - 1, "***");
                end = colon + 1 + 3 + (end - at);
            }
        }
        search = end > start ? end : start;
    }

    // 2. credential-like key=value pairs
    static const char *const keys[] = {"password", "passwd", "pwd", "token", "secret", "api_key", "apikey"};
    for (auto key : keys) {
        string lower = StringUtil::Lower(result);
        string needle(key);
        size_t pos = 0;
        while ((pos = lower.find(needle, pos)) != string::npos) {
            auto key_end = pos + needle.size();
            // A key ends a word: password=, access_token=, client_secret=
            auto eq = key_end;
            while (eq < lower.size() && lower[eq] == ' ') {
                eq++;
            }
            if (eq >= lower.size() || lower[eq] != '=') {
                pos = key_end;
                continue;
            }
            auto value_start = eq + 1;
            while (value_start < lower.size() && lower[value_start] == ' ') {
                value_start++;
            }
            auto value_end = value_start;
            if (value_start < lower.size() && (lower[value_start] == '\'' || lower[value_start] == '"')) {
                // Quoted value: mask up to the closing quote
                char quote = lower[value_start];
                value_start++;
                value_end = lower.find(quote, value_start);
                if (value_end == string::npos) {
                    value_end = lower.size();
                }
            } else {
                while (value_end < lower.size() && !is_value_end(lower[value_end])) {
                    value_end++;
                }
            }
            if (value_end > value_start && result.compare(value_start, value_end - value_start, "***") != 0) {
                result.replace(value_start, value_end - value_start, "***");
                lower.replace(value_start, value_end - value_start, "***");
            }
            pos = value_start + 3;
        }
    }
    return result;
}

// Check ADBC status and throw DuckDB exception on error
// Optional driver_name parameter to include in error messages for better debugging
inline void CheckAdbc(AdbcStatusCode status, AdbcError *error, const string &context,
                      const string &driver_name = "") {
    if (status == ADBC_STATUS_OK) {
        return;
    }

    string message;

    // Include driver name if provided
    if (!driver_name.empty()) {
        message = "[" + driver_name + "] ";
    }

    message += context + ": ";

    bool has_message = error && error->message && error->message[0] != '\0';

    if (has_message) {
        message += error->message;
    } else {
        // No message from driver - include status code name for clarity
        message += StatusCodeToString(status);
    }

    // Add SQLSTATE if available
    if (error && error->sqlstate[0] != '\0') {
        message += " (SQLSTATE: ";
        message += string(error->sqlstate, 5);
        message += ")";
    }

    // Extract additional error details from ADBC 1.1.0+ drivers
    // These can include database-specific error codes, stack traces, etc.
    if (error && error->vendor_code == ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA) {
        int detail_count = AdbcErrorGetDetailCount(error);
        for (int i = 0; i < detail_count; i++) {
            struct AdbcErrorDetail detail = AdbcErrorGetDetail(error, i);
            if (detail.key && detail.value && detail.value_length > 0) {
                message += "\n    ";
                message += detail.key;
                message += " = ";
                // Copy the driver-owned value into our own buffer first (see
                // CopyDriverBytes), then inspect the copy.
                string value_copy = CopyDriverBytes(detail.value, static_cast<size_t>(detail.value_length));
                // Treat value as string if it looks like text, otherwise show length
                bool is_text = true;
                for (unsigned char c : value_copy) {
                    // Allow printable ASCII and common whitespace
                    if (c < 0x20 && c != '\t' && c != '\n' && c != '\r') {
                        is_text = false;
                        break;
                    } else if (c > 0x7E && c < 0xC0) {
                        // Not valid UTF-8 start byte or ASCII
                        is_text = false;
                        break;
                    }
                }
                if (is_text) {
                    message += value_copy;
                } else {
                    message += "<binary, " + std::to_string(detail.value_length) + " bytes>";
                }
            }
        }
    }

    // Release error resources. Null the release callback afterward so the owning
    // AdbcErrorGuard's destructor does not release a second time — calling an
    // AdbcError's release twice is undefined behavior, and some drivers (e.g. the
    // Rust DataFusion driver, which frees the message with CString::from_raw)
    // crash with a use-after-free on the second call.
    if (error && error->release) {
        error->release(error);
        error->release = nullptr;
    }

    // Drivers may echo the connection URI (which can carry userinfo credentials,
    // e.g. after FoldCredentialsIntoUri) in their error text.
    message = ScrubCredentials(message);

    // Throw appropriate exception based on status code
    switch (status) {
    case ADBC_STATUS_INVALID_ARGUMENT:
        throw InvalidInputException(message);
    case ADBC_STATUS_NOT_IMPLEMENTED:
        throw NotImplementedException(message);
    case ADBC_STATUS_NOT_FOUND:
        throw CatalogException(message);
    case ADBC_STATUS_UNAUTHENTICATED:
    case ADBC_STATUS_UNAUTHORIZED:
        throw PermissionException(message);
    case ADBC_STATUS_IO:
    case ADBC_STATUS_TIMEOUT:
        throw IOException(message);
    default:
        throw IOException(message);
    }
}

// Extract a short driver name from a full path
// e.g., "/path/to/libadbc_driver_sqlite.dylib" -> "sqlite"
inline string ExtractDriverName(const string &driver_path) {
    // Find the last path separator
    size_t last_sep = driver_path.find_last_of("/\\");
    string filename = (last_sep != string::npos) ? driver_path.substr(last_sep + 1) : driver_path;

    // Try to extract driver name from common patterns
    // Pattern: libadbc_driver_<name>.so/dylib/dll
    size_t driver_pos = filename.find("adbc_driver_");
    if (driver_pos != string::npos) {
        size_t name_start = driver_pos + 12;  // length of "adbc_driver_"
        size_t name_end = filename.find_first_of(".", name_start);
        if (name_end != string::npos) {
            return filename.substr(name_start, name_end - name_start);
        }
        return filename.substr(name_start);
    }

    // Pattern: <name>_driver.so/dylib/dll
    driver_pos = filename.find("_driver");
    if (driver_pos != string::npos) {
        // Remove lib prefix if present
        size_t name_start = 0;
        if (filename.substr(0, 3) == "lib") {
            name_start = 3;
        }
        return filename.substr(name_start, driver_pos - name_start);
    }

    // Fallback: use filename without extension
    size_t dot_pos = filename.find('.');
    if (dot_pos != string::npos) {
        string name = filename.substr(0, dot_pos);
        // Remove lib prefix if present
        if (name.substr(0, 3) == "lib") {
            name = name.substr(3);
        }
        return name;
    }

    return filename;
}

// RAII wrapper for AdbcError - ensures proper cleanup
class AdbcErrorGuard {
public:
    AdbcErrorGuard() {
        memset(&error, 0, sizeof(error));
        error.vendor_code = ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA;
    }

    ~AdbcErrorGuard() {
        if (error.release) {
            error.release(&error);
        }
    }

    AdbcError *Get() {
        return &error;
    }

    AdbcError *operator->() {
        return &error;
    }

    // Non-copyable
    AdbcErrorGuard(const AdbcErrorGuard &) = delete;
    AdbcErrorGuard &operator=(const AdbcErrorGuard &) = delete;

private:
    AdbcError error;
};

// Name a table function's positional parameters before it is registered, so
// duckdb_functions() lists them by name instead of a generated colN. In DuckDB
// 2.0 parameter names live in the function's signature; named (keyword-only)
// parameters are declared with GetSignature().AddKeywordOnly(...).
inline void NameParameters(TableFunction &function, const vector<string> &positional_names) {
    auto &signature = function.GetSignature();
    D_ASSERT(positional_names.size() <= signature.GetParameterCount());
    for (idx_t i = 0; i < positional_names.size(); i++) {
        signature.GetParameter(i).SetName(Identifier(positional_names[i]));
    }
}

} // namespace adbc_scanner
