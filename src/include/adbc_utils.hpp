#pragma once

#include "duckdb.hpp"
#include <nanoarrow/nanoarrow.h>
#include <arrow-adbc/adbc.h>
#include <arrow-adbc/adbc_driver_manager.h>
#include <string>

namespace duckdb {
namespace adbc {

// Convert ADBC status code to a human-readable string
inline const char *StatusCodeToString(AdbcStatusCode code) {
    return AdbcStatusCodeMessage(code);
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
                // Treat value as string if it looks like text, otherwise show length
                bool is_text = true;
                for (size_t j = 0; j < detail.value_length && is_text; j++) {
                    // Allow printable ASCII and common whitespace
                    uint8_t c = detail.value[j];
                    if (c < 0x20 && c != '\t' && c != '\n' && c != '\r') {
                        is_text = false;
                    } else if (c > 0x7E && c < 0xC0) {
                        // Not valid UTF-8 start byte or ASCII
                        is_text = false;
                    }
                }
                if (is_text) {
                    message += string(reinterpret_cast<const char *>(detail.value), detail.value_length);
                } else {
                    message += "<binary, " + std::to_string(detail.value_length) + " bytes>";
                }
            }
        }
    }

    // Release error resources
    if (error && error->release) {
        error->release(error);
    }

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

} // namespace adbc
} // namespace duckdb
