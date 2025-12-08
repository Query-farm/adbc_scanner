#pragma once

#include "duckdb.hpp"
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
inline void CheckAdbc(AdbcStatusCode status, AdbcError *error, const string &context) {
    if (status == ADBC_STATUS_OK) {
        return;
    }

    string message = context + ": ";
    if (error && error->message) {
        message += error->message;
        // Add SQLSTATE if available
        if (error->sqlstate[0] != '\0') {
            message += " (SQLSTATE: ";
            message += string(error->sqlstate, 5);
            message += ")";
        }
    } else {
        message += StatusCodeToString(status);
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
