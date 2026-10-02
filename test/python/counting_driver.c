/* A small ADBC test driver for observing option dispatch and resource cleanup. */
#include <arrow-adbc/adbc.h>
#include <stdlib.h>
#include <string.h>

static int counters[12];
struct DatabaseState { int fail_database; int fail_connection; };

void AdbcTestReset(void) { memset(counters, 0, sizeof(counters)); }
int AdbcTestCounter(int index) { return counters[index]; }

static AdbcStatusCode DatabaseNew(struct AdbcDatabase *database, struct AdbcError *error) {
    database->private_data = calloc(1, sizeof(struct DatabaseState));
    if (!database->private_data) return ADBC_STATUS_INTERNAL;
    counters[0]++;
    return ADBC_STATUS_OK;
}
static AdbcStatusCode DatabaseRelease(struct AdbcDatabase *database, struct AdbcError *error) {
    free(database->private_data);
    database->private_data = NULL;
    counters[1]++;
    return ADBC_STATUS_OK;
}
static AdbcStatusCode DatabaseInit(struct AdbcDatabase *database, struct AdbcError *error) {
    struct DatabaseState *state = database->private_data;
    return state->fail_database ? ADBC_STATUS_IO : ADBC_STATUS_OK;
}
static AdbcStatusCode StringOption(struct AdbcDatabase *database, const char *key,
                                   const char *value, struct AdbcError *error) {
    struct DatabaseState *state = database->private_data;
    if (!strcmp(key, "fail_database")) state->fail_database = 1;
    else if (!strcmp(key, "fail_connection")) state->fail_connection = 1;
    else if (!strcmp(key, "test.string") && !strcmp(value, "example")) counters[5]++;
    else return ADBC_STATUS_INVALID_ARGUMENT;
    return ADBC_STATUS_OK;
}
static AdbcStatusCode IntOption(struct AdbcDatabase *database, const char *key,
                                int64_t value, struct AdbcError *error) {
    if (strcmp(key, "test.int") || value != 42) return ADBC_STATUS_INVALID_ARGUMENT;
    counters[6]++;
    return ADBC_STATUS_OK;
}
static AdbcStatusCode DoubleOption(struct AdbcDatabase *database, const char *key,
                                   double value, struct AdbcError *error) {
    if (strcmp(key, "test.double") || value != 1.25) return ADBC_STATUS_INVALID_ARGUMENT;
    counters[7]++;
    return ADBC_STATUS_OK;
}
static AdbcStatusCode BytesOption(struct AdbcDatabase *database, const char *key,
                                  const uint8_t *value, size_t length, struct AdbcError *error) {
    const uint8_t expected[] = {0, 1, 255};
    if (strcmp(key, "test.bytes") || length != 3 || memcmp(value, expected, 3)) {
        return ADBC_STATUS_INVALID_ARGUMENT;
    }
    counters[8]++;
    return ADBC_STATUS_OK;
}
static AdbcStatusCode ConnectionNew(struct AdbcConnection *connection, struct AdbcError *error) {
    connection->private_data = malloc(1);
    if (!connection->private_data) return ADBC_STATUS_INTERNAL;
    counters[2]++;
    return ADBC_STATUS_OK;
}
static AdbcStatusCode ConnectionInit(struct AdbcConnection *connection,
                                     struct AdbcDatabase *database, struct AdbcError *error) {
    struct DatabaseState *state = database->private_data;
    return state->fail_connection ? ADBC_STATUS_IO : ADBC_STATUS_OK;
}
static AdbcStatusCode ConnectionRelease(struct AdbcConnection *connection, struct AdbcError *error) {
    free(connection->private_data);
    connection->private_data = NULL;
    counters[3]++;
    return ADBC_STATUS_OK;
}
static AdbcStatusCode StatementNew(struct AdbcConnection *connection,
                                   struct AdbcStatement *statement, struct AdbcError *error) {
    statement->private_data = calloc(1, 1);
    return statement->private_data ? ADBC_STATUS_OK : ADBC_STATUS_INTERNAL;
}
static AdbcStatusCode StatementRelease(struct AdbcStatement *statement, struct AdbcError *error) {
    free(statement->private_data);
    statement->private_data = NULL;
    return ADBC_STATUS_OK;
}
static AdbcStatusCode SetSql(struct AdbcStatement *statement, const char *sql, struct AdbcError *error) {
    return ADBC_STATUS_OK;
}
static AdbcStatusCode Execute(struct AdbcStatement *statement, struct ArrowArrayStream *out,
                              int64_t *affected, struct AdbcError *error) {
    /* The command API must request ExecuteUpdate (no result stream). */
    if (out) return ADBC_STATUS_INVALID_ARGUMENT;
    counters[4]++;
    *affected = -1;
    return ADBC_STATUS_OK;
}
static AdbcStatusCode Prepare(struct AdbcStatement *statement, struct AdbcError *error) {
    counters[9]++;
    return ADBC_STATUS_NOT_IMPLEMENTED;
}
static AdbcStatusCode Release(struct AdbcDriver *driver, struct AdbcError *error) {
    return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcDriverTestInit(int version, void *out, struct AdbcError *error) {
    if (version != ADBC_VERSION_1_1_0) return ADBC_STATUS_NOT_IMPLEMENTED;
    struct AdbcDriver *driver = out;
    memset(driver, 0, sizeof(*driver));
    driver->release = Release;
    driver->DatabaseNew = DatabaseNew;
    driver->DatabaseInit = DatabaseInit;
    driver->DatabaseRelease = DatabaseRelease;
    driver->DatabaseSetOption = StringOption;
    driver->DatabaseSetOptionInt = IntOption;
    driver->DatabaseSetOptionDouble = DoubleOption;
    driver->DatabaseSetOptionBytes = BytesOption;
    driver->ConnectionNew = ConnectionNew;
    driver->ConnectionInit = ConnectionInit;
    driver->ConnectionRelease = ConnectionRelease;
    driver->StatementNew = StatementNew;
    driver->StatementRelease = StatementRelease;
    driver->StatementSetSqlQuery = SetSql;
    driver->StatementExecuteQuery = Execute;
    driver->StatementPrepare = Prepare;
    return ADBC_STATUS_OK;
}

/* Deliberately consume in BindStream, as a remote driver may do when uploading
 * bound batches. A producer that binds synchronously before feeding deadlocks. */
static AdbcStatusCode IngestOption(struct AdbcStatement *statement, const char *key,
                                   const char *value, struct AdbcError *error) {
    if (!strcmp(key, "adbc.ingest.target_table")) {
        *(char *)statement->private_data = !strcmp(value, "fail_bind") ? 1 :
                                           !strcmp(value, "fail_execute") ? 2 : 0;
    }
    return ADBC_STATUS_OK;
}
static AdbcStatusCode EagerBind(struct AdbcStatement *statement, struct ArrowArrayStream *input,
                               struct AdbcError *error) {
    struct ArrowArrayStream stream = *input;
    input->release = NULL;
    AdbcStatusCode status = ADBC_STATUS_OK;
    if (*(char *)statement->private_data == 1) {
        status = ADBC_STATUS_IO;
    } else {
        for (;;) {
            struct ArrowArray batch = {0};
            int result = stream.get_next(&stream, &batch);
            if (result) {
                if (batch.release) batch.release(&batch);
                status = ADBC_STATUS_IO;
                break;
            }
            if (!batch.release) break;
            counters[10] += (int)batch.length;
            counters[11]++;
            batch.release(&batch);
        }
    }
    stream.release(&stream);
    return status;
}
static AdbcStatusCode IngestExecute(struct AdbcStatement *statement, struct ArrowArrayStream *out,
                                    int64_t *affected, struct AdbcError *error) {
    if (*(char *)statement->private_data == 2) return ADBC_STATUS_IO;
    return Execute(statement, out, affected, error);
}
AdbcStatusCode AdbcDriverEagerInit(int version, void *out, struct AdbcError *error) {
    AdbcStatusCode status = AdbcDriverTestInit(version, out, error);
    if (status != ADBC_STATUS_OK) return status;
    struct AdbcDriver *driver = out;
    driver->StatementSetOption = IngestOption;
    driver->StatementBindStream = EagerBind;
    driver->StatementExecuteQuery = IngestExecute;
    return ADBC_STATUS_OK;
}
