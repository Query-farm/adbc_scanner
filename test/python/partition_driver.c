/* Synthetic ADBC partitions: independent streams, poison-on-release tokens and cancellation. */
#define _POSIX_C_SOURCE 200809L
#include <arrow-adbc/adbc.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static _Atomic int counts[16];
static char last_sql[4096];
struct DB {
    char mode[32];
};
struct Conn {
    struct DB *db;
    _Atomic int cancel;
    int active;
};
struct Stmt {
    struct Conn *conn;
    int projected;
    int64_t minimum;
};
struct Stream {
    struct Conn *conn;
    int partition, projected, emitted, metadata;
    int64_t minimum;
    char error[64];
};
int AdbcPartitionCounter(int i) {
    return atomic_load(&counts[i]);
}
const char *AdbcPartitionSQL(void) {
    return last_sql;
}
static void pause_ms(long ms) {
    struct timespec t = {ms / 1000, (ms % 1000) * 1000000};
    nanosleep(&t, NULL);
}
static void update_max(int value) {
    int old = counts[6];
    while (value > old && !atomic_compare_exchange_weak(&counts[6], &old, value)) {
    }
}
static void schema_release(struct ArrowSchema *s) {
    for (int64_t i = 0; i < s->n_children; i++) {
        s->children[i]->release(s->children[i]);
        free(s->children[i]);
    }
    free(s->children);
    s->release = NULL;
}
static void schema_node(struct ArrowSchema *s, const char *format, const char *name, int children) {
    memset(s, 0, sizeof(*s));
    s->format = format;
    s->name = name;
    s->flags = 2;
    s->n_children = children;
    s->release = schema_release;
    if (children) {
        s->children = calloc(children, sizeof(*s->children));
        for (int i = 0; i < children; i++)
            s->children[i] = calloc(1, sizeof(**s->children));
    }
}
static void data_schema(struct ArrowSchema *s, int projected) {
    schema_node(s, "+s", "", projected ? 1 : 2);
    schema_node(s->children[0], projected ? "d:18,2" : "l", projected ? "v" : "id", 0);
    if (!projected)
        schema_node(s->children[1], "d:18,2", "v", 0);
}
static void array_release(struct ArrowArray *a) {
    for (int64_t i = 0; i < a->n_children; i++) {
        a->children[i]->release(a->children[i]);
        free(a->children[i]);
    }
    for (int64_t i = 0; i < a->n_buffers; i++)
        free((void *)a->buffers[i]);
    free((void *)a->buffers);
    free(a->children);
    a->release = NULL;
}
static void array_node(struct ArrowArray *a, int64_t rows, int buffers, int children) {
    memset(a, 0, sizeof(*a));
    a->length = rows;
    a->n_buffers = buffers;
    a->n_children = children;
    a->release = array_release;
    a->buffers = calloc(buffers, sizeof(*a->buffers));
    if (children) {
        a->children = calloc(children, sizeof(*a->children));
        for (int i = 0; i < children; i++)
            a->children[i] = calloc(1, sizeof(**a->children));
    }
}
static void text_array(struct ArrowArray *a, const char *text) {
    array_node(a, 1, 3, 0);
    int32_t *offsets = calloc(2, sizeof(int32_t));
    offsets[1] = (int32_t)strlen(text);
    a->buffers[1] = offsets;
    a->buffers[2] = strdup(text);
}
static void metadata_array(struct ArrowArray *a) {
    array_node(a, 1, 1, 2);
    text_array(a->children[0], "main");
    struct ArrowArray *list = a->children[1];
    array_node(list, 1, 2, 1);
    int32_t *offsets = calloc(2, 4);
    offsets[1] = 1;
    list->buffers[1] = offsets;
    struct ArrowArray *schemas = list->children[0];
    array_node(schemas, 1, 1, 2);
    text_array(schemas->children[0], "main");
    list = schemas->children[1];
    array_node(list, 1, 2, 1);
    offsets = calloc(2, 4);
    offsets[1] = 1;
    list->buffers[1] = offsets;
    struct ArrowArray *tables = list->children[0];
    array_node(tables, 1, 1, 2);
    text_array(tables->children[0], "items");
    text_array(tables->children[1], "TABLE");
}
static int get_schema(struct ArrowArrayStream *out, struct ArrowSchema *s) {
    struct Stream *st = out->private_data;
    if (st->metadata) {
        schema_node(s, "+s", "", 2);
        schema_node(s->children[0], "u", "catalog_name", 0);
        struct ArrowSchema *list = s->children[1];
        schema_node(list, "+l", "catalog_db_schemas", 1);
        struct ArrowSchema *schemas = list->children[0];
        schema_node(schemas, "+s", "item", 2);
        schema_node(schemas->children[0], "u", "db_schema_name", 0);
        list = schemas->children[1];
        schema_node(list, "+l", "db_schema_tables", 1);
        struct ArrowSchema *tables = list->children[0];
        schema_node(tables, "+s", "item", 2);
        schema_node(tables->children[0], "u", "table_name", 0);
        schema_node(tables->children[1], "u", "table_type", 0);
        return 0;
    }
    data_schema(s, st->projected);
    if (st->conn && !strcmp(st->conn->db->mode, "schema") && st->partition == 1)
        s->children[0]->format = "i";
    return 0;
}
static int get_next(struct ArrowArrayStream *out, struct ArrowArray *a) {
    struct Stream *st = out->private_data;
    memset(a, 0, sizeof(*a));
    if (st->metadata) {
        if (!st->emitted++)
            metadata_array(a);
        return 0;
    }
    if (!strcmp(st->conn->db->mode, "limit_peer") && st->partition != 0) {
        while (!atomic_load(&st->conn->cancel))
            pause_ms(1);
        strcpy(st->error, "synthetic limited peer cancellation");
        return ECANCELED;
    }
    if (!strcmp(st->conn->db->mode, "peer_error")) {
        if (st->partition == 0) {
            int tries = 0;
            while (counts[5] < 2 && tries++ < 2000)
                pause_ms(1);
            strcpy(st->error, "synthetic first partition source failure");
            return EIO;
        }
        while (!atomic_load(&st->conn->cancel))
            pause_ms(1);
        strcpy(st->error, "synthetic peer cancellation");
        return ECANCELED;
    }
    if (!strcmp(st->conn->db->mode, "block")) {
        while (!atomic_load(&st->conn->cancel))
            pause_ms(1);
        strcpy(st->error, "synthetic cancelled partition");
        return ECANCELED;
    }
    if (st->emitted) {
        if (!strcmp(st->conn->db->mode, "error_after") && st->partition == 1) {
            strcpy(st->error, "synthetic late partition failure");
            return EIO;
        }
        return 0;
    }
    st->emitted = 1;
    pause_ms(10);
    if (atomic_load(&st->conn->cancel)) {
        strcpy(st->error, "synthetic cancelled partition");
        return ECANCELED;
    }
    int64_t ids[16];
    int n = 0;
    for (int i = 0; i < (st->partition < 0 ? 16 : 4); i++) {
        int64_t id = st->partition < 0 ? i : st->partition * 4 + i;
        if (id >= st->minimum)
            ids[n++] = id;
    }
    array_node(a, n, 1, st->projected ? 1 : 2);
    if (!st->projected) {
        struct ArrowArray *id = a->children[0];
        array_node(id, n, 2, 0);
        int64_t *values = calloc(n ? n : 1, 8);
        memcpy(values, ids, n * 8);
        id->buffers[1] = values;
    }
    struct ArrowArray *v = a->children[st->projected ? 0 : 1];
    array_node(v, n, 2, 0);
    uint8_t *valid = calloc((n + 7) / 8 ? (n + 7) / 8 : 1, 1);
    uint8_t *values = calloc(n ? n : 1, 16);
    for (int i = 0; i < n; i++) {
        if (ids[i] % 3) {
            valid[i / 8] |= (uint8_t)(1u << (i % 8));
        } else
            v->null_count++;
        int64_t scaled = (ids[i] % 4) * 125;
        memcpy(values + i * 16, &scaled, 8);
    }
    v->buffers[0] = valid;
    v->buffers[1] = values;
    return 0;
}
static const char *get_error(struct ArrowArrayStream *s) {
    struct Stream *st = s->private_data;
    return st->error[0] ? st->error : NULL;
}
static void stream_release(struct ArrowArrayStream *s) {
    struct Stream *st = s->private_data;
    if (st->conn) {
        st->conn->active = 0;
        counts[5]--;
        counts[8]++;
    }
    free(st);
    memset(s, 0, sizeof(*s));
}
static void make_stream(struct ArrowArrayStream *out, struct Stream *st) {
    memset(out, 0, sizeof(*out));
    out->private_data = st;
    out->get_schema = get_schema;
    out->get_next = get_next;
    out->get_last_error = get_error;
    out->release = stream_release;
}
static AdbcStatusCode database_new(struct AdbcDatabase *d, struct AdbcError *e) {
    d->private_data = calloc(1, sizeof(struct DB));
    counts[0]++;
    return ADBC_STATUS_OK;
}
static AdbcStatusCode database_option(struct AdbcDatabase *d, const char *k, const char *v, struct AdbcError *e) {
    if (!strcmp(k, "mode"))
        snprintf(((struct DB *)d->private_data)->mode, 32, "%s", v);
    return ADBC_STATUS_OK;
}
static AdbcStatusCode database_init(struct AdbcDatabase *d, struct AdbcError *e) {
    return ADBC_STATUS_OK;
}
static AdbcStatusCode database_release(struct AdbcDatabase *d, struct AdbcError *e) {
    if (counts[2] != counts[3])
        counts[13]++;
    free(d->private_data);
    d->private_data = NULL;
    counts[1]++;
    return ADBC_STATUS_OK;
}
static AdbcStatusCode connection_new(struct AdbcConnection *c, struct AdbcError *e) {
    c->private_data = calloc(1, sizeof(struct Conn));
    counts[2]++;
    return ADBC_STATUS_OK;
}
static AdbcStatusCode connection_init(struct AdbcConnection *c, struct AdbcDatabase *d, struct AdbcError *e) {
    ((struct Conn *)c->private_data)->db = d->private_data;
    return ADBC_STATUS_OK;
}
static AdbcStatusCode connection_release(struct AdbcConnection *c, struct AdbcError *e) {
    struct Conn *conn = c->private_data;
    if (conn->active)
        counts[15]++;
    free(conn);
    c->private_data = NULL;
    counts[3]++;
    return ADBC_STATUS_OK;
}
static AdbcStatusCode connection_cancel(struct AdbcConnection *c, struct AdbcError *e) {
    atomic_store(&((struct Conn *)c->private_data)->cancel, 1);
    counts[9]++;
    return ADBC_STATUS_OK;
}
static AdbcStatusCode objects(struct AdbcConnection *c, int depth, const char *cat, const char *schema,
                              const char *table, const char **types, const char *col, struct ArrowArrayStream *out,
                              struct AdbcError *e) {
    struct Stream *st = calloc(1, sizeof(*st));
    st->metadata = 1;
    make_stream(out, st);
    return ADBC_STATUS_OK;
}
static AdbcStatusCode table_schema(struct AdbcConnection *c, const char *cat, const char *schema, const char *table,
                                   struct ArrowSchema *s, struct AdbcError *e) {
    data_schema(s, 0);
    return ADBC_STATUS_OK;
}
static AdbcStatusCode statement_new(struct AdbcConnection *c, struct AdbcStatement *s, struct AdbcError *e) {
    struct Stmt *st = calloc(1, sizeof(*st));
    st->conn = c->private_data;
    s->private_data = st;
    return ADBC_STATUS_OK;
}
static AdbcStatusCode statement_release(struct AdbcStatement *s, struct AdbcError *e) {
    if (counts[5])
        counts[12]++;
    free(s->private_data);
    s->private_data = NULL;
    return ADBC_STATUS_OK;
}
static AdbcStatusCode set_sql(struct AdbcStatement *s, const char *q, struct AdbcError *e) {
    snprintf(last_sql, sizeof(last_sql), "%s", q);
    ((struct Stmt *)s->private_data)->projected = strstr(q, "SELECT \"v\" FROM") != NULL;
    return ADBC_STATUS_OK;
}
static AdbcStatusCode prepare(struct AdbcStatement *s, struct AdbcError *e) {
    return ADBC_STATUS_OK;
}
static AdbcStatusCode bind(struct AdbcStatement *s, struct ArrowArray *v, struct ArrowSchema *schema,
                           struct AdbcError *e) {
    if (v->n_children && v->children[0]->buffers[1]) {
        struct ArrowSchema *t = schema->children[0];
        if (!strcmp(t->format, "i"))
            ((struct Stmt *)s->private_data)->minimum = *(int32_t *)v->children[0]->buffers[1];
        else
            ((struct Stmt *)s->private_data)->minimum = *(int64_t *)v->children[0]->buffers[1];
    }
    if (v->release)
        v->release(v);
    if (schema->release)
        schema->release(schema);
    counts[14]++;
    return ADBC_STATUS_OK;
}
struct Tokens {
    uint8_t **data;
    size_t *lengths;
};
static void parts_release(struct AdbcPartitions *p) {
    struct Tokens *t = p->private_data;
    for (size_t i = 0; i < p->num_partitions; i++) {
        memset(t->data[i], 255, 16);
        free(t->data[i]);
    }
    free(t->data);
    free(t->lengths);
    free(t);
    memset(p, 0, sizeof(*p));
    counts[10]++;
}
static void empty_parts_release(struct AdbcPartitions *p) {
    memset(p, 0, sizeof(*p));
    counts[10]++;
}
static AdbcStatusCode execute_parts(struct AdbcStatement *s, struct ArrowSchema *schema, struct AdbcPartitions *p,
                                    int64_t *rows, struct AdbcError *e) {
    struct Stmt *st = s->private_data;
    const char *mode = st->conn->db->mode;
    counts[4]++;
    if (!strcmp(mode, "unsupported"))
        return ADBC_STATUS_NOT_IMPLEMENTED;
    if (!strcmp(mode, "error"))
        return ADBC_STATUS_IO;
    data_schema(schema, st->projected);
    if (!strcmp(mode, "partial_unsupported"))
        return ADBC_STATUS_NOT_IMPLEMENTED;
    if (!strcmp(mode, "too_many") || !strcmp(mode, "malformed")) {
        p->num_partitions = !strcmp(mode, "too_many") ? 65537 : 1;
        p->release = empty_parts_release;
        return ADBC_STATUS_OK;
    }
    size_t n = !strcmp(mode, "empty") ? 0 : 4;
    struct Tokens *t = calloc(1, sizeof(*t));
    t->data = calloc(n ? n : 1, sizeof(uint8_t *));
    t->lengths = calloc(n ? n : 1, sizeof(size_t));
    for (size_t i = 0; i < n; i++) {
        t->data[i] = calloc(16, 1);
        t->data[i][0] = (uint8_t)i;
        t->data[i][1] = (uint8_t)st->projected;
        memcpy(t->data[i] + 8, &st->minimum, 8);
        t->lengths[i] = 16;
    }
    if (!strcmp(mode, "too_large"))
        t->lengths[0] = 64 * 1024 * 1024 + 1;
    p->num_partitions = n;
    p->partitions = (const uint8_t **)t->data;
    p->partition_lengths = t->lengths;
    p->private_data = t;
    p->release = parts_release;
    *rows = -1;
    return ADBC_STATUS_OK;
}
static AdbcStatusCode read_part(struct AdbcConnection *c, const uint8_t *token, size_t n, struct ArrowArrayStream *out,
                                struct AdbcError *e) {
    struct Conn *conn = c->private_data;
    if (n != 16 || token[0] > 3 || conn->active)
        return ADBC_STATUS_INVALID_DATA;
    struct Stream *st = calloc(1, sizeof(*st));
    st->conn = conn;
    st->partition = token[0];
    st->projected = token[1];
    memcpy(&st->minimum, token + 8, 8);
    conn->active = 1;
    atomic_store(&conn->cancel, 0);
    counts[7]++;
    update_max(++counts[5]);
    make_stream(out, st);
    if (!strcmp(conn->db->mode, "block_open")) {
        while (!atomic_load(&conn->cancel))
            pause_ms(1);
        return ADBC_STATUS_CANCELLED;
    }
    return ADBC_STATUS_OK;
}
static AdbcStatusCode execute(struct AdbcStatement *s, struct ArrowArrayStream *out, int64_t *rows,
                              struct AdbcError *e) {
    struct Stmt *st = s->private_data;
    counts[11]++;
    if (!out)
        return ADBC_STATUS_INVALID_ARGUMENT;
    struct Stream *stream = calloc(1, sizeof(*stream));
    stream->conn = st->conn;
    stream->partition = -1;
    stream->projected = st->projected;
    stream->minimum = st->minimum;
    st->conn->active = 1;
    update_max(++counts[5]);
    make_stream(out, stream);
    return ADBC_STATUS_OK;
}
static AdbcStatusCode driver_release(struct AdbcDriver *d, struct AdbcError *e) {
    return ADBC_STATUS_OK;
}
AdbcStatusCode AdbcDriverPartitionTestInit(int version, void *out, struct AdbcError *e) {
    if (version != ADBC_VERSION_1_1_0)
        return ADBC_STATUS_NOT_IMPLEMENTED;
    struct AdbcDriver *d = out;
    memset(d, 0, sizeof(*d));
    d->release = driver_release;
    d->DatabaseNew = database_new;
    d->DatabaseInit = database_init;
    d->DatabaseSetOption = database_option;
    d->DatabaseRelease = database_release;
    d->ConnectionNew = connection_new;
    d->ConnectionInit = connection_init;
    d->ConnectionRelease = connection_release;
    d->ConnectionCancel = connection_cancel;
    d->ConnectionGetObjects = objects;
    d->ConnectionGetTableSchema = table_schema;
    d->ConnectionReadPartition = read_part;
    d->StatementNew = statement_new;
    d->StatementRelease = statement_release;
    d->StatementSetSqlQuery = set_sql;
    d->StatementPrepare = prepare;
    d->StatementBind = bind;
    d->StatementExecuteQuery = execute;
    d->StatementExecutePartitions = execute_parts;
    return ADBC_STATUS_OK;
}
