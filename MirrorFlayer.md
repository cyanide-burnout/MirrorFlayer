# MirrorFlayer

A Tarantool module that replicates MySQL/MariaDB tables into Tarantool spaces
over the replication protocol (binlog). It runs inside Tarantool itself, in
fibers: no separate process, no YAML, no MySQL sources or client library.

It replaces a Python `replicatord` (pymysqlreplication) that fed Tarantool
from a MariaDB/Galera cluster, and keeps the values that replicator wrote, so
the code reading the spaces sees no difference.

Status: first slice — MariaDB without TLS, `mysql_native_password`, initial
load and binlog stream with the position kept in a space. See *Plan*.

## Usage

```lua
local mirrorflayer = require('mirrorflayer')

local replica = mirrorflayer.new({
  hosts = { 'db1.example.com:3306', 'db2.example.com:3306' },
  user = 'replica', password = '...', server = 1001,
  tls = false, position = 'ReplicatorPositions'
})
replica:map('Registry', 'Locations', 'GlobalLocations', { 'ID', 'Node', 'Kind', 'Link', 'Slot', 'Modified' })
replica:start()

replica:stat()   -- status, position, lag, counters, last error
replica:stop()
```

Configuration is Lua only.

| Option      | Default        | Meaning |
|-------------|----------------|---------|
| `hosts`     | —              | `'host[:port]'` list; the next one is tried after a failure (Galera nodes) |
| `user`, `password` | —       | Replication user: `REPLICATION SLAVE`, `BINLOG MONITOR` (`REPLICATION CLIENT`), `SELECT` on the mapped tables |
| `server`    | —              | Server id of this replica, unique among the replicas of the source |
| `tls`       | `false`        | TLS (`mirrorflayer.tls`), not implemented yet |
| `position`  | `'MirrorFlayer'` | Space that keeps the position, created when missing |
| `name`      | `'mirrorflayer'` | Key of the position in that space, name in logs and metrics |
| `heartbeat` | `10`           | Seconds between heartbeats of an idle source; no data for three periods reconnects |
| `timeout`   | `30`           | Connect and query timeout, seconds |
| `batch`     | `1000`         | Rows per transaction during a full load |
| `retry`     | `5`            | Seconds between connection attempts |

### Mapping

`replica:map(database, table, target, columns)` — only before `start()`.

* `target` is a space name, or a function `function(operation, old, new)`.
* `columns` are the source column names in tuple order. Without them the field
  names of `space:format()` are used (spaces without a format need the list).
* The key is the primary index of the space; there is no `key_fields`.
* One table may go to several targets (call `map` several times).
* Spaces are written with ordinary `replace`/`delete`, so `on_replace`
  triggers keep working.

A function target receives `'insert' | 'update' | 'delete'` with the old and
new tuples (Lua arrays in `columns` order, `nil` where absent), and `'reset'`
with no tuples before a full reload, followed by `'insert'` for every row of
the snapshot. It is called **inside the box transaction**: it must not yield
(no network, no `fiber.sleep`, no waiting on channels), or the transaction
is aborted.

## Values

One conversion table (`mirrorflayer.column`) serves both the text result of
the initial load and binlog row images, so a row gives the same tuple either
way. Values follow the Python replicator:

| MySQL type | Tarantool value |
|------------|-----------------|
| integers, `YEAR` | number; `uint64_t`/`int64_t` cdata beyond 2^53 |
| `FLOAT` | number, rounded to single precision on both paths |
| `DOUBLE` | number |
| `DECIMAL` | `decimal` |
| `DATETIME`, `TIMESTAMP` | string `'YYYY-MM-DD HH:MM:SS'`, fractions dropped; `TIMESTAMP` in UTC (the session time zone is `+00:00`, the binlog keeps UTC epoch seconds) |
| `DATE`, `TIME` | `'YYYY-MM-DD'`, `'HH:MM:SS'` |
| zero dates | `box.NULL` |
| `CHAR`, `VARCHAR`, `TEXT`, `JSON` (MariaDB) | string |
| `BINARY`, `VARBINARY`, `BLOB` | `varbinary` (MP_BIN), as Python `bytes` were written |
| `ENUM`, `SET` | member string(s) |
| `BIT` | number |
| `NULL` | `box.NULL` |

Binlog row images carry no names, signedness or member lists; they come from
`information_schema.COLUMNS` on every connection and again after DDL. Types
not listed (MariaDB `UUID`, `INET6`, geometry) are refused when mapped.

## Behaviour

* **Transactions.** One binlog transaction (GTID … XID/COMMIT) is collected in
  memory and applied in one `box.begin()`/`box.commit()` together with the new
  position. Decoding and metadata queries happen outside the box transaction.
  Transactions that touch no mapped table only advance the position in memory;
  it is written at most once a second and on heartbeats.
* **Position.** A tuple `{ name, gtid_state, event_time }` in the `position`
  space, the GTID state string as MariaDB prints it (`0-1-100,1-2-5`). On the
  first start, without its own tuple, the position of the Python replicator —
  `[0, gtid, 0]` in `ReplicatorPositions` — is taken. If the position space has
  a numeric primary key (the layout Registry creates for `ReplicatorPositions`),
  the position is kept there under key `0` as `{ 0, gtid_state, event_time }`.
  Python stored the GTID of a transaction before applying it, so continuing from
  its position skips the transaction it may have been applying when it stopped.
* **Full load** (first start, error 1236, `TRUNCATE` of a mapped table, a table
  map that does not match the table definition): the GTID state
  (`@@gtid_binlog_pos`) is read first, then each table is selected with
  `COM_QUERY` and streamed; rows are written over the space in batches with
  `fiber.yield()` between them, then rows absent from the snapshot are deleted,
  so spaces never go empty. Changes made during the load come again from the
  binlog; applying is idempotent (`INSERT` → `replace`, `DELETE` of a missing
  row does nothing), so the overlap is safe.
* **Errors.** 1236 (the position is no longer in the binlog) → full load.
  Network and server errors → reconnect to the next host after `retry`
  seconds. A row image that is not FULL (`binlog_row_image` must be `FULL`),
  compressed binlog events, MySQL, TLS and unsupported column types stop the
  replicator with an error in `stat().error` and the log.
* **Metrics.** `stat()` returns `event_time` (source time of the last applied
  transaction), `applied_time`, `lag` (their difference, 0 after a heartbeat
  of an idle source), counters and the last error. With the `metrics` module
  the gauges `mirrorflayer_lag_seconds`, `mirrorflayer_event_timestamp_seconds`
  and `mirrorflayer_applied_timestamp_seconds` (label `name`) are exported.

Requirements on the source: `log_bin`, `binlog_format=ROW`,
`binlog_row_image=FULL`, GTID (always on in MariaDB). Tarantool 3.x (LuaJIT).

## Modules

| Module | Role |
|--------|------|
| `mirrorflayer` (`init.lua`) | API, replication fiber, reconnection, host rotation, event dispatch |
| `mirrorflayer.transport` | `read(n)`/`write(s)`/`close()`; plain TCP over the Tarantool `socket` |
| `mirrorflayer.tls` (`tls.so`, C) | optional, later: TLS over the already connected descriptor, RSA-OAEP for `caching_sha2_password` |
| `mirrorflayer.protocol` | packets, greeting, authentication, `COM_QUERY` with a streamed text result |
| `mirrorflayer.mariadb` | MariaDB dialect: GTID format, GTID event, stream request |
| `mirrorflayer.mysql` | MySQL dialect, later: GTID sets, `COM_BINLOG_DUMP_GTID` |
| `mirrorflayer.binlog` | events: FORMAT_DESCRIPTION, ROTATE, QUERY, XID, TABLE_MAP, ROWS v1/v2, CRC32 |
| `mirrorflayer.column` | type metadata and values, NULL bitmaps |
| `mirrorflayer.store` | targets, full load, position |

### Stream request (MariaDB)

```
SET @master_binlog_checksum = @@GLOBAL.binlog_checksum
SET @mariadb_slave_capability = 4
SET @slave_connect_state = '<gtid state>'
SET @slave_gtid_strict_mode = 0, @slave_gtid_ignore_duplicates = 0
SET @master_heartbeat_period = <ns>
COM_BINLOG_DUMP (position 4, flags 0, server id, no file name)
```

MySQL will use `COM_BINLOG_DUMP_GTID` with an executed GTID set.

### TLS

MySQL starts TLS after the greeting on the same TCP connection (SSL Request
packet), so `mirrorflayer.tls` takes over the descriptor of the connected
Tarantool socket (`sock:fd()`, `SSL_set_fd`), stays non-blocking and waits with
`coio_wait()` on `SSL_ERROR_WANT_READ`/`WANT_WRITE`. It also provides RSA-OAEP
for `caching_sha2_password` without TLS. It is built by the Makefile unless
`TLS=0`. C rules: short-lived buffers on the stack or from the caller, no
`malloc`/`strdup`; locals at the top of a function.

#### OpenSSL in the tarantool binary

Checked on 2026-11-24 to decide how `tls.so` links, so that the process does
not end up with two conflicting OpenSSL copies.

The `tarantool/tarantool:3` Docker image (Tarantool 3.8.2) and the `tarantool
3.9.0-1` Debian package of the Tarantool repository on Debian 13 are the same
kind of build:

```
Tarantool 3.9.0-0-g8b9a0f59a95b
Target: Linux-x86_64-RelWithDebInfo
Build options: cmake . -DCMAKE_INSTALL_PREFIX=/tarantool/static-build/tarantool-prefix -DENABLE_BACKTRACE=TRUE
Compiler: GNU-9.3.1
```

* It is the *static build*: OpenSSL 3.2.1 (`OpenSSL 3.2.1 30 Jan 2024` in
  the binary) is linked in statically. `ldd` shows only libc, libm, libdl,
  librt, libpthread and libgcc_s — no libssl/libcrypto.
* The dynamic symbol table exports about 690 symbols (`box_*`, `lua_*`,
  `luaL_*`, `fiber_*`, `coio_*`, `curl_*` and Tarantool's own `crypto_*`
  wrappers such as `crypto_EVP_DigestInit_ex`), and **no** OpenSSL symbols:
  `nm -D --defined-only` finds 0 `SSL_*`, `EVP_*`, `RSA_*`, `BIO_*`, `X509_*`.
  `coio_wait` is exported.
* The system has its own OpenSSL: `libssl3t64 3.5.7` on Debian 13.

Consequences for `tls.so`:

* It cannot use the OpenSSL inside tarantool: nothing to link against.
* Linking `tls.so` with the system `libssl.so.3`/`libcrypto.so.3` gives a
  second, independent OpenSSL in the process. Since the embedded copy is not
  exported, the dynamic linker cannot mix the two (no symbol interposition in
  either direction); each copy has its own state and they never exchange
  objects. Tarantool's own TLS (Enterprise iproto, `http.client` via the
  embedded curl) stays on the embedded copy.
* Alternative: link OpenSSL statically into `tls.so` with hidden visibility
  (`-Wl,--exclude-libs,ALL`), so the module does not depend on the system
  libssl version. Decide when `tls.so` is written; the system library is the
  simpler default for a Debian package (`Depends: libssl3t64`).
* A Tarantool built against the system OpenSSL (e.g. distribution packages)
  would export the `SSL_*` symbols; `tls.so` linked with the same system
  library then shares that single copy, which is also fine.

## Build and install

```
make install [PREFIX=/usr] [DESTDIR=...]   # Lua modules into $(PREFIX)/share/tarantool/mirrorflayer
make check                                 # syntax check with tarantool
make test                                  # Docker test, see test/
```

`TLS=0` (the default for now) builds without `tls.so`.

## Tests

`test/run.sh` runs MariaDB (`log_bin`, `binlog_format=ROW`,
`binlog_row_image=FULL`) and Tarantool 3 in Docker and drives
`test/scenario.lua` through phases:

1. `load` — tables with data exist before the start: full load, values of all
   types, then INSERT/UPDATE/DELETE over the stream, a key change, a
   multi-table transaction, `on_replace` triggers and a function target.
2. `resume` — changes made while Tarantool is down; restart continues from the
   stored GTID without a full load.
3. `purge` — changes and a purge of the binlog while down: error 1236, full
   load that also deletes rows gone from the source.
4. `legacy` — a fresh instance with the Python position `[0, gtid, 0]` in a
   numeric-keyed `ReplicatorPositions` continues from it.

`test/remote.sh [host]` copies the tree to a Docker host and
runs the test there.

## Plan

1. ~~First slice: MariaDB, no TLS, native password, load, stream, position.~~
2. Run beside the Python replicator in production and compare spaces.
3. `caching_sha2_password` and TLS (`tls.so`), `client_ed25519` later.
4. MySQL dialect (`COM_BINLOG_DUMP_GTID`, MySQL JSON).
5. Column names and signedness from optional TABLE_MAP metadata
   (`binlog_row_metadata=FULL`) instead of `information_schema`, which reflects
   the current definition rather than the one of an old event.
6. Debian package.
