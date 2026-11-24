# MirrorFlayer

A Tarantool module that replicates MySQL/MariaDB tables into Tarantool spaces
over the replication protocol (binlog). It runs inside Tarantool itself, in
fibers: no separate process, no YAML, no MySQL sources or client library.

It replaces a Python `replicatord` (pymysqlreplication) that fed Tarantool
from a MariaDB/Galera cluster, and keeps the values that replicator wrote, so
the code reading the spaces sees no difference.

Status: first slice — MariaDB without TLS, `mysql_native_password`, initial
load and binlog stream with the position kept in a space. Internals,
OpenSSL findings and the plan are in [Documentation/Design.md](Documentation/Design.md).

Requires Tarantool 3.x and a source with `log_bin`, `binlog_format=ROW`,
`binlog_row_image=FULL` (MariaDB keeps GTIDs always).

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

## Build and install

`Source/` holds the Lua modules; they are installed as
`$(PREFIX)/share/tarantool/mirrorflayer/`, which is what `require('mirrorflayer')`
finds. File names stay lowercase because they are the module names.

```
make install [PREFIX=/usr] [DESTDIR=...]   # Lua modules into $(PREFIX)/share/tarantool/mirrorflayer
make check                                 # syntax check with tarantool
make test                                  # Docker test, see Tests/
```

`TLS=0` (the default for now) builds without `tls.so`.

## Tests

`Tests/DockerTest.sh` runs MariaDB (`log_bin`, `binlog_format=ROW`,
`binlog_row_image=FULL`) and Tarantool 3 in Docker and drives
`Tests/MirrorFlayerTest.lua` through phases:

1. `load` — tables with data exist before the start: full load, values of all
   types, then INSERT/UPDATE/DELETE over the stream, a key change, a
   multi-table transaction, `on_replace` triggers and a function target.
2. `resume` — changes made while Tarantool is down; restart continues from the
   stored GTID without a full load.
3. `purge` — changes and a purge of the binlog while down: error 1236, full
   load that also deletes rows gone from the source.
4. `legacy` — a fresh instance with the Python position `[0, gtid, 0]` in a
   numeric-keyed `ReplicatorPositions` continues from it.

`Tests/RemoteTest.sh [host]` copies the tree to a Docker host and
runs the test there.

