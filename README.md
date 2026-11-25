# MirrorFlayer

A Tarantool module that replicates MySQL, MariaDB and PostgreSQL tables into
Tarantool spaces over their replication protocols: the binlog of MySQL and
MariaDB, logical replication with pgoutput of PostgreSQL. It runs inside
Tarantool itself, in fibers: no separate process, no YAML, no client library.

The goal of the project is to replace
[tarantool/mysql-tarantool-replication](https://github.com/tarantool/mysql-tarantool-replication)
— `replicatord`, a separate C++ daemon built on libslave and the MySQL client
library, configured in YAML and writing into Tarantool over the network —
with a replicator that lives inside Tarantool, where every value gets its
Tarantool type.

MariaDB, MySQL and PostgreSQL, with or without TLS; `mysql_native_password`,
`caching_sha2_password`, `sha256_password`, `SCRAM-SHA-256` and `md5`.
Internals and OpenSSL findings are in
[Documentation/Design.md](Documentation/Design.md).

Requires Tarantool 3.x and

* MySQL or MariaDB with `log_bin`, `binlog_format=ROW`,
  `binlog_row_image=FULL` and GTIDs: always there in MariaDB, `gtid_mode=ON`
  in MySQL. MySQL must not compress transactions
  (`binlog_transaction_compression=OFF`, the default) nor log partial JSON
  updates (`binlog_row_value_options` empty, the default);
* or PostgreSQL 12 or later with `wal_level=logical`, free replication slots
  and WAL senders, and a publication of the mapped tables.

## 2.0

2.0 breaks with the first release in what applications read:

* Values have their Tarantool types:
  * `DATETIME`, `TIMESTAMP` and `DATE` are `datetime` in UTC; with
    `datetime = 'number'` they are seconds since the Unix epoch.
  * `TIME` is `interval`; with `datetime = 'number'` it is a duration in
    seconds.
  * JSON is MsgPack maps and arrays; `json = 'string'` keeps the text.
  * UUID is `uuid`; `uuid = 'binary' | 'string'` gives 16 bytes or the text.

  Code that parsed dates from strings reads them directly: `value.timestamp`
  instead of `datetime.parse(value, ...)`, and a space format may type the
  field as `datetime`.
* Sources are drivers: `type = 'mysql'` (the default) or `'postgresql'`. The
  MySQL modules moved to `MirrorFlayer.MySQL.*` in the `MySQL/` subdirectory;
  remove an installed `MirrorFlayer/MySQL.lua` of the first release, it would
  win `require('MirrorFlayer.MySQL')`.
* The position of `replicatord` is not read any more: a replicator without a
  position of its own starts with a full load, written over the spaces.

## Usage

```lua
local MirrorFlayer = require('MirrorFlayer')

local replica = MirrorFlayer.new({
  hosts = { 'db1.example.com:3306', 'db2.example.com:3306' },
  user = 'replica', password = '...', server = 1001,
  tls = false, position = 'ReplicatorPositions'
})
replica:map('Registry', 'Locations', 'GlobalLocations', { 'ID', 'Node', 'Kind', 'Link', 'Slot', 'Modified' })
replica:start()

replica:stat()   -- status, host, position, lag, event time, counters (transactions, rows, loads, fills, split, aborts), last error
replica:reload() -- restart with a full load over the spaces
replica:reload(true) -- the same, each space emptied right before its table is loaded
replica:stop()
```

PostgreSQL, with a slot and a publication of its own:

```lua
local replica = MirrorFlayer.new({
  type = 'postgresql', hosts = { 'pg.example.com' },
  user = 'replica', password = '...', database = 'registry',
  slot = 'mirrorflayer', publication = 'mirrorflayer'
})
replica:map('public', 'locations', 'GlobalLocations')
replica:start()
```

Configuration is Lua only.

| Option      | Default        | Meaning |
|-------------|----------------|---------|
| `type`      | `'mysql'`      | Source: `'mysql'` (MySQL and MariaDB) or `'postgresql'` |
| `hosts`     | —              | `'host[:port]'` list, port 3306 or 5432 by default; the next one is tried after a failure (Galera nodes) |
| `user`, `password` | —       | MySQL: `REPLICATION SLAVE`, `BINLOG MONITOR` (`REPLICATION CLIENT`), `SELECT` on the mapped tables. PostgreSQL: a role with `REPLICATION`, `USAGE` on the schemas and `SELECT` on the tables |
| `server`    | —              | MySQL: server id of this replica, unique among the replicas of the source |
| `database`  | `user`         | PostgreSQL: the database |
| `slot`      | `'mirrorflayer'` | PostgreSQL: the logical replication slot, one per replicator |
| `publication` | `'mirrorflayer'` | PostgreSQL: the publication of the mapped tables (`CREATE PUBLICATION mirrorflayer FOR TABLE ...`) |
| `tls`       | `false`        | `true` for TLS without verification, or `{ verify = 'none' \| 'ca' \| 'identity', ca = 'ca.pem', cert = 'client.pem', key = 'client.key' }`; with `ca` verification defaults to `'ca'`, without it to `'none'`; `'ca'` and `'identity'` without `ca` use the system store. Needs `MirrorFlayer.TLS` |
| `public_key` | —             | PEM public key of the source for `caching_sha2_password`/`sha256_password` without TLS; without it the key is asked from the server (open to a man in the middle) |
| `position`  | `'ReplicatorPositions'` | Space that keeps the position, created when missing with a key of the type of `key`; the tuple is `{ key, position, event time, type of the source }`, a position of another type under the same key is not taken (a full load follows) |
| `key`       | `server`, `slot` | Key of the position in that space: a stable number or string of this replicator, the replica server id of MySQL or the slot of PostgreSQL by default; two replicators of an instance with the same `position` and `key` are refused by `start()`, so replicators of different sources with the same server id or slot need a `key` of their own |
| `datetime`  | `'datetime'`   | Dates and times: `'datetime'` (`datetime` and `interval` of Tarantool) or `'number'` (seconds), see *Values* |
| `json`      | `'map'`        | JSON: `'map'` (MsgPack maps and arrays) or `'string'` (the text the server prints), see *Values* |
| `uuid`      | `'uuid'`       | UUID: `'uuid'` (`uuid` of Tarantool), `'binary'` (16 bytes in network byte order, `varbinary`) or `'string'` (canonical text, lowercase) |
| `name`      | `'MirrorFlayer'` | Name in logs, the label of metrics and the name of the replicator in its thread: unique among the replicators of an instance, `start()` refuses a second one |
| `heartbeat` | `10`           | Seconds between heartbeats of an idle source; no data for three periods reconnects |
| `check`     | `30`           | Galera: seconds between checks that the node is ready (`wsrep_ready`, primary component); a node that is not is left for the next host, on connect as well |
| `timeout`   | `30`           | Connect and query timeout, seconds |
| `batch`     | `100`          | Statements per transaction during a full load; the fiber yields after each |
| `retry`     | `5`            | Seconds between connection attempts |
| `thread`    | —              | Name of a group of application threads (Tarantool 3.9 or later, `box.cfg{ app_threads = N }` makes the group `app`): the source side runs in a thread of it, TX only applies; see *Threads* |
| `thread_id` | `1`            | Which thread of that group |

### Mapping

`replica:map(database, table, target, columns)` — only before `start()`.
For PostgreSQL `database` is the schema; schema, table and column names are
the ones of the catalogue, case-sensitive.
Targets are looked up by the replication fiber, so `start()` may come before
the spaces are made; a missing space is retried with the connection, other
mapping errors stop the replicator (`stat().error`).

* `target` is a space name, or a function `function(operation, old, new)`.
* `columns` are the source column names in tuple order. Without them the field
  names of `space:format()` are used (spaces without a format need the list).
* The key is the primary index of the space; there is no `key_fields`.
* One table may go to several targets (call `map` several times).
* A table whose spaces are all empty while the position is there (a space
  made anew, emptied by hand) is loaded alone before the stream, without a
  full load of the others (`stat().fills`); the stream from the stored
  position converges over it. `replica:reload()` reloads everything over
  the spaces, which never go empty: rows gone from the source are deleted
  afterwards. `replica:reload(true)` empties the spaces of each table
  (`truncate`, which fires no `on_replace` triggers) right before it is
  loaded.
* Spaces are written with ordinary `replace`/`delete`, so `on_replace`
  triggers keep working. A full load writes only rows that differ from the
  stored tuples, at most `batch` per transaction, and yields after each, so
  triggers that put to channels do not find them full. If a trigger yields
  anyway, Tarantool aborts the transaction; MirrorFlayer repeats it in smaller
  pieces after a pause and slows down for a minute (`stat().aborts`). A
  binlog transaction split that way is no longer atomic (`stat().split`);
  applying is idempotent, so a crash in between only repeats statements.

A function target receives `'insert' | 'update' | 'delete'` with the old and
new tuples (Lua arrays in `columns` order, `nil` where absent), and `'reset'`
with no tuples before a full reload, followed by `'insert'` for every row of
the snapshot. It is called **inside the box transaction**: it must not yield
(no network, no `fiber.sleep`, no waiting on channels), or the transaction
is aborted.

### Threads

With `thread`, the source side of a replicator runs in an application thread:
the socket, TLS, the protocol, the binlog or pgoutput and the conversion of
values. The spaces stay in TX, which only applies the rows of a full load
and the transactions of the stream, with their positions, as in TX alone:
same triggers, same pace, same `stat()`. A full load and a busy stream then
take no CPU of TX beyond the writes.

```lua
box.cfg({ app_threads = 1 })
local replica = MirrorFlayer.new({ hosts = { 'db1:3306' }, user = 'replica', password = '...',
  server = 1001, thread = 'app' })
```

`threads.eval()` runs code in every thread of a group, so each thread starts
the replicator only when it is the one asked for (`thread_id`); one thread
can run several replicators of different names. The thread calls the
replicator in TX through one exported function, `mirrorflayer.call`.

## Values

One conversion table per driver (`MirrorFlayer.MySQL.Column`,
`MirrorFlayer.PostgreSQL.Column`, both over `MirrorFlayer.Column`) serves the
snapshot of the full load and the changes of the stream, so a row gives the
same tuple either way. Every value gets its Tarantool type.

### MySQL and MariaDB

| Source type | Tarantool value |
|-------------|-----------------|
| `TINYINT` … `BIGINT`, signed or unsigned, `TINYINT(1)` and `BOOLEAN` included | number; `int64_t`/`uint64_t` cdata beyond 2^53 |
| `FLOAT` | number, rounded to single precision on both paths |
| `DOUBLE`, `REAL` | number |
| `DECIMAL`, `NUMERIC` | `decimal` |
| `DATETIME`, `TIMESTAMP` | `datetime` in UTC with fractions; `'number'`: seconds since the Unix epoch with fractions |
| `DATE` | `datetime` at midnight UTC; `'number'`: seconds since the Unix epoch |
| `TIME` | `interval`, negative for a negative time; `'number'`: seconds |
| `YEAR` | number |
| zero dates (`0000-00-00`) | `box.NULL` |
| `CHAR`, `VARCHAR`, `TEXT` | string |
| `BINARY`, `VARBINARY`, `BLOB` | `varbinary` (MP_BIN) |
| `JSON` | MsgPack map or array, JSON `null` as `box.NULL`; `'string'`: the text the server prints |
| `UUID` (MariaDB) | `uuid`; `'binary'`: 16 bytes in network byte order; `'string'`: canonical text |
| `ENUM` | member string |
| `SET` | members joined by commas |
| `BIT` | number |
| `NULL` | `box.NULL` |

`TIMESTAMP` is read in UTC: the session time zone is `+00:00`, and the binlog
keeps UTC epoch seconds. In the `'string'` JSON mode the binary JSON of MySQL
is printed the way MySQL prints it (`{"a": [1, 2.5, true]}`, keys in the
stored order); MariaDB keeps JSON as text, a `LONGTEXT` with a `json_valid()`
check, which is how the driver knows these columns. Values of `DECIMAL` or
date types inside MySQL JSON stop the replicator.

Binlog row images carry no names, signedness or member lists; they come from
`information_schema.COLUMNS` on every connection and again after DDL. Types
not listed (`INET4`, `INET6`, geometry) are refused when mapped.

### PostgreSQL

| Source type | Tarantool value |
|-------------|-----------------|
| `smallint`, `integer`, `bigint`, `oid` | number; `int64_t` cdata beyond 2^53 |
| `real` | number, rounded to single precision |
| `double precision` | number; `NaN` and `±Infinity` as they are |
| `numeric` | `decimal`; `NaN` and `±Infinity` as `box.NULL` |
| `boolean` | boolean |
| `timestamp`, `timestamptz` | `datetime` in UTC with fractions; `'number'`: seconds since the Unix epoch |
| `date` | `datetime` at midnight UTC; `'number'`: seconds since the Unix epoch |
| `±infinity` dates | `box.NULL` |
| `time` | `interval`; `'number'`: seconds |
| `interval` | `interval` with years, months, days and time; `'number'`: seconds, a month counted as 30 days and a year as 365.25 days, as `EXTRACT(EPOCH)` does |
| `char`, `varchar`, `text`, `name`, `citext` and other string types, `inet`, `cidr`, `macaddr`, `xml` | string (`char(n)` padded as PostgreSQL prints it) |
| enums | member string |
| `bytea` | `varbinary` (MP_BIN) |
| `json`, `jsonb` | MsgPack map or array, JSON `null` as `box.NULL`; `'string'`: the text the server prints |
| `uuid` | `uuid`; `'binary'`: 16 bytes in network byte order; `'string'`: canonical text |
| domains | as their base type |
| `NULL` | `box.NULL` |

The session prints dates in ISO with `TimeZone=UTC`, so `timestamptz` comes
in UTC. Arrays, `timetz`, ranges, geometry and dates BC are refused when
mapped or met.

The full load creates the slot anew with an exported snapshot and reads the
tables in that snapshot over a second connection: the load and the stream
meet exactly. The stream confirms the LSN of every applied transaction, so
the server frees the WAL before it; a slot keeps WAL while the replicator is
away, drop it with `pg_drop_replication_slot()` when the replicator is gone
for good. A slot that is gone means a full load.

`UPDATE` and `DELETE` carry the old key of the `REPLICA IDENTITY` (the
primary key by default): the primary key of a target space has to be among
its columns, or the table needs `REPLICA IDENTITY FULL`; a function target
gets the other old values as `NULL`. A TOAST value an update did not touch
is not sent: the target keeps the one it has. `TRUNCATE` of a mapped table
means a full load.

## Build and install

`Source/` holds the engine and the shared Lua modules, `Source/MySQL/` the
driver of MySQL and MariaDB, `Source/PostgreSQL/` the driver of PostgreSQL;
they are installed as `$(PREFIX)/share/tarantool/MirrorFlayer/` with the
`MySQL/` and `PostgreSQL/` subdirectories, which is what
`require('MirrorFlayer')` finds. Only `init.lua` is lowercase: that is the
file Lua looks for in a package directory. `Source/TLS.c` is `MirrorFlayer.TLS`, installed as
`$(PREFIX)/lib/<triplet>/tarantool/MirrorFlayer/TLS.so`; it needs the headers
of `tarantool-dev` and `libssl-dev` and links the system OpenSSL.

```
make                                       # Source/TLS.so
make install [PREFIX=/usr] [DESTDIR=...]   # Lua modules and TLS.so
make check                                 # syntax check with tarantool
make test                                  # Docker test, see Tests/
```

`TLS=0` builds and installs no `TLS.so`. Then only plain connections work,
and `caching_sha2_password` only while the server has the password cached
(it needs RSA for the full authentication without TLS).

With LuaRocks of Tarantool (`tt rocks`), the same headers are needed:

```
tt rocks install https://raw.githubusercontent.com/cyanide-burnout/MirrorFlayer/2.2.1/mirrorflayer-2.2.1-1.rockspec
tt rocks install https://raw.githubusercontent.com/cyanide-burnout/MirrorFlayer/main/mirrorflayer-scm-1.rockspec   # main
tt rocks make mirrorflayer-scm-1.rockspec   # from a checkout
```

## Tests

`Tests/DockerTest.sh` builds `TLS.so`, runs MariaDB or MySQL with a row-based
binlog, or PostgreSQL with `wal_level=logical`, and Tarantool 3 in Docker and
drives `Tests/MirrorFlayerTest.lua` or `Tests/PostgreSQLTest.lua` through
phases. `SERVER=mariadb|galera|mysql|postgresql` picks the source
(`mariadb:11.4`, the same as a Galera cluster of one node, `mysql:8.4`,
`postgres:17`, `SERVER_IMAGE` overrides), `TLS=off|on|ca` the
connection of the replicator (`ca` verifies against the CA of the server).
MySQL users authenticate with `caching_sha2_password`, so a run without TLS
goes through RSA first and the server cache later; PostgreSQL users with
`SCRAM-SHA-256`.
`THREAD=1` runs every replicator of the test in an application thread; it
needs Tarantool 3.9 or later, `TARANTOOL_IMAGE=tarantool/tarantool:3.9.0`.

1. `load` — tables with data exist before the start: full load, values of all
   types, then INSERT/UPDATE/DELETE over the stream, a key change, a
   multi-table transaction, `on_replace` triggers, a trigger that yields and a
   function target; two more replicators check `datetime = 'number'`,
   `json = 'string'`, `uuid = 'string'` and `uuid = 'binary'` over the load
   and the stream. PostgreSQL also keeps a TOAST value an update did not
   send. A Galera node that leaves its cluster is left and taken again when
   it is back.
2. `resume` — changes made while Tarantool is down; restart continues from the
   stored position without a full load, and a space emptied meanwhile gets
   its table loaded alone.
3. `purge` — changes and a lost position while down (a purge of the binlog,
   error 1236; a dropped slot): full load that also deletes rows gone from
   the source.
4. `late` — a fresh instance started before its space exists retries, loads
   everything once the space is made, and stops quietly when its fiber is
   cancelled from outside.

`Tests/RemoteTest.sh host` copies the tree to a Docker host over ssh and
runs the test there with the same variables:

```
SERVER=postgresql TLS=ca Tests/RemoteTest.sh docker-host
```


## License

MIT, see [LICENSE](LICENSE).
