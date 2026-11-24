# MirrorFlayer

A Tarantool module that replicates MySQL/MariaDB tables into Tarantool spaces
over the replication protocol (binlog). It runs inside Tarantool itself, in
fibers: no separate process, no YAML, no MySQL sources or client library.

The goal of the project is to replace
[tarantool/mysql-tarantool-replication](https://github.com/tarantool/mysql-tarantool-replication)
— `replicatord`, a separate C++ daemon built on libslave and the MySQL client
library, configured in YAML and writing into Tarantool over the network —
with a replicator that lives inside Tarantool. MirrorFlayer keeps the values
replicatord wrote, so the code reading the spaces sees no difference.

MariaDB and MySQL, with or without TLS; `mysql_native_password`,
`caching_sha2_password` and `sha256_password`. Internals, OpenSSL findings
and the plan are in [Documentation/Design.md](Documentation/Design.md).

Requires Tarantool 3.x and a source with `log_bin`, `binlog_format=ROW`,
`binlog_row_image=FULL` and GTIDs: always there in MariaDB, `gtid_mode=ON`
in MySQL. MySQL must not compress transactions
(`binlog_transaction_compression=OFF`, the default) nor log partial JSON
updates (`binlog_row_value_options` empty, the default).

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

replica:stat()   -- status, position, lag, counters, last error
replica:stop()
```

Configuration is Lua only.

| Option      | Default        | Meaning |
|-------------|----------------|---------|
| `hosts`     | —              | `'host[:port]'` list; the next one is tried after a failure (Galera nodes) |
| `user`, `password` | —       | Replication user: `REPLICATION SLAVE`, `BINLOG MONITOR` (`REPLICATION CLIENT`), `SELECT` on the mapped tables |
| `server`    | —              | Server id of this replica, unique among the replicas of the source |
| `tls`       | `false`        | `true` for TLS without verification, or `{ verify = 'none' \| 'ca' \| 'identity', ca = 'ca.pem', cert = 'client.pem', key = 'client.key' }`; with `ca` verification defaults to `'ca'`, without it to `'none'`; `'ca'` and `'identity'` without `ca` use the system store. Needs `MirrorFlayer.TLS` |
| `public_key` | —             | PEM public key of the source for `caching_sha2_password`/`sha256_password` without TLS; without it the key is asked from the server (open to a man in the middle) |
| `position`  | `'ReplicatorPositions'` | Space that keeps the position, created when missing with an unsigned key as Registry creates it |
| `key`       | `server`       | Key of the position in that space: a stable number or string of this replicator, the replica server id by default (0 is the Python replicator's) |
| `name`      | `'MirrorFlayer'` | Name in logs and the label of metrics |
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

One conversion table (`MirrorFlayer.Column`) serves both the text result of
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
| `CHAR`, `VARCHAR`, `TEXT` | string |
| `JSON` | string; MariaDB keeps it as text, the binary JSON of MySQL is printed the way MySQL prints it (`{"a": [1, 2.5, true]}`, keys in stored order); values of DECIMAL or date types inside MySQL JSON stop the replicator |
| `BINARY`, `VARBINARY`, `BLOB` | `varbinary` (MP_BIN), as Python `bytes` were written |
| `ENUM`, `SET` | member string(s) |
| `BIT` | number |
| `NULL` | `box.NULL` |

Binlog row images carry no names, signedness or member lists; they come from
`information_schema.COLUMNS` on every connection and again after DDL. Types
not listed (MariaDB `UUID`, `INET6`, geometry) are refused when mapped.

## Build and install

`Source/` holds the Lua modules; they are installed as
`$(PREFIX)/share/tarantool/MirrorFlayer/`, which is what `require('MirrorFlayer')`
finds. Only `init.lua` is lowercase: that is the file Lua looks for in a
package directory. `Source/TLS.c` is `MirrorFlayer.TLS`, installed as
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

## Tests

`Tests/DockerTest.sh` builds `TLS.so`, runs MariaDB or MySQL with a row-based
binlog and Tarantool 3 in Docker and drives `Tests/MirrorFlayerTest.lua`
through phases. `SERVER=mariadb|mysql` picks the source (`mariadb:11.4`,
`mysql:8.4`, `SERVER_IMAGE` overrides), `TLS=off|on|ca` the connection of the
replicator (`ca` verifies against the CA of the MySQL server). MySQL users
authenticate with `caching_sha2_password`, so a run without TLS goes through
RSA first and the server cache later.

1. `load` — tables with data exist before the start: full load, values of all
   types, then INSERT/UPDATE/DELETE over the stream, a key change, a
   multi-table transaction, `on_replace` triggers and a function target.
2. `resume` — changes made while Tarantool is down; restart continues from the
   stored GTID without a full load.
3. `purge` — changes and a purge of the binlog while down: error 1236, full
   load that also deletes rows gone from the source.
4. `legacy` — a fresh instance with the Python position `[0, gtid, 0]` in
   `ReplicatorPositions` continues from it, writes its own position under the
   server id and leaves key 0 as it was.

`Tests/RemoteTest.sh host` copies the tree to a Docker host over ssh and
runs the test there with the same variables:

```
SERVER=mysql TLS=ca Tests/RemoteTest.sh docker-host
```


## License

MIT, see [LICENSE](LICENSE).
