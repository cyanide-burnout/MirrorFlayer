# MirrorFlayer design

Usage and values are in the [README](../README.md).

## Behaviour

* **Transactions.** One binlog transaction (GTID … XID/COMMIT) is collected in
  memory and applied in one `box.begin()`/`box.commit()` together with the new
  position. Decoding and metadata queries happen outside the box transaction.
  Transactions that touch no mapped table only advance the position in memory;
  it is written at most once a second and on heartbeats.
* **Position.** A tuple `{ key, gtid_state, event_time }` in the `position`
  space (`ReplicatorPositions` by default, the space Registry already has with
  an unsigned primary key), the GTID state string as the source prints it:
  `0-1-100,1-2-5` for MariaDB, `uuid:1-100:105` for MySQL. The key is the replica server id unless `key` is given:
  it has to be unique among the replicas of the source anyway and does not
  change when Galera nodes are added to `hosts`, which a hash of the addresses
  would. Key 0 is the Python replicator's `[0, gtid, 0]`: without its own
  tuple MirrorFlayer starts from it once and never writes it, so both can run
  side by side. Python stored the GTID of a transaction before applying it, so
  continuing from its position skips the transaction it may have been applying
  when it stopped. `resetReplication()` of Registry truncates the space, which
  makes the next start do a full load.
* **Full load** (first start, error 1236, `TRUNCATE` of a mapped table, a table
  map that does not match the table definition): the GTID state
  (`@@gtid_binlog_pos`, `@@gtid_executed` on MySQL) is read first, then all tables are selected with
  one `COM_QUERY`, a `SELECT` of each separated with `;` (`CLIENT_MULTI_STATEMENTS`), one round
  trip for the load, and their results streamed one after another; rows are written over the space
  in batches with `fiber.yield()` between them, then rows absent from the snapshot are deleted,
  so spaces never go empty. The server waits meanwhile with the next result (`net_write_timeout`,
  60 s by default, against seconds of deleting). Changes made during the load come again from the
  binlog; applying is idempotent (`INSERT` → `replace`, `DELETE` of a missing
  row does nothing), so the overlap is safe.
* **Errors.** 1236 (the position is no longer in the binlog) → full load.
  Network and server errors → reconnect to the next host after `retry`
  seconds. A row image that is not FULL (`binlog_row_image` must be `FULL`),
  anonymous or tagged GTIDs, partial JSON updates, XA, a missing
  `MirrorFlayer.Support` when TLS is asked for or a compressed event or
  transaction comes, and
  unsupported column types stop the replicator with an error in
  `stat().error` and the log.
* **Metrics.** `stat()` returns `event_time` (source time of the last applied
  transaction), `applied_time`, `lag` (their difference, 0 after a heartbeat
  of an idle source), counters, `tls` and the last error. With the `metrics` module
  the gauges `mirrorflayer_lag_seconds`, `mirrorflayer_event_timestamp_seconds`
  and `mirrorflayer_applied_timestamp_seconds` (label `name`) are exported.

Requirements on the source: `log_bin`, `binlog_format=ROW`,
`binlog_row_image=FULL`, GTID (always on in MariaDB, `gtid_mode=ON` in
MySQL). Tarantool 3.x (LuaJIT).

* **Galera.** With `wsrep_on`, a node is used only while `wsrep_ready` is ON
  and its cluster status `Primary`: on connect, and every `check` seconds
  during the stream over a short connection of its own in a fiber of its own,
  since a node that leaves the primary component goes silent rather than
  breaking the stream. The stream is read meanwhile; a failed check ends it at
  the next packet, a heartbeat at the latest. A node that is not ready is a
  failure like any other, the next host is tried.
* **Round trips.** Each step from connect to the stream costs one round trip
  to the source, statements separated with `;` (`CLIENT_MULTI_STATEMENTS`):
  the time zone with the state of Galera (and `gtid_mode` of MySQL), the
  columns of all tables (with the checks of MariaDB), the position, the load of
  all tables, the variables of the replica with the checksum of the binlog.
* **Tables loaded alone.** A table whose spaces are all empty while a
  position is stored is loaded by itself before the stream (`fills`); the
  stream from the older position converges over it.
* **Dialects.** The greeting tells MariaDB (`MariaDB` in the version) from
  MySQL, and `MirrorFlayer.MySQL.MariaDB` or `MirrorFlayer.MySQL.GTID` handles the GTID
  format, the GTID event and the stream request. A MariaDB GTID event says
  whether the group is standalone (DDL); in MySQL a DDL statement is a GTID
  event and a QUERY without BEGIN, so a MySQL group counts as standalone until
  BEGIN comes.
* **Authentication.** `mysql_native_password`, `caching_sha2_password`
  (`SHA256(password) XOR SHA256(SHA256(SHA256(password)) + scramble)`; the
  server answers 3 when it has the password cached, 4 for the full
  authentication: the password in clear over TLS, otherwise RSA-OAEP with the
  public key from `public_key` or asked from the server) and `sha256_password`
  the same way. AuthSwitchRequest works for all of them.

## Modules

The engine and what every source shares are `Source/<Name>.lua`, installed as
`MirrorFlayer/<Name>.lua`; a source driver is a subdirectory, `Source/MySQL/`
installed as `MirrorFlayer/MySQL/`, its `init.lua` being the driver itself.

| Module | Role |
|--------|------|
| `MirrorFlayer` (`Source/init.lua`) | the replicator in TX: API, targets, loads and transactions applied, positions, statistics, metrics |
| `MirrorFlayer.Runner` | the loop with the source: hosts, retries, sessions, columns, full loads, the stream; it touches no space, a sink takes what concerns them |
| `MirrorFlayer.Thread` | the Runner in an application thread, its sink a proxy to the replicator in TX through `threads.call` |
| `MirrorFlayer.Failure` | failure objects: fatal stops the replicator, reload asks for a full load |
| `MirrorFlayer.Column` | values in their Tarantool types, datetime and json modes |
| `MirrorFlayer.Store` | targets, pacing of writes, full load, position |
| `MirrorFlayer.Transport` | `read(n)`/`write(s)`/`close()`; plain TCP over the Tarantool `socket`, `secure()` moves it onto TLS |
| `MirrorFlayer.Support` (`Source/Support.c` → `Support.so`, `TLS.so` up to 2.2) | TLS over the already connected descriptor, RSA-OAEP for `caching_sha2_password` and `sha256_password`, inflate (zlib, zstd) for the compressed protocol, the compressed events of MariaDB and transactions of MySQL, CRC32 of binlog events, Ed25519 of `client_ed25519` and `parsec` |
| `MirrorFlayer.MySQL` (`Source/MySQL/init.lua`) | the driver of MySQL and MariaDB: session, columns, snapshot, binlog stream and its events |
| `MirrorFlayer.MySQL.Protocol` | packets, greeting, SSL Request, authentication, `COM_QUERY` with a streamed text result |
| `MirrorFlayer.MySQL.MariaDB` | MariaDB dialect: GTID state, GTID event, `COM_BINLOG_DUMP` after `@slave_connect_state` |
| `MirrorFlayer.MySQL.GTID` | MySQL dialect: GTID sets, GTID_LOG_EVENT, `COM_BINLOG_DUMP_GTID` |
| `MirrorFlayer.MySQL.BinLog` | events: FORMAT_DESCRIPTION, ROTATE, QUERY, XID, TABLE_MAP, ROWS v1/v2, CRC32 |
| `MirrorFlayer.MySQL.Column` | types of MySQL, text and binlog values, NULL bitmaps, binary JSON |
| `MirrorFlayer.PostgreSQL` (`Source/PostgreSQL/init.lua`) | the driver of PostgreSQL: session, catalogue, slot with an exported snapshot, pgoutput stream, standby status |
| `MirrorFlayer.PostgreSQL.Protocol` | frontend/backend protocol 3.0, SSLRequest, SCRAM-SHA-256, md5, simple query with a streamed result, CopyBoth |
| `MirrorFlayer.PostgreSQL.Output` | pgoutput 1: Begin, Commit, Relation, Insert, Update, Delete, Truncate; LSNs |
| `MirrorFlayer.PostgreSQL.Column` | types by OID, text values, intervals in ISO 8601 |

### Drivers

`config.type` picks the driver, `'mysql'` by default, or `'postgresql'`. The engine knows nothing
of binlogs: a driver gives it a session on one host.

| Call | Does |
|------|------|
| `driver.connect(config, host, port)` | connects and returns a session with `version` and `secure` |
| `session:describe(sources)` | the columns of the tables in one query, `{ [source] = columns }`: name, type, kind, supported |
| `session:position()` | the current position of the source as a string |
| `session:load(sources, each, done)` | the snapshot of the tables in one query, a `SELECT` of each: `each(source, row)` with values converted, `done(source)` at the end of a table |
| `session:stream(position, sources, handler)` | changes after the position until an error: `handler:started()`, `handler:commit(changes, position, event_time)`, `handler:heartbeat()`, `handler:refresh(source)` after DDL |
| `session:close()` | closes the connection |

A change is `{ source, operation, old, new }`. A failure with `reload` (a
position the source no longer has, a table changed under the stream) makes
the next connection begin with a full load. `driver.PORT`, `driver.key(config)`
(the default key of the position) and `driver.check(config)` complete it.

### PostgreSQL

The replication connection has `replication=database` and runs catalogue
queries too. `position()` drops the slot if it is there (`DROP_REPLICATION_SLOT
... WAIT`, it may still be held by the previous session) and creates it with
`CREATE_REPLICATION_SLOT ... LOGICAL pgoutput EXPORT_SNAPSHOT`; a second,
ordinary connection opens `REPEATABLE READ` with `SET TRANSACTION SNAPSHOT`
before the replication connection does anything else, and `load()` reads
there. `stream()` commits it and sends `START_REPLICATION SLOT ... LOGICAL
lsn (proto_version '1', publication_names ...)`.

In the stream, XLogData carries the pgoutput messages, a keepalive the end of
WAL sent so far: outside a transaction the position moves there
(`handler:advance()`), so an idle source does not leave the position behind.
The standby status update reports the LSN of the last applied commit at most
once a second and on request; when the server is silent for `heartbeat`
seconds, the driver asks for a keepalive (`reply` set), and three periods
without data reconnect. `Transport:wait()` waits for data without reading it
for that.

A Relation message describes a table before its first change and after DDL;
its columns are bound by name and type OID to the columns of the source,
which are described again when they differ. Values come as text in protocol
1, printed by the same output functions as the snapshot. A position is
`slot/X/Y`; a slot that is gone (42704 on `START_REPLICATION`) asks for a
full load.

### SCRAM-SHA-256-PLUS of PostgreSQL

Over TLS, when the server offers `SCRAM-SHA-256-PLUS`, the client takes it
with the GS2 header `p=tls-server-end-point,,` and sends `c=` as base64 of the
header and the channel binding: the hash of the certificate of the server by
the hash of its signature, SHA-256 for MD5 and SHA-1 (RFC 5929),
`session:endpoint()` of `Support.so`. The proof then holds only for that TLS
session. Over TLS without PLUS the header is `y,,`, without TLS `n,,`.

### GSSAPI of PostgreSQL

`AuthenticationGSS` (7) starts an exchange for the service principal
`postgres@host` with `gss_init_sec_context()` of the system GSSAPI (MIT
Kerberos, `Support.gss()`): its tokens go to the server as password
messages, `AuthenticationGSSContinue` (8) brings the answers, until the
context is complete. The credentials are those of the process: the
credential cache, or a client keytab named by `KRB5_CLIENT_KTNAME`, from
which MIT Kerberos takes a ticket itself. A step may ask the KDC, so it runs
in a thread of the pool of Tarantool (`coio_call`) while the fiber waits.
GSS encryption (`GSSENCRequest`) is not used: TLS does that part.

### Streamed transactions of PostgreSQL

PostgreSQL 14 and later get `proto_version '2', streaming 'on'`: a
transaction larger than `logical_decoding_work_mem` comes in blocks while it
runs, Stream Start (its xid) and Stream Stop around them, the changes inside
with the xid of their (sub)transaction before the rest. The driver keeps the
changes of each with the xid of their subtransaction; Stream Abort drops a
subtransaction or the whole one, Stream Commit hands the rest to the engine
with its end LSN. While a streamed transaction is open the position does not
follow keepalives, so a reconnect gets it again from its beginning.
PostgreSQL 12 and 13 stay on version 1.

### Stream request (MariaDB)

```
SET @master_binlog_checksum = @@GLOBAL.binlog_checksum, @mariadb_slave_capability = 4,
    @slave_connect_state = '<gtid state>', @slave_gtid_strict_mode = 0,
    @slave_gtid_ignore_duplicates = 0, @master_heartbeat_period = <ns>;
  SELECT @@GLOBAL.binlog_checksum                       (one round trip)
COM_BINLOG_DUMP (position 4, flags 0, server id, no file name)
```

### Stream request (MySQL)

```
SET @master_binlog_checksum = @@GLOBAL.binlog_checksum, @source_binlog_checksum = ...,
    @master_heartbeat_period = <ns>, @source_heartbeat_period = <ns>;
  SELECT @@GLOBAL.gtid_mode, @@GLOBAL.binlog_checksum   (one round trip)
COM_BINLOG_DUMP_GTID (flags BINLOG_THROUGH_GTID, server id, no file name, position 4,
                      the GTID set: sids, each with intervals [first, last + 1))
```

### TLS

MySQL starts TLS after the greeting on the same TCP connection (SSL Request
packet), so `MirrorFlayer.Support` takes over the descriptor of the connected
Tarantool socket (`sock:fd()`, `SSL_set_fd`), stays non-blocking and waits with
`coio_wait()` on `SSL_ERROR_WANT_READ`/`WANT_WRITE`. It also provides RSA-OAEP
for `caching_sha2_password` without TLS. It is built by the Makefile unless
`SUPPORT=0`. `verify` is `none` (encryption only, MySQL `ssl-mode=REQUIRED`),
`ca` (`VERIFY_CA`) or `identity` (`VERIFY_IDENTITY`, `SSL_set1_host`); SNI is
sent for host names. Timeouts are deadlines over the whole read or write,
and a cancelled fiber stops waiting.

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
  libssl version. `Support.so` (`TLS.so` then) links the system library: the simpler default for
  a Debian package (`Depends: libssl3t64`).
* A Tarantool built against the system OpenSSL (e.g. distribution packages)
  would export the `SSL_*` symbols; `tls.so` linked with the same system
  library then shares that single copy, which is also fine.

### Compression

MySQL and MariaDB: with `CLIENT_COMPRESS` in the handshake response, packets
go in frames after the authentication OK (as libmariadb and libmysqlclient
switch): 3 bytes of the length, a sequence of the frames (reset with each
command, as the packet sequence), 3 bytes of the length before compression,
0 when the frame is stored as it is. A frame carries packets or a part of
one. The server compresses frames of 50 bytes and more with `compress()` of
zlib; the replicator sends its frames uncompressed, so it only inflates. MySQL
8.0.18 and later also has zstd (`CLIENT_ZSTD_COMPRESSION_ALGORITHM`, the
handshake response ends with the level, 3): it is taken where the server
offers it, the frames are the same with zstd inside. MariaDB has zlib only. PostgreSQL has no compression in its protocol
(the libpq patch was never merged, `sslcompression` is gone since 14).

MariaDB with `log_bin_compress` writes `QUERY_COMPRESSED` (the query text is
compressed) and `WRITE/UPDATE/DELETE_ROWS_COMPRESSED(_V1)` (the rows after the
column bitmaps are compressed): a header byte `0x80 | algorithm << 4 |
count of length bytes`, the length before compression big-endian, then zlib
(algorithm 0, the only one). The checksum covers the event as written.

MySQL with `binlog_transaction_compression` writes a transaction after its
GTID as one `TRANSACTION_PAYLOAD`: fields of a type, a length and a value,
length-encoded integers, up to the end mark 0 (1 the payload size, 2 the
compression, 0 zstd or 255 none, 3 the size before compression), then the
payload: the events of the transaction one after another, without checksums
of their own.

### XA transactions

MySQL and MariaDB log an XA transaction as its changes between `XA START`
(a query of MySQL, a flag of the GTID event of MariaDB) and `XA END`, ended by
`XA_PREPARE_LOG_EVENT` (one phase, the format id, gtrid and bqual); the
decision comes later as a transaction of its own, the query `XA COMMIT` or
`XA ROLLBACK X'gtrid',X'bqual',format`. `XA COMMIT ... ONE PHASE` is the
prepare event with one phase set in MySQL and an ordinary transaction in
MariaDB.

The driver keeps the TABLE_MAP and rows events of every transaction. At the
prepare of two phases they go to the engine instead of the changes:
`prepare(xid, events, position)` writes them into the space named after the
position space with `XA` (`ReplicatorPositionsXA`, `{ key, xid, events }`,
made with the first one) in the box transaction of the position. `XA COMMIT`
takes them back (`prepared(xid)`), runs them through the decoder as they came
and commits the changes, deleting the record in the same box transaction;
`XA ROLLBACK` deletes it with the position. A transaction prepared before the
position of a full load is not in the snapshot and has no record: its commit
is logged and has no changes.

### Binlog files of MySQL without GTIDs

With `gtid_mode` other than `ON` the driver takes `MirrorFlayer.MySQL.File`:
the position is `file:<server_uuid>:<file>:<offset>`, the full load starts at
`SHOW BINARY LOG STATUS` (`SHOW MASTER STATUS` before 8.4), the stream comes
from `COM_BINLOG_DUMP` with the file and the offset. `ANONYMOUS_GTID` and
`GTID` events begin transactions; a transaction ends at the end of its last
event, the position of the event after it; `ROTATE` gives the next file. The
files and offsets are those of one server: a position of another server
(after the next of `hosts`) means a full load.

### Tagged GTIDs of MySQL

MySQL 8.3 and later has GTIDs with a tag, `uuid:tag:n`; a set prints the
untagged intervals of a uuid first and then each tag with its intervals,
`uuid:1-5:tag:1-3`. `GTID_TAGGED_LOG_EVENT` is in the serialization format of
MySQL: a version byte, the size of the message and the id of its last field
that cannot be ignored, then fields by their ids (0 flags, 1 the uuid, 16
bytes as integers, 2 the gno, signed, 3 the tag as a length and bytes, and
more not needed here), integers of variable length: the count of trailing
ones of the first byte plus one is the count of bytes, the value above them.
`COM_BINLOG_DUMP_GTID` takes a set with tags in the tagged format: the count
of uuids as `1 << 56 | count << 8 | 1`, after each uuid its tag as a length and
the bytes, an empty one for none.

### Row images and partial JSON

A row image that is not `FULL` (`MINIMAL`, `NOBLOB`, `FULL_NODUP` of MariaDB)
lacks columns: they come as `Column.UNCHANGED`, as the TOAST values of
PostgreSQL, and the target space keeps what it has (`Store` takes them from
the stored tuple, `NULL` for a function target or a new row). An insert of
`MINIMAL` lacks the columns the statement did not set: they become `NULL`,
the server would write their defaults.

`PARTIAL_UPDATE_ROWS` of MySQL (`binlog_row_value_options=PARTIAL_JSON`):
between the before and the after image a shared image, the value options and
with partial JSON a bit for every JSON column of the table. A JSON column with
its bit set comes as diffs: a 4-byte length, then for each an operation byte
(0 replace, 1 insert, 2 remove), the path as text (`$.a[1]`, `$."a b"`,
`[last]`) and, but for remove, the value in binary JSON, lengths as
length-encoded integers. `MirrorFlayer.MySQL.JSON` keeps JSON as a tree with
the types of its numbers, applies the diffs to the old document of the before
image or, when the image lacks it, to the stored one (a `Column.patch` that
`Store` resolves; it crosses from a thread as data), and renders the tree in
the json mode: Lua values, or the text MySQL prints, object keys by length and
then by bytes as its binary form keeps them, the text of doubles and decimals
as it was. Opaque values inside JSON (DECIMAL, DATE, TIME, DATETIME,
TIMESTAMP) are read as MySQL prints them, others as `base64:typeN:...`.

### Authentication of MariaDB

* `client_ed25519`: the server sends a nonce of 32 bytes in the switch
  request, the client answers with an Ed25519 signature whose expanded key is
  SHA-512 of the password itself (not of a 32-byte seed). EVP of OpenSSL
  takes only seeds; libsodium has the group and scalar operations
  (`crypto_scalarmult_ed25519_base_noclamp`, `crypto_core_ed25519_scalar_*`)
  to sign with an expanded key. Checked against the vectors of RFC 8032,
  where a 32-byte password is a seed.
* `parsec` (11.6 and later): a scramble of the server in the switch request,
  an empty answer asks for `'P'`, the iteration factor and 18 bytes of salt;
  the key is PBKDF2-HMAC-SHA512 over `1024 << factor` rounds, the answer a
  scramble of the client and the Ed25519 signature (EVP) of both scrambles.
  A factor whose rounds do not fit into the timeout is refused, as the client
  library of MariaDB does.
* `mysql_clear_password` sends the password; `dialog` answers questions of
  type 4 and 5 (passwords) with it and refuses the others.
* After the first packet the server prefixes the data of a plugin with 1, the
  switch request keeps it as it is: a nonce may end with a zero byte.
* The CRC32 of binlog events is `crc32()` of zlib when `Support.so` is there,
  a table in Lua otherwise.

## Plan

1. ~~First slice: MariaDB, no TLS, native password, load, stream, position.~~
2. ~~`caching_sha2_password`, `sha256_password` and TLS (`TLS.so`).~~
3. ~~MySQL dialect (`COM_BINLOG_DUMP_GTID`, binary JSON).~~
4. Run beside the Python replicator in production and compare spaces.
5. `client_ed25519` of MariaDB; tagged GTIDs of MySQL 8.4; DECIMAL and dates
   inside MySQL JSON.
6. Column names and signedness from optional TABLE_MAP metadata
   (`binlog_row_metadata=FULL`) instead of `information_schema`, which reflects
   the current definition rather than the one of an old event.
7. Debian package.
