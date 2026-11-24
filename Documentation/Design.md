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
  an unsigned primary key), the GTID state string as MariaDB prints it
  (`0-1-100,1-2-5`). The key is the replica server id unless `key` is given:
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

Each module is `Source/<name>.lua`, installed as `mirrorflayer/<name>.lua`.

| Module | Role |
|--------|------|
| `mirrorflayer` (`Source/init.lua`) | API, replication fiber, reconnection, host rotation, event dispatch |
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

## Plan

1. ~~First slice: MariaDB, no TLS, native password, load, stream, position.~~
2. Run beside the Python replicator in production and compare spaces.
3. `caching_sha2_password` and TLS (`tls.so`), `client_ed25519` later.
4. MySQL dialect (`COM_BINLOG_DUMP_GTID`, MySQL JSON).
5. Column names and signedness from optional TABLE_MAP metadata
   (`binlog_row_metadata=FULL`) instead of `information_schema`, which reflects
   the current definition rather than the one of an old event.
6. Debian package.
