package = 'mirrorflayer'
version = 'scm-1'

source = {
  url    = 'git+https://github.com/cyanide-burnout/MirrorFlayer.git',
  branch = 'main'
}

description = {
  summary  = 'Replication of MySQL, MariaDB and PostgreSQL tables into Tarantool spaces, inside Tarantool',
  detailed = [[
MirrorFlayer replicates MySQL, MariaDB and PostgreSQL tables into Tarantool
spaces over their replication protocols (the binlog, logical replication with
pgoutput), in a fiber of Tarantool itself: no separate process, no YAML, no
client library. With or without TLS; mysql_native_password,
caching_sha2_password, sha256_password, SCRAM-SHA-256 and md5.
MirrorFlayer.Support is a C module over OpenSSL, zlib, zstd and libsodium: TLS, RSA, the
compressed protocol and binlog events, the authentication plugins of MariaDB;
without it only plain uncompressed connections work.
]],
  homepage = 'https://github.com/cyanide-burnout/MirrorFlayer',
  license  = 'MIT'
}

dependencies = {
  'lua ~> 5.1',
  'tarantool >= 3.0'
}

external_dependencies = {
  ZLIB      = { header = 'zlib.h' },
  ZSTD      = { header = 'zstd.h' },
  SODIUM    = { header = 'sodium.h' },
  OPENSSL   = { header = 'openssl/ssl.h' },
  TARANTOOL = { header = 'tarantool/module.h' }
}

build = {
  -- The Makefile builds Source/Support.so (stripped) and installs it with the Lua modules into the tree of the rock
  type              = 'make',
  build_variables   = {
    CC      = '$(CC)',
    CFLAGS  = '$(CFLAGS) -D_GNU_SOURCE -I$(OPENSSL_INCDIR) -I$(ZLIB_INCDIR) -I$(ZSTD_INCDIR) -I$(SODIUM_INCDIR) -I$(TARANTOOL_INCDIR)/tarantool',
    LDFLAGS = '-L$(OPENSSL_LIBDIR) -L$(ZLIB_LIBDIR) -L$(ZSTD_LIBDIR) -L$(SODIUM_LIBDIR)'
  },
  install_variables = {
    LUADIR = '$(LUADIR)',
    LIBDIR = '$(LIBDIR)'
  }
}
