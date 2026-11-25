package = 'mirrorflayer'
version = 'scm-1'

source = {
  url    = 'git+https://github.com/cyanide-burnout/MirrorFlayer.git',
  branch = 'main'
}

description = {
  summary  = 'Replication of MySQL/MariaDB tables into Tarantool spaces over the binlog, inside Tarantool',
  detailed = [[
MirrorFlayer replicates MySQL and MariaDB tables into Tarantool spaces over
the replication protocol (binlog), in a fiber of Tarantool itself: no separate
process, no YAML, no MySQL client library. MariaDB and MySQL, with or without
TLS; mysql_native_password, caching_sha2_password and sha256_password.
MirrorFlayer.TLS is a C module over OpenSSL; without it only plain
connections work.
]],
  homepage = 'https://github.com/cyanide-burnout/MirrorFlayer',
  license  = 'MIT'
}

dependencies = {
  'lua >= 5.1'
}

external_dependencies = {
  OPENSSL   = { header = 'openssl/ssl.h' },
  TARANTOOL = { header = 'tarantool/module.h' }
}

build = {
  type    = 'builtin',
  modules = {
    ['MirrorFlayer']                = 'Source/init.lua',
    ['MirrorFlayer.Column']         = 'Source/Column.lua',
    ['MirrorFlayer.Failure']        = 'Source/Failure.lua',
    ['MirrorFlayer.Store']          = 'Source/Store.lua',
    ['MirrorFlayer.Transport']      = 'Source/Transport.lua',
    ['MirrorFlayer.MySQL']          = 'Source/MySQL/init.lua',
    ['MirrorFlayer.MySQL.BinLog']   = 'Source/MySQL/BinLog.lua',
    ['MirrorFlayer.MySQL.Column']   = 'Source/MySQL/Column.lua',
    ['MirrorFlayer.MySQL.GTID']     = 'Source/MySQL/GTID.lua',
    ['MirrorFlayer.MySQL.MariaDB']  = 'Source/MySQL/MariaDB.lua',
    ['MirrorFlayer.MySQL.Protocol'] = 'Source/MySQL/Protocol.lua',
    ['MirrorFlayer.TLS']            = {
      sources   = { 'Source/TLS.c' },
      defines   = { '_GNU_SOURCE' },
      incdirs   = { '$(OPENSSL_INCDIR)', '$(TARANTOOL_INCDIR)/tarantool' },
      libdirs   = { '$(OPENSSL_LIBDIR)' },
      libraries = { 'ssl', 'crypto' }
    }
  }
}
