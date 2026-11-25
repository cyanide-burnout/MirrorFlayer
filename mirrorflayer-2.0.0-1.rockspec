package = 'mirrorflayer'
version = '2.0.0-1'

source = {
  url    = 'git+https://github.com/cyanide-burnout/MirrorFlayer.git',
  tag    = '2.0.0'
}

description = {
  summary  = 'Replication of MySQL, MariaDB and PostgreSQL tables into Tarantool spaces, inside Tarantool',
  detailed = [[
MirrorFlayer replicates MySQL, MariaDB and PostgreSQL tables into Tarantool
spaces over their replication protocols (the binlog, logical replication with
pgoutput), in a fiber of Tarantool itself: no separate process, no YAML, no
client library. With or without TLS; mysql_native_password,
caching_sha2_password, sha256_password, SCRAM-SHA-256 and md5.
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
    ['MirrorFlayer']                     = 'Source/init.lua',
    ['MirrorFlayer.Column']              = 'Source/Column.lua',
    ['MirrorFlayer.Failure']             = 'Source/Failure.lua',
    ['MirrorFlayer.Store']               = 'Source/Store.lua',
    ['MirrorFlayer.Transport']           = 'Source/Transport.lua',
    ['MirrorFlayer.MySQL']               = 'Source/MySQL/init.lua',
    ['MirrorFlayer.MySQL.BinLog']        = 'Source/MySQL/BinLog.lua',
    ['MirrorFlayer.MySQL.Column']        = 'Source/MySQL/Column.lua',
    ['MirrorFlayer.MySQL.GTID']          = 'Source/MySQL/GTID.lua',
    ['MirrorFlayer.MySQL.MariaDB']       = 'Source/MySQL/MariaDB.lua',
    ['MirrorFlayer.MySQL.Protocol']      = 'Source/MySQL/Protocol.lua',
    ['MirrorFlayer.PostgreSQL']          = 'Source/PostgreSQL/init.lua',
    ['MirrorFlayer.PostgreSQL.Column']   = 'Source/PostgreSQL/Column.lua',
    ['MirrorFlayer.PostgreSQL.Output']   = 'Source/PostgreSQL/Output.lua',
    ['MirrorFlayer.PostgreSQL.Protocol'] = 'Source/PostgreSQL/Protocol.lua',
    ['MirrorFlayer.TLS']                 = {
      sources   = { 'Source/TLS.c' },
      defines   = { '_GNU_SOURCE' },
      incdirs   = { '$(OPENSSL_INCDIR)', '$(TARANTOOL_INCDIR)/tarantool' },
      libdirs   = { '$(OPENSSL_LIBDIR)' },
      libraries = { 'ssl', 'crypto' }
    }
  }
}
