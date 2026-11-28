#!/usr/bin/env tarantool
-- Docker test of MirrorFlayer against MariaDB, a node of MariaDB Galera or
-- MySQL (SERVER), with or without TLS (TLS = off | on | ca), compressed
-- unless COMPRESS=0, driven by
-- Tests/DockerTest.sh:
--   load    full load, values of all types, INSERT/UPDATE/DELETE over the stream
--   resume  restart continues from the stored GTID without a full load, a
--           table whose space was emptied is loaded alone
--   purge   the position is purged from the binlog: error 1236, full reload
--   late    a fresh instance started before its space exists
--   discover unmapped databases and tables go to handlers.discover, map() at
--           run time and reconnect(tables), failed sessions to handlers.fail

local ffi       = require('ffi')
local fiber     = require('fiber')
local log       = require('log')
local msgpack   = require('msgpack')
local uuid      = require('uuid')
local decimal   = require('decimal')
local datetime  = require('datetime')
local varbinary = require('varbinary')

local MirrorFlayer = require('MirrorFlayer')
local protocol     = require('MirrorFlayer.MySQL.Protocol')

local phase  = arg[1]
local HOST   = os.getenv('SERVER_HOST') or 'mirrorflayer-server'
local SERVER = os.getenv('SERVER') or 'mariadb'
local TLS    = os.getenv('TLS') or 'off'
local COMPRESS = os.getenv('COMPRESS') ~= '0'
-- GTID=0: MySQL without GTIDs, the position is file:<server_uuid>:<file>:<offset>
local GTID     = os.getenv('GTID') ~= '0'

-- Users of the authentication plugins of MariaDB made by Tests/DockerTest.sh
local AUTH    = os.getenv('AUTH') or ''
local PLUGINS = { ed25519 = 'client_ed25519', parsec = 'parsec',
  pam = os.getenv('CLEARTEXT') == '1' and 'mysql_clear_password' or 'dialog' }

-- MariaDB alone or a node of Galera
local MARIADB = SERVER ~= 'mysql'

require('compat').binary_data_decoding = 'new'

-- THREAD=1: the replicators run their source side in an application thread
local THREAD = os.getenv('THREAD') == '1'

box.cfg({ work_dir = os.getenv('MIRRORFLAYER_DATA') or '/data', log_level = 5, app_threads = THREAD and 1 or nil })

-- The setting of the thread mode in a config
local function threaded(config)
  if THREAD then
    config.thread = 'app'
  end
  return config
end

-- Helpers --------------------------------------------------------------------

local failures = 0

local function ok(condition, format, ...)
  local message = string.format(format, ...)
  if condition then
    log.info('ok: %s', message)
  else
    failures = failures + 1
    log.error('FAIL: %s', message)
  end
end

local function wait(condition, what, timeout)
  local deadline = fiber.clock() + (timeout or 60)
  while not condition() do
    if fiber.clock() > deadline then
      error('timeout waiting for ' .. what, 0)
    end
    fiber.sleep(0.1)
  end
end

local function encode(value)
  return msgpack.encode(value)
end

-- Equal values; maps compare by content, since the order of their keys in
-- MsgPack depends on how the Lua table was built
local function same(left, right)
  if type(left) == 'table' and type(right) == 'table' then
    for key, value in pairs(left) do
      if not same(value, right[key]) then
        return false
      end
    end
    for key, value in pairs(right) do
      if not same(left[key], value) then
        return false
      end
    end
    return true
  end
  return encode(left) == encode(right)
end

local sql = protocol.connect({ host = HOST, port = 3306, user = 'root', password = 'root', timeout = 30 })

local function exec(...)
  for _, statement in ipairs({ ... }) do
    sql:query(statement)
  end
end

exec("SET time_zone = '+00:00'")

-- Binlog position of the source as the replicator prints it
local function gtid()
  if SERVER == 'mysql' and not GTID then
    local _, rows = sql:query('SHOW BINARY LOG STATUS')
    return string.format('file:%s:%s:%s', string.lower(sql:value('SELECT @@GLOBAL.server_uuid')), rows[1][1], rows[1][2])
  end
  local query = SERVER == 'mysql' and 'SELECT @@GLOBAL.gtid_executed' or 'SELECT @@GLOBAL.gtid_binlog_pos'
  return (string.gsub(sql:value(query), '%s', ''))
end

-- Numbers first..last as a derived table with the column seq (no
-- seq_1_to_N of MariaDB on MySQL)
local function seq(first, last)
  return string.format('(SELECT a.D + 10 * b.D + 100 * c.D + 1000 * d.D + 10000 * e.D + 1 AS seq ' ..
    'FROM Registry.Digits a, Registry.Digits b, Registry.Digits c, Registry.Digits d, Registry.Digits e) s ' ..
    'WHERE seq BETWEEN %d AND %d', first, last)
end

-- Name of the current binlog file
local function binlog_file()
  for _, statement in ipairs({ 'SHOW BINARY LOG STATUS', 'SHOW BINLOG STATUS', 'SHOW MASTER STATUS' }) do
    local status, value = pcall(sql.value, sql, statement)
    if status and value ~= nil then
      return value
    end
  end
  error('the binlog status is unknown', 0)
end

-- Schema ---------------------------------------------------------------------

-- A duration of TIME, expected as the datetime mode makes it
local function D(negative, hours, minutes, seconds, usec)
  return { duration = true, negative = negative, hours = hours, minutes = minutes, seconds = seconds, usec = usec }
end

-- A moment in UTC, expected as the datetime mode makes it
local function M(year, month, day, hour, minute, second, usec, date)
  return { moment = true, year = year, month = month, day = day, hour = hour, minute = minute,
    second = second, usec = usec, date = date }
end

local function expect(value, mode, json, ids)
  if uuid.is_uuid(value) then
    if ids == 'string' then
      return value:str()
    end
    if ids == 'binary' then
      return varbinary.new(value:bin('b'))
    end
    return value
  end
  if type(value) == 'table' and value.json then
    return json == 'string' and value.text or value.map
  end
  if type(value) == 'table' and value.duration then
    local sign = value.negative and -1 or 1
    if mode == 'number' then
      return sign * (value.hours * 3600 + value.minutes * 60 + value.seconds + value.usec / 1e6)
    end
    return datetime.interval.new({ hour = sign * value.hours, min = sign * value.minutes, sec = sign * value.seconds,
      nsec = sign * value.usec * 1000 })
  end
  if type(value) ~= 'table' or not value.moment then
    return value
  end
  local moment = datetime.new({ year = value.year, month = value.month, day = value.day, hour = value.hour,
    min = value.minute, sec = value.second, nsec = value.usec * 1000, tzoffset = 0 })
  if mode == 'number' then
    return moment.epoch + value.usec / 1e6
  end
  return moment
end

-- 'YYYY-MM-DD HH:MM:SS' of the text protocol as the datetime mode makes it
local function moment_of(text, mode)
  if text == box.NULL then
    return box.NULL
  end
  local year, month, day, hour, minute, second = string.match(text, '^(%d+)-(%d+)-(%d+) (%d+):(%d+):(%d+)')
  return expect(M(tonumber(year), tonumber(month), tonumber(day), tonumber(hour), tonumber(minute),
    tonumber(second), 0), mode)
end

local TYPES = {
  { 'TinyS', 'TINYINT', '-5', -5 },
  { 'TinyU', 'TINYINT UNSIGNED', '250', 250 },
  { 'Small', 'SMALLINT', '-30000', -30000 },
  { 'Medium', 'MEDIUMINT', '-8000000', -8000000 },
  { 'MediumU', 'MEDIUMINT UNSIGNED', '16000000', 16000000 },
  { 'IntS', 'INT', '-2000000000', -2000000000 },
  { 'IntU', 'INT UNSIGNED', '4000000000', 4000000000 },
  { 'Big', 'BIGINT', '-9007199254740993', -9007199254740993LL },
  { 'BigU', 'BIGINT UNSIGNED', '18446744073709551615', 18446744073709551615ULL },
  { 'Flt', 'FLOAT', '1.1', tonumber(ffi.new('float', 1.1)) },
  { 'Dbl', 'DOUBLE', '2.5e100', 2.5e100 },
  { 'Dec1', 'DECIMAL(10,2)', '-12.30', decimal.new('-12.30') },
  { 'Dec2', 'DECIMAL(30,12)', '123456789012345678.123456789012', decimal.new('123456789012345678.123456789012') },
  { 'Dec3', 'DECIMAL(5,3)', '0.050', decimal.new('0.050') },
  { 'Dt', 'DATETIME', "'2024-02-29 13:14:15'", M(2024, 2, 29, 13, 14, 15, 0) },
  { 'Dt6', 'DATETIME(6)', "'2024-02-29 13:14:15.123456'", M(2024, 2, 29, 13, 14, 15, 123456) },
  { 'Ts', 'TIMESTAMP NULL', "'2024-03-01 01:02:03'", M(2024, 3, 1, 1, 2, 3, 0) },
  { 'Ts3', 'TIMESTAMP(3) NULL', "'2024-03-01 01:02:03.456'", M(2024, 3, 1, 1, 2, 3, 456000) },
  { 'D', 'DATE', "'2024-12-31'", M(2024, 12, 31, 0, 0, 0, 0, true) },
  { 'T', 'TIME', "'-12:34:56'", D(true, 12, 34, 56, 0) },
  { 'T3', 'TIME(3)', "'838:59:58.500'", D(false, 838, 59, 58, 500000) },
  { 'Y', 'YEAR', '2024', 2024 },
  { 'Ch', 'CHAR(10)', "'abc'", 'abc' },
  { 'Vc', 'VARCHAR(300)', "'Привет'", 'Привет' },
  { 'Tx', 'TEXT', "'long text'", 'long text' },
  { 'Bn', 'BINARY(4)', '0x01020304', varbinary.new('\1\2\3\4') },
  { 'Vb', 'VARBINARY(20)', '0x00FF10', varbinary.new('\0\255\16') },
  { 'Bl', 'BLOB', '0xDEADBEEF', varbinary.new('\222\173\190\239') },
  { 'Bt', 'BIT(10)', "b'1010101010'", 682 },
  { 'En', "ENUM('red','green','blue')", "'green'", 'green' },
  { 'St', "SET('a','b','c')", "'a,c'", 'a,c' },
  { 'Js', 'JSON', [['{"a": "x\\"y", "b": [true, false, null, 1.5, -3, 70000], "c": {}}']],
    { json = true, text = '{"a": "x\\"y", "b": [true, false, null, 1.5, -3, 70000], "c": {}}',
      map = { a = 'x"y', b = { true, false, box.NULL, 1.5, -3, 70000 }, c = setmetatable({}, { __serialize = 'map' }) } } }
}

-- UUID of MariaDB, versions 1 and 4 kept with their groups reversed, 7 as it is
if MARIADB then
  for index, text in ipairs({ '6ccd780c-baba-1026-9564-5b8c656024db', 'f47ac10b-58cc-4372-a567-0e02b2c3d479',
                             '01890a5d-ac96-774b-bcce-b302099a8057' }) do
    table.insert(TYPES, { 'U' .. index, 'UUID', "'" .. text .. "'", uuid.fromstr(text) })
  end
end

local function type_columns()
  local names = {}
  for index, item in ipairs(TYPES) do
    names[index] = item[1]
  end
  return table.concat(names, ', ')
end

local function setup_server()
  local columns, values = { 'ID INT UNSIGNED PRIMARY KEY' }, { '1' }
  for _, item in ipairs(TYPES) do
    table.insert(columns, item[1] .. ' ' .. item[2])
    table.insert(values, item[3])
  end
  exec('DROP DATABASE IF EXISTS Registry',
    'CREATE DATABASE Registry',
    'USE Registry',
    'CREATE TABLE Locations (ID INT UNSIGNED PRIMARY KEY, Node INT UNSIGNED NOT NULL, Kind CHAR(1) NOT NULL, ' ..
      'Link INT UNSIGNED, Slot TINYINT UNSIGNED, ' ..
      'Modified TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, Note VARCHAR(50))',
    'CREATE TABLE Users (ID INT UNSIGNED PRIMARY KEY, Active TINYINT NOT NULL, `Call` VARCHAR(20) NOT NULL, ' ..
      'SSID TINYINT UNSIGNED, Text VARCHAR(100))',
    'CREATE TABLE Gateways (BMC INT UNSIGNED, TPC INT UNSIGNED, Comment VARCHAR(20), PRIMARY KEY (BMC, TPC))',
    'CREATE TABLE Other (ID INT PRIMARY KEY AUTO_INCREMENT, Value VARCHAR(20))',
    'CREATE TABLE Xa (ID INT UNSIGNED PRIMARY KEY, Value VARCHAR(20))',
    'CREATE TABLE Digits (D INT PRIMARY KEY)',
    'INSERT INTO Digits VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)',
    'CREATE TABLE Types (' .. table.concat(columns, ', ') .. ')',
    "CREATE USER IF NOT EXISTS 'replica'@'%' IDENTIFIED BY 'secret'",
    "GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replica'@'%'",
    'INSERT INTO Locations (ID, Node, Kind, Link, Slot, Modified) ' ..
      "SELECT seq, 2000 + seq % 7, 'N', IF(seq % 10 = 0, NULL, seq * 10), seq % 3, '2026-01-02 03:04:05' FROM " .. seq(1, 100),
    "INSERT INTO Users SELECT seq, seq % 2, CONCAT('C', seq), seq % 16, NULL FROM " .. seq(1, 20000),
    "INSERT INTO Gateways VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c')",
    'INSERT INTO Types (ID, ' .. type_columns() .. ') VALUES (' .. table.concat(values, ', ') .. ')',
    'INSERT INTO Types (ID) VALUES (3)')
end

local function setup_spaces()
  box.schema.space.create('GlobalLocations', { if_not_exists = true })
  box.space.GlobalLocations:create_index('ID', { if_not_exists = true, parts = { 1, 'unsigned' } })

  box.schema.space.create('GlobalProfiles', { if_not_exists = true, format = {
    { 'ID', 'unsigned' }, { 'Call', 'string' }, { 'Active', 'integer' },
    { name = 'SSID', type = 'unsigned', is_nullable = true } } })
  box.space.GlobalProfiles:create_index('ID', { if_not_exists = true, parts = { 1, 'unsigned' } })
  box.space.GlobalProfiles:create_index('Call', { if_not_exists = true, unique = false, parts = { 2, 'string' } })

  box.schema.space.create('GlobalXa', { if_not_exists = true })
  box.space.GlobalXa:create_index('ID', { if_not_exists = true, parts = { 1, 'unsigned' } })

  box.schema.space.create('GlobalGateways', { if_not_exists = true })
  box.space.GlobalGateways:create_index('Gateway', { if_not_exists = true, parts = { 1, 'unsigned', 2, 'unsigned' } })

  local format = { { 'ID', 'unsigned' } }
  for _, item in ipairs(TYPES) do
    table.insert(format, { name = item[1], type = 'any', is_nullable = true })
  end
  for _, name in ipairs({ 'Types', 'TypesNumber', 'TypesBinary' }) do
    box.schema.space.create(name, { if_not_exists = true, format = format })
    box.space[name]:create_index('ID', { if_not_exists = true, parts = { 1, 'unsigned' } })
  end
end

-- Replicator -----------------------------------------------------------------

local journal   = { reset = 0, insert = 0, update = 0, delete = 0 }
local triggered = 0
local profiled  = 0

-- A trigger that puts to a channel smaller than a batch: a write in a
-- transaction finds it full, yields and is aborted
local channel = fiber.channel(10)
fiber.create(function ()
  while channel:get() ~= nil do
  end
end)

-- The position goes to ReplicatorPositions under the server id 1001
local KEY = 1001

local function replicator(only_locations)
  local replica = MirrorFlayer.new(threaded({
    hosts = { 'mirrorflayer-nowhere:3306', HOST .. ':3306' },
    user = 'replica', password = 'secret', server = KEY,
    heartbeat = 1, retry = 1, batch = 500, check = 1,
    tls = (TLS == 'on' and true) or (TLS == 'ca' and { ca = '/certs/ca.pem' }) or false, compress = COMPRESS
  }))
  replica:map('Registry', 'Locations', 'GlobalLocations', { 'ID', 'Node', 'Kind', 'Link', 'Slot', 'Modified' })
  if not only_locations then
    replica:map('Registry', 'Locations', function (operation, old, new)
      assert(box.is_in_txn(), 'function target outside of a transaction')
      journal[operation] = journal[operation] + 1
    end, { 'ID', 'Node' })
    replica:map('Registry', 'Users', 'GlobalProfiles')
    replica:map('Registry', 'Gateways', 'GlobalGateways', { 'BMC', 'TPC', 'Comment' })
    replica:map('Registry', 'Types', 'Types')
    replica:map('Registry', 'Xa', 'GlobalXa', { 'ID', 'Value' })
  end
  if box.space.GlobalLocations ~= nil then
    box.space.GlobalLocations:on_replace(function ()
      triggered = triggered + 1
    end)
  end
  if not only_locations then
    box.space.GlobalProfiles:on_replace(function ()
      profiled = profiled + 1
      channel:put(true, 1)
    end)
  end
  return replica:start()
end

local function caught_up(replica)
  local target = gtid()
  wait(function ()
    local stat = replica:stat()
    if stat.status == 'failed' then
      error('replicator failed: ' .. tostring(stat.error), 0)
    end
    return stat.position == target and stat.status == 'streaming'
  end, 'position ' .. target)
  return target
end

-- Checks ---------------------------------------------------------------------

local function number(value)
  if value == box.NULL then
    return box.NULL
  end
  return tonumber(value)
end

local function locations_equal(what)
  local _, rows = sql:query('SELECT ID, Node, Kind, Link, Slot, Modified FROM Registry.Locations ORDER BY ID')
  local tuples = box.space.GlobalLocations:select({}, { iterator = 'ALL' })
  if #rows ~= #tuples then
    ok(false, '%s: Locations has %d rows, GlobalLocations %d', what, #rows, #tuples)
    return
  end
  for index, row in ipairs(rows) do
    local expected = { number(row[1]), number(row[2]), row[3], number(row[4]), number(row[5]), moment_of(row[6]) }
    if encode(expected) ~= encode(tuples[index]) then
      ok(false, '%s: row %s differs: %s vs %s', what, row[1], require('json').encode(expected), tostring(tuples[index]))
      return
    end
  end
  ok(true, '%s: GlobalLocations equals Locations (%d rows)', what, #rows)
end

local function check_types(id, what, space, mode, json, ids)
  space       = space or 'Types'
  local tuple = box.space[space]:get(id)
  if tuple == nil then
    ok(false, '%s: %s row %d is missing', what, space, id)
    return
  end
  local good = true
  for index, item in ipairs(TYPES) do
    local expected = expect(item[4], mode, json, ids)
    if not same(tuple[index + 1], expected) then
      good = false
      ok(false, '%s: %s %d.%s is %s (%s), expected %s', what, space, id, item[1],
        tostring(tuple[index + 1]), type(tuple[index + 1]), tostring(expected))
    end
  end
  ok(good, '%s: all %d typed values of %s row %d', what, #TYPES, space, id)
end

local function same_rows(left, right, what)
  local first, second = box.space.Types:get(left), box.space.Types:get(right)
  ok(first ~= nil and second ~= nil and same(first:transform(1, 1):totable(), second:transform(1, 1):totable()),
    '%s: Types rows %d and %d are identical', what, left, right)
end

local function stored_position(replica, key, target)
  wait(function ()
    local tuple = box.space[replica.config.position]:get(key)
    return tuple ~= nil and tuple[2] == target
  end, 'stored position ' .. target)
  ok(true, 'stored position %s', target)
end

-- A transaction of MySQL with the tag mftag, on a table nobody maps
local function tagged(value)
  exec("SET gtid_next = 'AUTOMATIC:mftag'", "INSERT INTO Registry.Other (Value) VALUES ('" .. value .. "')",
    "SET gtid_next = 'AUTOMATIC'")
end

-- Each user of an authentication plugin logs in with the password, and not
-- with a wrong one
local function authentication()
  local function login(name, password)
    return pcall(protocol.connect, { host = HOST, port = 3306, user = 'auth_' .. name, password = password,
      timeout = 30, tls = TLS ~= 'off' and { verify = 'none' } or nil, compress = COMPRESS })
  end
  for name in string.gmatch(AUTH, '[^,]+') do
    local status, connection = login(name, 'secret')
    local user = status and connection:value('SELECT CURRENT_USER()')
    ok(status and connection.plugin == PLUGINS[name] and string.find(tostring(user), 'auth_' .. name, 1, true) == 1,
      'authentication %s: %s, %s', name, status and connection.plugin or tostring(connection), tostring(user))
    if status then
      connection:close()
    end
    status = login(name, 'wrong')
    ok(not status, 'authentication %s: a wrong password is refused', name)
  end
end

-- Phases ---------------------------------------------------------------------

local function load()
  authentication()
  setup_server()
  setup_spaces()
  local replica = replicator()
  local target  = caught_up(replica)

  ok(replica:stat().loads == 1, 'one full load')
  ok(replica:stat().tls == (TLS ~= 'off'), 'TLS %s, connection %s', TLS, replica:stat().tls and 'encrypted' or 'plain')
  ok(replica:stat().compressed == COMPRESS, 'compress %s, connection %s', tostring(COMPRESS),
    replica:stat().compressed and 'compressed' or 'not compressed')
  -- zstd where MySQL has it, zlib of MariaDB
  local probe = protocol.connect({ host = HOST, port = 3306, user = 'root', password = 'root', timeout = 30,
    compress = COMPRESS })
  ok(probe.algorithm == (COMPRESS and (MARIADB and 'zlib' or 'zstd') or nil), 'compression algorithm %s',
    tostring(probe.algorithm))
  probe:close()
  locations_equal('load')
  ok(box.space.GlobalProfiles:count() == 20000, 'GlobalProfiles has 20000 rows')
  ok(encode(box.space.GlobalProfiles:get(5)) == encode({ 5, 'C5', 1, 5 }), 'GlobalProfiles columns follow the space format')
  ok(box.space.GlobalGateways:count() == 3, 'GlobalGateways has 3 rows')
  check_types(1, 'load')
  local nulls = { 3 }
  for index = 1, #TYPES do
    nulls[index + 1] = box.NULL
  end
  ok(encode(box.space.Types:get(3)) == encode(box.tuple.new(nulls)), 'load: Types row 3 is all NULL')
  ok(journal.reset == 1 and journal.insert == 100, 'function target: reset and 100 inserts (%d, %d)', journal.reset, journal.insert)
  ok(profiled >= 20000, 'a yielding trigger did not stop the load (%d calls)', profiled)
  ok(replica:stat().aborts <= 20, 'the pace kept aborts by the trigger few (%d)', replica:stat().aborts)
  ok(triggered >= 100, 'on_replace triggers fired during the load (%d)', triggered)
  stored_position(replica, KEY, target)

  -- Two replicators of one instance share neither a name nor a position key
  local twin = MirrorFlayer.new(threaded({ hosts = { HOST .. ':3306' }, user = 'replica', password = 'secret', server = KEY }))
  twin:map('Registry', 'Locations', 'GlobalLocations', { 'ID' })
  local status, problem = pcall(twin.start, twin)
  ok(not status and string.find(tostring(problem), 'config.name', 1, true) ~= nil,
    'a second replicator of the same name is refused: %s', tostring(problem))
  twin.config.name = 'Twin'
  status, problem = pcall(twin.start, twin)
  ok(not status and string.find(tostring(problem), 'config.key', 1, true) ~= nil,
    'a second replicator of the same position key is refused: %s', tostring(problem))

  local inserts = journal.insert
  triggered     = 0
  exec('USE Registry',
    'INSERT INTO Locations (ID, Node, Kind, Link, Slot, Modified) ' ..
      "SELECT seq, 3000, 'R', NULL, 1, '2026-05-06 07:08:09' FROM " .. seq(101, 110),
    "UPDATE Locations SET Node = 4000, Kind = 'X' WHERE ID BETWEEN 20 AND 29",
    'DELETE FROM Locations WHERE ID BETWEEN 30 AND 34',
    'UPDATE Locations SET ID = 1005 WHERE ID = 5',
    'START TRANSACTION',
    "INSERT INTO Locations (ID, Node, Kind, Modified) VALUES (111, 1, 'T', '2026-07-07 07:07:07')",
    "UPDATE Users SET `Call` = 'CHANGED', SSID = NULL WHERE ID = 7",
    'DELETE FROM Users WHERE ID = 8',
    'COMMIT',
    'START TRANSACTION',
    "INSERT INTO Locations (ID, Node, Kind) VALUES (999, 1, 'Z')",
    'ROLLBACK',
    "INSERT INTO Other (Value) VALUES ('unmapped')",
    "UPDATE Gateways SET TPC = 3, Comment = 'moved' WHERE BMC = 1 AND TPC = 2",
    "UPDATE Users SET Text = 'yield' WHERE ID BETWEEN 100 AND 129",
    'INSERT INTO Types (ID, ' .. type_columns() .. ') SELECT 2, ' .. type_columns() .. ' FROM Types WHERE ID = 1',
    'INSERT INTO Types (ID) VALUES (4)',
    'ALTER TABLE Locations ADD COLUMN Extra INT',
    "INSERT INTO Locations (ID, Node, Kind, Modified, Extra) VALUES (112, 2, 'A', '2026-08-08 08:08:08', 5)")
  target = caught_up(replica)

  locations_equal('stream')
  ok(box.space.GlobalLocations:get(5) == nil and box.space.GlobalLocations:get(1005) ~= nil, 'key change moved row 5 to 1005')
  ok(box.space.GlobalLocations:get(999) == nil, 'rolled back insert is absent')
  ok(encode(box.space.GlobalProfiles:get(7)) == encode({ 7, 'CHANGED', 1, box.NULL }), 'update in a multi-table transaction')
  ok(box.space.GlobalProfiles:get(8) == nil and box.space.GlobalProfiles:count() == 19999, 'delete in a multi-table transaction')
  ok(box.space.GlobalGateways:get({ 1, 2 }) == nil and box.space.GlobalGateways:get({ 1, 3 }) ~= nil, 'composite key change')
  ok(replica:stat().split >= 1, 'a transaction with a yielding trigger was applied statement by statement (%d)', replica:stat().split)
  ok(encode(box.space.GlobalProfiles:get(105)) == encode({ 105, 'C105', 1, 9 }), 'its rows are in place')
  check_types(2, 'stream')
  same_rows(1, 2, 'load vs stream')
  same_rows(3, 4, 'load vs stream NULL')
  ok(journal.insert - inserts == 12 and journal.update == 11 and journal.delete == 5,
    'function target: %d inserts, %d updates, %d deletes', journal.insert - inserts, journal.update, journal.delete)
  ok(triggered > 0, 'on_replace triggers fired during the stream (%d)', triggered)
  stored_position(replica, KEY, target)
  local stat = replica:stat()
  ok(type(stat.lag) == 'number' and type(stat.event_time) == 'number' and stat.event_time > 0,
    'stat: lag %s, event time %s', tostring(stat.lag), tostring(stat.event_time))

  -- The other modes, over the full load (rows 1-4) and the stream (row 5)
  local MODES = {
    { key = KEY + 1, space = 'TypesNumber', datetime = 'number', json = 'string', uuid = 'string' },
    { key = KEY + 2, space = 'TypesBinary', datetime = 'datetime', json = 'map', uuid = 'binary' }
  }
  for _, item in ipairs(MODES) do
    local other = MirrorFlayer.new(threaded({
      hosts = { HOST .. ':3306' }, user = 'replica', password = 'secret', server = item.key,
      name = item.space, datetime = item.datetime, json = item.json, uuid = item.uuid, heartbeat = 1, retry = 1,
      tls = (TLS == 'on' and true) or (TLS == 'ca' and { ca = '/certs/ca.pem' }) or false, compress = COMPRESS
    }))
    other:map('Registry', 'Types', item.space)
    item.replica = other:start()
    caught_up(other)
  end
  exec('INSERT INTO Registry.Types (ID, ' .. type_columns() .. ') SELECT 5, ' .. type_columns() .. ' FROM Registry.Types WHERE ID = 1')
  caught_up(replica)
  check_types(5, 'stream', 'Types', 'datetime')
  for _, item in ipairs(MODES) do
    caught_up(item.replica)
    check_types(1, 'load', item.space, item.datetime, item.json, item.uuid)
    check_types(5, 'stream', item.space, item.datetime, item.json, item.uuid)
  end

  -- JSON updates of MySQL, as diffs with PARTIAL_JSON: every mode ends with
  -- the document the server has
  if not MARIADB then
    exec([[UPDATE Registry.Types SET Js = JSON_SET(Js, '$.a', 'z', '$.d', 7, '$.b[0]', 2.5) WHERE ID = 5]],
      [[UPDATE Registry.Types SET Js = JSON_REMOVE(Js, '$.b[1]', '$.c') WHERE ID = 5]],
      [[UPDATE Registry.Types SET Js = JSON_REPLACE(Js, '$.d', JSON_OBJECT('k', 'v', 'long key', JSON_ARRAY(1, 2)))
        WHERE ID = 5]],
      [[UPDATE Registry.Types SET Js = JSON_SET(Js, '$."long key"', NULL, '$.b[last]', -1) WHERE ID = 5]],
      -- DECIMAL, DATETIME, DATE and TIME are opaque values inside JSON
      [[UPDATE Registry.Types SET Js = JSON_SET(Js, '$.e', CAST('12.50' AS DECIMAL(6, 2)),
        '$.f', CAST('2026-01-02 03:04:05.678' AS DATETIME(3)), '$.g', CAST('2026-01-02' AS DATE),
        '$.h', CAST('-838:59:58.5' AS TIME(1))) WHERE ID = 5]])
    local text = sql:value('SELECT Js FROM Registry.Types WHERE ID = 5')
    local function same(left, right)
      if type(left) ~= 'table' or type(right) ~= 'table' then
        return left == right
      end
      for key, value in pairs(left) do
        if not same(value, right[key]) then return false end
      end
      for key in pairs(right) do
        -- box.NULL == nil: a key with JSON null is there all the same
        if rawequal(left[key], nil) then return false end
      end
      return true
    end
    caught_up(replica)
    ok(same(box.space.Types:get(5).Js, require('json').decode(text)), 'partial JSON, map: %s',
      require('json').encode(box.space.Types:get(5).Js))
    for _, item in ipairs(MODES) do
      caught_up(item.replica)
      local value = box.space[item.space]:get(5).Js
      if item.json == 'string' then
        ok(value == text, 'partial JSON, %s: %s vs %s', item.space, tostring(value), text)
      else
        ok(same(value, require('json').decode(text)), 'partial JSON, %s: %s', item.space, require('json').encode(value))
      end
    end
  end
  for _, item in ipairs(MODES) do
    item.replica:stop()
  end

  -- XA: a prepared transaction waits for its decision; x4 stays prepared for
  -- the resume, the replicator restarts in between. Galera has no XA: a plain
  -- row keeps GlobalXa from being empty, which the resume would load alone
  if SERVER == 'galera' then
    exec("INSERT INTO Registry.Xa VALUES (1, 'one')")
    caught_up(replica)
  else
    exec("XA START 'x1'", "INSERT INTO Registry.Xa VALUES (1, 'one')", "XA END 'x1'", "XA PREPARE 'x1'")
    caught_up(replica)
    ok(box.space.GlobalXa:get(1) == nil, 'XA: prepared, not applied')
    exec("XA COMMIT 'x1'")
    caught_up(replica)
    ok(box.space.GlobalXa:get(1) ~= nil and box.space.GlobalXa:get(1)[2] == 'one', 'XA: applied by XA COMMIT')
    exec("XA START 'x2','b2',7", "INSERT INTO Registry.Xa VALUES (2, 'two')", "XA END 'x2','b2',7",
      "XA PREPARE 'x2','b2',7", "XA ROLLBACK 'x2','b2',7")
    caught_up(replica)
    ok(box.space.GlobalXa:get(2) == nil, 'XA: dropped by XA ROLLBACK')
    exec("XA START 'x3'", "INSERT INTO Registry.Xa VALUES (3, 'three')", "XA END 'x3'", "XA COMMIT 'x3' ONE PHASE")
    caught_up(replica)
    ok(box.space.GlobalXa:get(3) ~= nil, 'XA: one phase')
    exec("XA START 'x4'", "UPDATE Registry.Xa SET Value = 'uno' WHERE ID = 1", "INSERT INTO Registry.Xa VALUES (4, 'four')",
      "XA END 'x4'", "XA PREPARE 'x4'")
    caught_up(replica)
    ok(box.space.GlobalXa:get(4) == nil and box.space.ReplicatorPositionsXA:count() == 1,
      'XA: x4 prepared and kept for the resume')
  end

  -- A tagged GTID of MySQL 8.3 and later: the position takes the tag
  if not MARIADB and GTID then
    tagged('load')
    caught_up(replica)
    ok(string.find(replica:stat().position, ':mftag:', 1, true) ~= nil, 'tagged position %s', replica:stat().position)
  end

  -- A node of Galera that leaves the cluster is left, and taken again when it is back
  if SERVER == 'galera' then
    if not THREAD then
      ok(replica.runner.connection ~= nil and replica.runner.connection.galera == true, 'the node is known as one of Galera')
    end
    exec("SET GLOBAL wsrep_cluster_address = ''")
    wait(function ()
      -- Our check, or the node itself refusing its clients (1047)
      local problem = tostring(replica:stat().error)
      return replica:stat().status == 'waiting' and
        (string.find(problem, 'not ready', 1, true) ~= nil or string.find(problem, 'WSREP', 1, true) ~= nil)
    end, 'a node out of the cluster to be left')
    ok(true, 'a node out of the cluster is left: %s', tostring(replica:stat().error))
    exec("SET GLOBAL wsrep_cluster_address = 'gcomm://'")
    wait(function ()
      local _, rows = sql:query("SHOW GLOBAL STATUS LIKE 'wsrep_ready'")
      return rows[1] ~= nil and rows[1][2] == 'ON'
    end, 'the node back in its cluster')
    exec("INSERT INTO Registry.Locations (ID, Node, Kind, Modified) VALUES (113, 3, 'G', '2026-08-09 10:11:12')")
    caught_up(replica)
    locations_equal('galera')
  end
  replica:stop()
end

local function resume()
  setup_spaces()
  -- Emptied while down: the table is loaded alone, without a full load
  box.space.GlobalGateways:truncate()
  -- The stream asked for after a tagged position, with a tagged GTID after it
  if not MARIADB and GTID then
    tagged('resume')
  end
  exec('USE Registry',
    "INSERT INTO Locations (ID, Node, Kind, Modified) SELECT seq, 5000, 'D', '2026-09-09 09:09:09' FROM " .. seq(201, 205),
    'DELETE FROM Locations WHERE ID = 101',
    'UPDATE Locations SET Node = 5555 WHERE ID = 102')
  local replica = replicator()
  caught_up(replica)
  ok(replica:stat().loads == 0 and journal.reset == 0, 'continued from the stored GTID without a full load')
  -- x4, prepared before the restart, decided now
  if SERVER ~= 'galera' then
    exec("XA COMMIT 'x4'")
    caught_up(replica)
    ok(box.space.GlobalXa:get(4) ~= nil and box.space.GlobalXa:get(1)[2] == 'uno' and
      box.space.ReplicatorPositionsXA:count() == 0, 'XA: x4 prepared before the restart applied by XA COMMIT')
  end
  ok(replica:stat().fills == 1 and box.space.GlobalGateways:count() == 3,
    'the emptied GlobalGateways was loaded alone (%d fills, %d rows)', replica:stat().fills, box.space.GlobalGateways:count())
  ok(journal.insert == 5 and journal.delete == 1 and journal.update == 1, 'only the changes made while down were applied')
  locations_equal('resume')
  exec("INSERT INTO Registry.Locations (ID, Node, Kind, Modified) VALUES (300, 6, 'S', '2026-10-10 10:10:10')")
  local target = caught_up(replica)
  locations_equal('resume stream')
  local before = box.space.GlobalLocations:count()
  replica:reload()
  ok(box.space.GlobalLocations:count() == before, 'reload keeps the spaces filled')
  wait(function () return replica:stat().loads == 1 end, 'full load after reload()')
  target = caught_up(replica)
  locations_equal('reload')
  stored_position(replica, KEY, target)
  -- With flush the spaces are emptied table by table and loaded again
  replica:reload(true)
  wait(function () return replica:stat().loads == 2 end, 'full load after reload(true)')
  target = caught_up(replica)
  locations_equal('reload with flush')
  ok(box.space.GlobalProfiles:count() == 19999 and box.space.GlobalGateways:count() == 3,
    'reload(true) filled the spaces again (%d profiles, %d gateways)', box.space.GlobalProfiles:count(),
    box.space.GlobalGateways:count())
  stored_position(replica, KEY, target)
  replica:stop()
end

local function purge()
  setup_spaces()
  exec('USE Registry',
    'DELETE FROM Locations WHERE ID <= 10',
    "INSERT INTO Locations (ID, Node, Kind, Modified) VALUES (400, 7, 'P', '2026-11-11 11:11:11')",
    'FLUSH BINARY LOGS')
  -- A file is purged only after its binlog checkpoint, which comes later
  local file = binlog_file()
  wait(function ()
    exec('PURGE BINARY LOGS TO ' .. protocol.literal(file))
    local _, logs = sql:query('SHOW BINARY LOGS')
    return #logs == 1
  end, 'binlog purge')
  ok(true, 'binlogs before %s purged', file)
  local replica = replicator()
  wait(function () return replica:stat().loads == 1 end, 'full reload after 1236')
  local target = caught_up(replica)
  ok(journal.reset == 1, 'function target got reset')
  locations_equal('purge')
  ok(box.space.GlobalLocations:get(1) == nil, 'rows deleted while down are removed by the reload')
  ok(box.space.GlobalProfiles:count() == 19999, 'GlobalProfiles kept 19999 rows')
  ok(profiled == 0, 'the reload did not rewrite unchanged profiles (%d trigger calls)', profiled)
  stored_position(replica, KEY, target)
  replica:stop()
end

local function late()
  -- Under its key lies a position of another kind of source: not taken, a full load
  box.schema.space.create('ReplicatorPositions', { if_not_exists = true })
  box.space.ReplicatorPositions:create_index('ID', { if_not_exists = true, parts = { 1, 'unsigned' } })
  box.space.ReplicatorPositions:replace({ KEY, 'mirrorflayer/0/16B3748', 0, 'postgresql' })
  -- Started before its space exists, as when a later plug-in makes it
  local replica = replicator(true)
  wait(function () return string.find(tostring(replica:stat().error), 'does not exist', 1, true) ~= nil end, 'a missing space')
  ok(replica:stat().status ~= 'failed', 'a missing space is retried: %s', tostring(replica:stat().error))
  box.schema.space.create('GlobalLocations', { if_not_exists = true })
  box.space.GlobalLocations:create_index('ID', { if_not_exists = true, parts = { 1, 'unsigned' } })
  exec("INSERT INTO Registry.Locations (ID, Node, Kind, Modified) VALUES (600, 8, 'L', '2026-12-12 12:12:12')")
  local target = caught_up(replica)
  ok(replica:stat().loads == 1, 'a fresh instance makes one full load')
  locations_equal('late')
  ok(box.space.GlobalLocations:get(600) ~= nil, 'the row inserted meanwhile is there')
  stored_position(replica, KEY, target)
  ok(box.space.ReplicatorPositions:get(KEY)[3] > 0, 'event time stored next to the position')
  ok(box.space.ReplicatorPositions:get(KEY)[4] == 'mysql', 'the position of another kind was replaced by one of mysql')
  -- Cancelled from outside, as on shutdown: no error, just stopped
  -- In TX its fiber may be cancelled from outside; in a thread stop() does it
  if THREAD then
    replica:stop()
  else
    replica.fiber:cancel()
  end
  wait(function () return replica:stat().status == 'stopped' end, 'stop after a cancel')
  ok(replica:stat().error == nil, 'a cancel is not an error: %s', tostring(replica:stat().error))
  replica:stop()
end

-- Unmapped tables go to handlers.discover: a database is asked about once and
-- an ignored one never again, a table the handler maps and reconnects for
-- arrives with the rows written before, a table it handles without mapping
-- is asked again after the reconnect; map() at run time with reconnect() of
-- an idle source
local function discover()
  box.schema.space.create('GlobalLocations', { if_not_exists = true })
  box.space.GlobalLocations:create_index('ID', { if_not_exists = true, parts = { 1, 'unsigned' } })
  box.schema.space.create('FreshItems', { if_not_exists = true })
  box.space.FreshItems:create_index('ID', { if_not_exists = true, parts = { 1, 'unsigned' } })
  box.schema.space.create('DiscoveredGateways', { if_not_exists = true })
  box.space.DiscoveredGateways:create_index('Gateway', { if_not_exists = true, parts = { 1, 'unsigned', 2, 'unsigned' } })
  exec('DROP DATABASE IF EXISTS Fresh', 'DROP DATABASE IF EXISTS Ignored')
  local config = threaded({
    hosts = { 'mirrorflayer-nowhere:3306', HOST .. ':3306' }, user = 'replica', password = 'secret', server = KEY + 1,
    name = 'Discovery',
    heartbeat = 1, retry = 1, check = 1,
    tls = (TLS == 'on' and true) or (TLS == 'ca' and { ca = '/certs/ca.pem' }) or false, compress = COMPRESS
  })
  ok(not pcall(MirrorFlayer.new, config, { discovered = function () end }), 'new() refuses an unknown handler')
  local asked, described, failures = {}, nil, {}
  local replica = MirrorFlayer.new(config, {
    fail = function (replica, problem)
      table.insert(failures, problem)
    end,
    discover = function (replica, database, table_name, columns)
      local key   = table_name and database .. '.' .. table_name or database
      asked[key] = (asked[key] or 0) + 1
      if key == 'Fresh.Items' then
        described = columns
        replica:map('Fresh', 'Items', 'FreshItems', { 'ID', 'Name' })
        replica:reconnect({ 'Fresh.Items' })
      end
      return key == 'Fresh' or key == 'Fresh.Items' or key == 'Registry.Other'
    end
  })
  replica:map('Registry', 'Locations', 'GlobalLocations', { 'ID', 'Node', 'Kind', 'Link', 'Slot', 'Modified' })
  replica:start()
  caught_up(replica)
  local first = failures[1]
  ok(first ~= nil and first.host == 'mirrorflayer-nowhere' and first.fatal == false and type(first.message) == 'string' and
    first.message ~= '', 'fail gets the failed session of the host that is not there: %s',
    first and tostring(first.message) or 'none')
  local failed = #failures
  ok(not pcall(replica.reconnect, replica, { 'Fresh.Items' }), 'reconnect() refuses a table that is not mapped')
  exec('CREATE DATABASE Ignored',
    'CREATE TABLE Ignored.T (ID INT PRIMARY KEY)',
    'INSERT INTO Ignored.T VALUES (1), (2)',
    'INSERT INTO Ignored.T VALUES (3)',
    'CREATE DATABASE Fresh',
    'CREATE TABLE Fresh.Items (ID INT UNSIGNED PRIMARY KEY, Name VARCHAR(20))',
    "UPDATE Registry.Users SET Text = 'd1' WHERE ID = 1",
    "INSERT INTO Registry.Other (Value) VALUES ('d1')",
    "INSERT INTO Fresh.Items VALUES (1, 'one'), (2, 'two')",
    "INSERT INTO Fresh.Items VALUES (3, 'three')",
    "UPDATE Fresh.Items SET Name = 'deux' WHERE ID = 2",
    "UPDATE Registry.Users SET Text = 'd2' WHERE ID = 1",
    "INSERT INTO Registry.Other (Value) VALUES ('d2')",
    'INSERT INTO Ignored.T VALUES (4)')
  caught_up(replica)
  ok(asked.Ignored == 1 and asked['Ignored.T'] == nil, 'an ignored database is asked about once, its tables never')
  ok(asked.Fresh == 1 and asked['Fresh.Items'] == 1 and asked.Registry == nil,
    'a new database and its table asked about once, a mapped database never')
  ok(asked['Registry.Users'] == 1, 'an ignored table is not asked about again after a reconnect (%s)',
    tostring(asked['Registry.Users']))
  ok(asked['Registry.Other'] == 2, 'a table handled without a mapping is asked about again after a reconnect (%s)',
    tostring(asked['Registry.Other']))
  ok(type(described) == 'table' and #described == 2 and described[1].name == 'ID' and described[1].type == 'int' and
    described[2].name == 'Name' and described[2].type == 'varchar', 'discover gets the columns with their types')
  local items = {}
  for _, tuple in box.space.FreshItems:pairs() do
    items[tuple[1]] = tuple[2]
  end
  ok(box.space.FreshItems:count() == 3 and items[1] == 'one' and items[2] == 'deux' and items[3] == 'three',
    'the discovered table arrived with the rows written before it was mapped (%d rows)', box.space.FreshItems:count())
  ok(replica:stat().fills == 1 and replica:stat().loads == 1 and replica:stat().error == nil,
    'loaded alone after the reconnect, without a full load or an error (%d fills, %d loads, %s)',
    replica:stat().fills, replica:stat().loads, tostring(replica:stat().error))
  -- map() at run time, reconnect() from outside while the source is idle
  replica:map('Registry', 'Gateways', 'DiscoveredGateways', { 'BMC', 'TPC', 'Comment' })
  replica:reconnect({ 'Registry.Gateways' })
  local gateways = tonumber(sql:value('SELECT COUNT(*) FROM Registry.Gateways'))
  wait(function ()
    return replica:stat().fills == 2 and replica:stat().status == 'streaming'
  end, 'Registry.Gateways loaded after map() and reconnect()', 10)
  ok(box.space.DiscoveredGateways:count() == gateways, 'map() at run time loaded with reconnect() (%d rows of %d)',
    box.space.DiscoveredGateways:count(), gateways)
  exec("INSERT INTO Registry.Gateways VALUES (9, 9, 'discovered')")
  caught_up(replica)
  ok(box.space.DiscoveredGateways:get({ 9, 9 }) ~= nil, 'the table mapped at run time streams')
  ok(#failures == failed, 'a reconnect is not a failure (%d calls of fail, %d before)', #failures, failed)
  replica:stop()
  exec('DROP DATABASE Fresh', 'DROP DATABASE Ignored', 'DELETE FROM Registry.Gateways WHERE BMC = 9')
end

local phases = { load = load, resume = resume, purge = purge, late = late, discover = discover }

local status, problem = pcall(phases[phase] or function () error('unknown phase ' .. tostring(phase), 0) end)
if not status then
  failures = failures + 1
  log.error('FAIL: %s: %s', phase, tostring(problem))
end
sql:close()
if failures > 0 then
  log.error('phase %s: %d failures', phase, failures)
  os.exit(1)
end
log.info('phase %s passed', phase)
os.exit(0)
