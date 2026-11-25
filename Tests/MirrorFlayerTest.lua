#!/usr/bin/env tarantool
-- Docker test of MirrorFlayer against MariaDB, a node of MariaDB Galera or
-- MySQL (SERVER), with or without TLS (TLS = off | on | ca), driven by
-- Tests/DockerTest.sh:
--   load    full load, values of all types, INSERT/UPDATE/DELETE over the stream
--   resume  restart continues from the stored GTID without a full load, a
--           table whose space was emptied is loaded alone
--   purge   the position is purged from the binlog: error 1236, full reload
--   late    a fresh instance started before its space exists

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

-- MariaDB alone or a node of Galera
local MARIADB = SERVER ~= 'mysql'

require('compat').binary_data_decoding = 'new'

box.cfg({ work_dir = os.getenv('MIRRORFLAYER_DATA') or '/data', log_level = 5 })

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
  local replica = MirrorFlayer.new({
    hosts = { 'mirrorflayer-nowhere:3306', HOST .. ':3306' },
    user = 'replica', password = 'secret', server = KEY,
    heartbeat = 1, retry = 1, batch = 500, check = 1,
    tls = (TLS == 'on' and true) or (TLS == 'ca' and { ca = '/certs/ca.pem' }) or false
  })
  replica:map('Registry', 'Locations', 'GlobalLocations', { 'ID', 'Node', 'Kind', 'Link', 'Slot', 'Modified' })
  if not only_locations then
    replica:map('Registry', 'Locations', function (operation, old, new)
      assert(box.is_in_txn(), 'function target outside of a transaction')
      journal[operation] = journal[operation] + 1
    end, { 'ID', 'Node' })
    replica:map('Registry', 'Users', 'GlobalProfiles')
    replica:map('Registry', 'Gateways', 'GlobalGateways', { 'BMC', 'TPC', 'Comment' })
    replica:map('Registry', 'Types', 'Types')
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
  space = space or 'Types'
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

-- Phases ---------------------------------------------------------------------

local function load()
  setup_server()
  setup_spaces()
  local replica = replicator()
  local target  = caught_up(replica)

  ok(replica:stat().loads == 1, 'one full load')
  ok(replica:stat().tls == (TLS ~= 'off'), 'TLS %s, connection %s', TLS, replica:stat().tls and 'encrypted' or 'plain')
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

  local inserts = journal.insert
  triggered = 0
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
    local other = MirrorFlayer.new({
      hosts = { HOST .. ':3306' }, user = 'replica', password = 'secret', server = item.key,
      name = item.space, datetime = item.datetime, json = item.json, uuid = item.uuid, heartbeat = 1, retry = 1,
      tls = (TLS == 'on' and true) or (TLS == 'ca' and { ca = '/certs/ca.pem' }) or false
    })
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
    item.replica:stop()
  end

  -- A node of Galera that leaves the cluster is left, and taken again when it is back
  if SERVER == 'galera' then
    ok(replica.connection ~= nil and replica.connection.galera == true, 'the node is known as one of Galera')
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
  exec('USE Registry',
    "INSERT INTO Locations (ID, Node, Kind, Modified) SELECT seq, 5000, 'D', '2026-09-09 09:09:09' FROM " .. seq(201, 205),
    'DELETE FROM Locations WHERE ID = 101',
    'UPDATE Locations SET Node = 5555 WHERE ID = 102')
  local replica = replicator()
  caught_up(replica)
  ok(replica:stat().loads == 0 and journal.reset == 0, 'continued from the stored GTID without a full load')
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
  -- Cancelled from outside, as on shutdown: no error, just stopped
  replica.fiber:cancel()
  wait(function () return replica:stat().status == 'stopped' end, 'stop after a cancel')
  ok(replica:stat().error == nil, 'a cancel is not an error: %s', tostring(replica:stat().error))
  replica:stop()
end

local phases = { load = load, resume = resume, purge = purge, late = late }

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
