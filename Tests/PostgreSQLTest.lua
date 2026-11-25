#!/usr/bin/env tarantool
-- Docker test of MirrorFlayer against PostgreSQL, with or without TLS
-- (TLS = off | on | ca), driven by Tests/DockerTest.sh:
--   load    full load in the snapshot of the slot, values of all types,
--           INSERT/UPDATE/DELETE over pgoutput, TOAST values an update kept
--   resume  restart continues from the stored LSN without a full load, a
--           table whose space was emptied is loaded alone
--   purge   the slot is dropped while down: full reload
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
local protocol     = require('MirrorFlayer.PostgreSQL.Protocol')
local output       = require('MirrorFlayer.PostgreSQL.Output')

local phase = arg[1]
local HOST  = os.getenv('SERVER_HOST') or 'mirrorflayer-server'
local TLS   = os.getenv('TLS') or 'off'

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

-- The server of a fresh container may still be starting: the readiness
-- check of DockerTest.sh runs inside it, a client from outside retries
local function open()
  local deadline = fiber.clock() + 30
  while true do
    local status, connection = pcall(protocol.connect, { host = HOST, port = 5432, user = 'postgres',
      password = 'root', database = 'postgres', timeout = 30 })
    if status then
      return connection
    end
    if fiber.clock() > deadline then
      error(connection, 0)
    end
    log.info('waiting for the server: %s', tostring(connection))
    fiber.sleep(1)
  end
end

local sql = open()

local function exec(...)
  for _, statement in ipairs({ ... }) do
    sql:query(statement)
  end
end

-- The LSN the source has flushed
local function current()
  return output.lsn(sql:value('SELECT pg_current_wal_flush_lsn()'))
end

local function lsn_of(position)
  local text = string.match(tostring(position), '^[^/]+/(%x+/%x+)$')
  return text and output.lsn(text) or nil
end

-- Schema ---------------------------------------------------------------------

local function D(negative, hours, minutes, seconds, usec)
  return { duration = true, negative = negative, hours = hours, minutes = minutes, seconds = seconds, usec = usec }
end

local function M(year, month, day, hour, minute, second, usec)
  return { moment = true, year = year, month = month, day = day, hour = hour, minute = minute,
    second = second, usec = usec }
end

local function I(fields, seconds)
  return { interval = true, fields = fields, seconds = seconds }
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
  if type(value) ~= 'table' then
    return value
  end
  if value.json then
    return json == 'string' and value.text or value.map
  end
  if value.interval then
    return mode == 'number' and value.seconds or datetime.interval.new(value.fields)
  end
  if value.duration then
    local sign = value.negative and -1 or 1
    if mode == 'number' then
      return sign * (value.hours * 3600 + value.minutes * 60 + value.seconds + value.usec / 1e6)
    end
    return datetime.interval.new({ hour = sign * value.hours, min = sign * value.minutes, sec = sign * value.seconds,
      nsec = sign * value.usec * 1000 })
  end
  if value.moment then
    local moment = datetime.new({ year = value.year, month = value.month, day = value.day, hour = value.hour,
      min = value.minute, sec = value.second, nsec = value.usec * 1000, tzoffset = 0 })
    return mode == 'number' and moment.epoch + value.usec / 1e6 or moment
  end
  return value
end

local function moment_of(text)
  if text == box.NULL then
    return box.NULL
  end
  local year, month, day, hour, minute, second = string.match(text, '^(%d+)-(%d+)-(%d+) (%d+):(%d+):(%d+)')
  return expect(M(tonumber(year), tonumber(month), tonumber(day), tonumber(hour), tonumber(minute),
    tonumber(second), 0))
end

local JSON_TEXT = '{"a": "x\\"y", "b": [true, false, null, 1.5, -3, 70000], "c": {}}'
local JSON_MAP  = { a = 'x"y', b = { true, false, box.NULL, 1.5, -3, 70000 }, c = setmetatable({}, { __serialize = 'map' }) }

local TYPES = {
  { 'Small', 'SMALLINT', '-30000', -30000 },
  { 'IntS', 'INTEGER', '-2000000000', -2000000000 },
  { 'Big', 'BIGINT', '-9007199254740993', -9007199254740993LL },
  { 'Flt', 'REAL', '1.1', tonumber(ffi.new('float', 1.1)) },
  { 'Dbl', 'DOUBLE PRECISION', '2.5e100', 2.5e100 },
  { 'Dec1', 'NUMERIC(10,2)', '-12.30', decimal.new('-12.30') },
  { 'Dec2', 'NUMERIC(30,12)', '123456789012345678.123456789012', decimal.new('123456789012345678.123456789012') },
  { 'Bool', 'BOOLEAN', 'true', true },
  { 'Dt', 'TIMESTAMP', "'2024-02-29 13:14:15'", M(2024, 2, 29, 13, 14, 15, 0) },
  { 'Dt6', 'TIMESTAMP(6)', "'2024-02-29 13:14:15.123456'", M(2024, 2, 29, 13, 14, 15, 123456) },
  { 'Ts', 'TIMESTAMPTZ', "'2024-03-01 01:02:03+00'", M(2024, 3, 1, 1, 2, 3, 0) },
  { 'Ts3', 'TIMESTAMPTZ(3)', "'2024-03-01 04:02:03.456+03'", M(2024, 3, 1, 1, 2, 3, 456000) },
  { 'D', 'DATE', "'2024-12-31'", M(2024, 12, 31, 0, 0, 0, 0) },
  { 'T', 'TIME', "'12:34:56'", D(false, 12, 34, 56, 0) },
  { 'T3', 'TIME(3)', "'23:59:58.5'", D(false, 23, 59, 58, 500000) },
  { 'Iv', 'INTERVAL', "'1 day 02:03:04.5'", I({ day = 1, hour = 2, min = 3, sec = 4, nsec = 500000000 }, 93784.5) },
  { 'Ch', 'CHAR(10)', "'abc'", 'abc       ' },
  { 'Vc', 'VARCHAR(300)', "'Привет'", 'Привет' },
  { 'Tx', 'TEXT', "'long text'", 'long text' },
  { 'By', 'BYTEA', "'\\x00ff10'", varbinary.new('\0\255\16') },
  { 'En', '"Registry"."Color"', "'green'", 'green' },
  { 'Dm', '"Registry"."Positive"', '5', 5 },
  { 'U', 'UUID', "'f47ac10b-58cc-4372-a567-0e02b2c3d479'", uuid.fromstr('f47ac10b-58cc-4372-a567-0e02b2c3d479') },
  { 'Js', 'JSON', "'" .. JSON_TEXT .. "'", { json = true, text = JSON_TEXT, map = JSON_MAP } },
  { 'Jb', 'JSONB', "'" .. JSON_TEXT .. "'", { json = true, text = JSON_TEXT, map = JSON_MAP } }
}

local function quoted(names)
  local result = {}
  for index, name in ipairs(names) do
    result[index] = '"' .. name .. '"'
  end
  return table.concat(result, ', ')
end

local function type_columns()
  local names = {}
  for index, item in ipairs(TYPES) do
    names[index] = item[1]
  end
  return quoted(names)
end

local function setup_server()
  local columns, values = { '"ID" INTEGER PRIMARY KEY' }, { '1' }
  for _, item in ipairs(TYPES) do
    table.insert(columns, '"' .. item[1] .. '" ' .. item[2])
    table.insert(values, item[3])
  end
  for _, name in ipairs({ 'mirrorflayer', 'mirrorflayer_number', 'mirrorflayer_binary' }) do
    exec(string.format("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '%s'", name))
  end
  exec('DROP PUBLICATION IF EXISTS mirrorflayer',
    'DROP SCHEMA IF EXISTS "Registry" CASCADE',
    'DROP ROLE IF EXISTS replica',
    'CREATE SCHEMA "Registry"',
    [[CREATE TYPE "Registry"."Color" AS ENUM ('red', 'green', 'blue')]],
    'CREATE DOMAIN "Registry"."Positive" AS INTEGER CHECK (VALUE > 0)',
    'CREATE TABLE "Registry"."Locations" ("ID" INTEGER PRIMARY KEY, "Node" INTEGER NOT NULL, "Kind" CHAR(1) NOT NULL, ' ..
      '"Link" INTEGER, "Slot" SMALLINT, "Modified" TIMESTAMPTZ NOT NULL DEFAULT now(), "Note" VARCHAR(50))',
    'CREATE TABLE "Registry"."Users" ("ID" INTEGER PRIMARY KEY, "Active" SMALLINT NOT NULL, "Call" VARCHAR(20) NOT NULL, ' ..
      '"SSID" SMALLINT, "Text" VARCHAR(100))',
    'CREATE TABLE "Registry"."Gateways" ("BMC" INTEGER, "TPC" INTEGER, "Comment" VARCHAR(20), PRIMARY KEY ("BMC", "TPC"))',
    'CREATE TABLE "Registry"."Other" ("ID" SERIAL PRIMARY KEY, "Value" VARCHAR(20))',
    'CREATE TABLE "Registry"."Types" (' .. table.concat(columns, ', ') .. ')',
    "CREATE ROLE replica LOGIN REPLICATION PASSWORD 'secret'",
    'GRANT USAGE ON SCHEMA "Registry" TO replica',
    'GRANT SELECT ON ALL TABLES IN SCHEMA "Registry" TO replica',
    'CREATE PUBLICATION mirrorflayer FOR ALL TABLES',
    'INSERT INTO "Registry"."Locations" ("ID", "Node", "Kind", "Link", "Slot", "Modified") ' ..
      "SELECT s, 2000 + s % 7, 'N', CASE WHEN s % 10 = 0 THEN NULL ELSE s * 10 END, s % 3, '2026-01-02 03:04:05+00' " ..
      'FROM generate_series(1, 100) s',
    [[INSERT INTO "Registry"."Users" SELECT s, s % 2, 'C' || s, s % 16, NULL FROM generate_series(1, 20000) s]],
    [[INSERT INTO "Registry"."Gateways" VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c')]],
    'INSERT INTO "Registry"."Types" ("ID", ' .. type_columns() .. ') VALUES (' .. table.concat(values, ', ') .. ')',
    'INSERT INTO "Registry"."Types" ("ID") VALUES (3)')
end

local function setup_spaces()
  box.schema.space.create('GlobalLocations', { if_not_exists = true })
  box.space.GlobalLocations:create_index('ID', { if_not_exists = true, parts = { 1, 'unsigned' } })

  box.schema.space.create('GlobalProfiles', { if_not_exists = true, format = {
    { 'ID', 'unsigned' }, { 'Call', 'string' }, { 'Active', 'integer' },
    { name = 'SSID', type = 'unsigned', is_nullable = true } } })
  box.space.GlobalProfiles:create_index('ID', { if_not_exists = true, parts = { 1, 'unsigned' } })

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

local channel = fiber.channel(10)
fiber.create(function ()
  while channel:get() ~= nil do
  end
end)

-- The position goes to ReplicatorPositions under the slot name
local KEY = 'mirrorflayer'

local function settings(extra)
  local config = {
    type = 'postgresql',
    hosts = { 'mirrorflayer-nowhere:5432', HOST },
    user = 'replica', password = 'secret', database = 'postgres',
    heartbeat = 1, retry = 1, batch = 500,
    tls = (TLS == 'on' and true) or (TLS == 'ca' and { ca = '/certs/ca.pem' }) or false
  }
  for key, value in pairs(extra or {}) do
    config[key] = value
  end
  return threaded(config)
end

local function replicator(only_locations)
  local replica = MirrorFlayer.new(settings())
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

-- Waits until the replicator streams at or after the flushed LSN of the source
local function caught_up(replica)
  local target = current()
  wait(function ()
    local stat = replica:stat()
    if stat.status == 'failed' then
      error('replicator failed: ' .. tostring(stat.error), 0)
    end
    local position = lsn_of(stat.position)
    return stat.status == 'streaming' and position ~= nil and position >= target
  end, 'LSN ' .. output.format(target))
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
  local _, rows = sql:query('SELECT "ID", "Node", "Kind", "Link", "Slot", "Modified" FROM "Registry"."Locations" ORDER BY "ID"')
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
    local position = tuple and lsn_of(tuple[2])
    return position ~= nil and position >= target
  end, 'stored position ' .. output.format(target))
  ok(true, 'stored position at or after %s', output.format(target))
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
  exec('INSERT INTO "Registry"."Locations" ("ID", "Node", "Kind", "Link", "Slot", "Modified") ' ..
      "SELECT s, 3000, 'R', NULL, 1, '2026-05-06 07:08:09+00' FROM generate_series(101, 110) s",
    [[UPDATE "Registry"."Locations" SET "Node" = 4000, "Kind" = 'X' WHERE "ID" BETWEEN 20 AND 29]],
    'DELETE FROM "Registry"."Locations" WHERE "ID" BETWEEN 30 AND 34',
    'UPDATE "Registry"."Locations" SET "ID" = 1005 WHERE "ID" = 5',
    'BEGIN',
    [[INSERT INTO "Registry"."Locations" ("ID", "Node", "Kind", "Modified") VALUES (111, 1, 'T', '2026-07-07 07:07:07+00')]],
    [[UPDATE "Registry"."Users" SET "Call" = 'CHANGED', "SSID" = NULL WHERE "ID" = 7]],
    'DELETE FROM "Registry"."Users" WHERE "ID" = 8',
    'COMMIT',
    'BEGIN',
    [[INSERT INTO "Registry"."Locations" ("ID", "Node", "Kind") VALUES (999, 1, 'Z')]],
    'ROLLBACK',
    [[INSERT INTO "Registry"."Other" ("Value") VALUES ('unmapped')]],
    [[UPDATE "Registry"."Gateways" SET "TPC" = 3, "Comment" = 'moved' WHERE "BMC" = 1 AND "TPC" = 2]],
    [[UPDATE "Registry"."Users" SET "Text" = 'yield' WHERE "ID" BETWEEN 100 AND 129]],
    'INSERT INTO "Registry"."Types" ("ID", ' .. type_columns() .. ') SELECT 2, ' .. type_columns() ..
      ' FROM "Registry"."Types" WHERE "ID" = 1',
    'INSERT INTO "Registry"."Types" ("ID") VALUES (4)',
    'ALTER TABLE "Registry"."Locations" ADD COLUMN "Extra" INTEGER',
    [[INSERT INTO "Registry"."Locations" ("ID", "Node", "Kind", "Modified", "Extra") VALUES (112, 2, 'A', '2026-08-08 08:08:08+00', 5)]],
    -- A TOAST value out of line, then an update that leaves it alone
    [[INSERT INTO "Registry"."Types" ("ID", "Tx") SELECT 6, string_agg(md5(s::text), '') FROM generate_series(1, 500) s]],
    'UPDATE "Registry"."Types" SET "Small" = 1 WHERE "ID" = 6')
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
  local toast = sql:value('SELECT "Tx" FROM "Registry"."Types" WHERE "ID" = 6')
  local row6  = box.space.Types:get(6)
  ok(row6 ~= nil and row6[2] == 1 and row6[20] == toast and #toast == 16000,
    'an update kept the TOAST value it did not send (%s bytes)', row6 and tostring(row6[20] and #row6[20]) or 'no row')
  ok(journal.insert - inserts == 12 and journal.update == 11 and journal.delete == 5,
    'function target: %d inserts, %d updates, %d deletes', journal.insert - inserts, journal.update, journal.delete)
  ok(triggered > 0, 'on_replace triggers fired during the stream (%d)', triggered)
  stored_position(replica, KEY, target)
  local stat = replica:stat()
  ok(type(stat.lag) == 'number' and type(stat.event_time) == 'number' and stat.event_time > 0,
    'stat: lag %s, event time %s', tostring(stat.lag), tostring(stat.event_time))

  -- The other modes, over the full load (rows 1-4) and the stream (row 5),
  -- each replicator with a slot of its own
  local MODES = {
    { slot = 'mirrorflayer_number', space = 'TypesNumber', datetime = 'number', json = 'string', uuid = 'string' },
    { slot = 'mirrorflayer_binary', space = 'TypesBinary', datetime = 'datetime', json = 'map', uuid = 'binary' }
  }
  for _, item in ipairs(MODES) do
    local other = MirrorFlayer.new(settings({ slot = item.slot, name = item.space, datetime = item.datetime,
      json = item.json, uuid = item.uuid }))
    other:map('Registry', 'Types', item.space)
    item.replica = other:start()
    caught_up(other)
  end
  exec('INSERT INTO "Registry"."Types" ("ID", ' .. type_columns() .. ') SELECT 5, ' .. type_columns() ..
    ' FROM "Registry"."Types" WHERE "ID" = 1')
  caught_up(replica)
  check_types(5, 'stream', 'Types', 'datetime')
  for _, item in ipairs(MODES) do
    caught_up(item.replica)
    check_types(1, 'load', item.space, item.datetime, item.json, item.uuid)
    check_types(5, 'stream', item.space, item.datetime, item.json, item.uuid)
    item.replica:stop()
    -- Its slot is not needed any more and would keep WAL
    wait(function ()
      return pcall(exec, string.format("SELECT pg_drop_replication_slot('%s')", item.slot))
    end, 'drop of slot ' .. item.slot)
  end
  replica:stop()
end

local function resume()
  setup_spaces()
  -- Emptied while down: the table is loaded alone, without a full load
  box.space.GlobalGateways:truncate()
  exec('INSERT INTO "Registry"."Locations" ("ID", "Node", "Kind", "Modified") ' ..
      "SELECT s, 5000, 'D', '2026-09-09 09:09:09+00' FROM generate_series(201, 205) s",
    'DELETE FROM "Registry"."Locations" WHERE "ID" = 101',
    'UPDATE "Registry"."Locations" SET "Node" = 5555 WHERE "ID" = 102')
  local replica = replicator()
  caught_up(replica)
  ok(replica:stat().loads == 0 and journal.reset == 0, 'continued from the stored LSN without a full load')
  ok(replica:stat().fills == 1 and box.space.GlobalGateways:count() == 3,
    'the emptied GlobalGateways was loaded alone (%d fills, %d rows)', replica:stat().fills, box.space.GlobalGateways:count())
  ok(journal.insert == 5 and journal.delete == 1 and journal.update == 1, 'only the changes made while down were applied')
  locations_equal('resume')
  exec([[INSERT INTO "Registry"."Locations" ("ID", "Node", "Kind", "Modified") VALUES (300, 6, 'S', '2026-10-10 10:10:10+00')]])
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
  exec('DELETE FROM "Registry"."Locations" WHERE "ID" <= 10',
    [[INSERT INTO "Registry"."Locations" ("ID", "Node", "Kind", "Modified") VALUES (400, 7, 'P', '2026-11-11 11:11:11+00')]])
  wait(function ()
    return pcall(exec, "SELECT pg_drop_replication_slot('mirrorflayer')")
  end, 'drop of the slot')
  ok(true, 'the slot is dropped')
  local replica = replicator()
  wait(function () return replica:stat().loads == 1 end, 'full reload after the slot is gone')
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
  local replica = replicator(true)
  wait(function () return string.find(tostring(replica:stat().error), 'does not exist', 1, true) ~= nil end, 'a missing space')
  ok(replica:stat().status ~= 'failed', 'a missing space is retried: %s', tostring(replica:stat().error))
  box.schema.space.create('GlobalLocations', { if_not_exists = true })
  box.space.GlobalLocations:create_index('ID', { if_not_exists = true, parts = { 1, 'unsigned' } })
  exec([[INSERT INTO "Registry"."Locations" ("ID", "Node", "Kind", "Modified") VALUES (600, 8, 'L', '2026-12-12 12:12:12+00')]])
  local target = caught_up(replica)
  ok(replica:stat().loads == 1, 'a fresh instance makes one full load')
  locations_equal('late')
  ok(box.space.GlobalLocations:get(600) ~= nil, 'the row inserted meanwhile is there')
  stored_position(replica, KEY, target)
  ok(box.space.ReplicatorPositions:get(KEY)[3] > 0, 'event time stored next to the position')
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
