local has_datetime, datetime = pcall(require, 'datetime')

if not has_datetime then
    error('Datetime unsupported, use Tarantool 2.10 or newer')
end

-- Do not set listen for now so connector won't be
-- able to send requests until everything is configured.
box.cfg{
    work_dir = os.getenv("TEST_TNT_WORK_DIR"),
}

box.schema.user.create('test', { password = 'test' , if_not_exists = true })
box.schema.user.grant('test', 'execute', 'universe', nil, { if_not_exists = true })

function get_datetime_no_offset()
    return datetime.new({ year = 2023, month = 9, day = 2, tzoffset = 0 })
end

function get_datetime_offset()
    return datetime.new({ year = 2023, month = 9, day = 2, tzoffset = 240 })
end

box.once("init", function()
    local s_1 = box.schema.space.create('testDatetime_1', {
        id = 524,
        if_not_exists = true,
    })
    s_1:create_index('primary', {
        type = 'TREE',
        parts = {
            { field = 1, type = 'datetime' },
        },
        if_not_exists = true
    })
    s_1:truncate()

    local s_3 = box.schema.space.create('testDatetime_2', {
        id = 526,
        if_not_exists = true,
    })
    s_3:create_index('primary', {
        type = 'tree',
        parts = {
            {1, 'uint'},
        },
        if_not_exists = true
    })
    s_3:truncate()

    box.schema.func.create('call_datetime_testdata')
    box.schema.user.grant('test', 'read,write', 'space', 'testDatetime_1', { if_not_exists = true })
    box.schema.user.grant('test', 'read,write', 'space', 'testDatetime_2', { if_not_exists = true })
end)

local function call_datetime_testdata()
    local dt1 = datetime.new({ year = 1934 })
    local dt2 = datetime.new({ year = 1961 })
    local dt3 = datetime.new({ year = 1968 })
    return {
        {
            5, "Go!", {
                {"Klushino", dt1},
                {"Baikonur", dt2},
                {"Novoselovo", dt3},
            },
        }
    }
end
rawset(_G, 'call_datetime_testdata', call_datetime_testdata)

local function call_interval_testdata(interval)
    return interval
end
rawset(_G, 'call_interval_testdata', call_interval_testdata)

local function call_datetime_interval(dtleft, dtright)
    return dtright - dtleft
end
rawset(_G, 'call_datetime_interval', call_datetime_interval)

-- Set listen only when every other thing is configured.
box.cfg{
    listen = os.getenv("TEST_TNT_LISTEN"),
}

require('console').start()
