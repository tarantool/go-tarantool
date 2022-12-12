-- Do not set listen for now so connector won't be
-- able to send requests until everything is configured.
box.cfg{
    work_dir = os.getenv("TEST_TNT_WORK_DIR"),
    memtx_use_mvcc_engine = os.getenv("TEST_TNT_MEMTX_USE_MVCC_ENGINE") == 'true' or nil,
}

box.once("init", function()
    local st = box.schema.space.create('schematest', {
        id = 616,
        temporary = true,
        if_not_exists = true,
        field_count = 7,
        format = {
            {name = "name0", type = "unsigned"},
            {name = "name1", type = "unsigned"},
            {name = "name2", type = "string"},
            {name = "name3", type = "unsigned"},
            {name = "name4", type = "unsigned"},
            {name = "name5", type = "string"},
        },
    })
    st:create_index('primary', {
        type = 'hash',
        parts = {1, 'uint'},
        unique = true,
        if_not_exists = true,
    })
    st:create_index('secondary', {
        id = 3,
        type = 'tree',
        unique = false,
        parts = { 2, 'uint', 3, 'string' },
        if_not_exists = true,
    })
    st:truncate()

    local s = box.schema.space.create('test', {
        id = 617,
        if_not_exists = true,
    })
    s:create_index('primary', {
        type = 'tree',
        parts = {1, 'uint'},
        if_not_exists = true
    })

    local s = box.schema.space.create('teststring', {
        id = 618,
        if_not_exists = true,
    })
    s:create_index('primary', {
        type = 'tree',
        parts = {1, 'string'},
        if_not_exists = true
    })

    local s = box.schema.space.create('testintint', {
        id = 619,
        if_not_exists = true,
    })
    s:create_index('primary', {
        type = 'tree',
        parts = {1, 'int', 2, 'int'},
        if_not_exists = true
    })

    local s = box.schema.space.create('SQL_TEST', {
        id = 620,
        if_not_exists = true,
        format = {
            {name = "NAME0", type = "unsigned"},
            {name = "NAME1", type = "string"},
            {name = "NAME2", type = "string"},
        }
    })
    s:create_index('primary', {
        type = 'tree',
        parts = {1, 'uint'},
        if_not_exists = true
    })
    s:insert{1, "test", "test"}

    local s = box.schema.space.create('test_perf', {
        id = 621,
        temporary = true,
        if_not_exists = true,
        field_count = 3,
        format = {
            {name = "id", type = "unsigned"},
            {name = "name", type = "string"},
            {name = "arr1", type = "array"},
        },
    })
    s:create_index('primary', {
        type = 'tree',
        unique = true,
        parts = {1, 'unsigned'},
        if_not_exists = true
    })
    s:create_index('secondary', {
        id = 5, type = 'tree',
        unique = false,
        parts = {2, 'string'},
        if_not_exists = true
    })
    local arr_data = {}
    for i = 1,100 do
        arr_data[i] = i
    end
    for i = 1,1000 do
        s:insert{
            i,
            'test_name',
            arr_data,
        }
    end

    local s = box.schema.space.create('test_error_type', {
        id = 622,
        temporary = true,
        if_not_exists = true,
        field_count = 2,
        -- You can't specify box.error as format type,
        -- but can put box.error objects.
    })
    s:create_index('primary', {
        type = 'tree',
        unique = true,
        parts = {1, 'string'},
        if_not_exists = true
    })

    --box.schema.user.grant('guest', 'read,write,execute', 'universe')
    box.schema.func.create('box.info')
    box.schema.func.create('simple_concat')

    -- auth testing: access control
    box.schema.user.create('test', {password = 'test'})
    box.schema.user.grant('test', 'execute', 'universe')
    box.schema.user.grant('test', 'read,write', 'space', 'test')
    box.schema.user.grant('test', 'read,write', 'space', 'schematest')
    box.schema.user.grant('test', 'read,write', 'space', 'test_perf')
    box.schema.user.grant('test', 'read,write', 'space', 'test_error_type')

    -- grants for sql tests
    box.schema.user.grant('test', 'create,read,write,drop,alter', 'space')
    box.schema.user.grant('test', 'create', 'sequence')

    box.schema.user.create('no_grants')
end)

local function func_name()
    return {
        {221, "", {
                {"Moscow", 34},
                {"Minsk", 23},
                {"Kiev", 31},
            }
        }
    }
end
rawset(_G, 'func_name', func_name)

local function simple_concat(a)
    return a .. a
end
rawset(_G, 'simple_concat', simple_concat)

local function push_func(cnt)
    for i = 1, cnt do
        box.session.push(i)
    end
    return cnt
end
rawset(_G, 'push_func', push_func)

local function tarantool_version_at_least(wanted_major, wanted_minor, wanted_patch)
    -- https://github.com/tarantool/crud/blob/733528be02c1ffa3dacc12c034ee58c9903127fc/test/helper.lua#L316-L337
    local major_minor_patch = _TARANTOOL:split('-', 1)[1]
    local major_minor_patch_parts = major_minor_patch:split('.', 2)

    local major = tonumber(major_minor_patch_parts[1])
    local minor = tonumber(major_minor_patch_parts[2])
    local patch = tonumber(major_minor_patch_parts[3])

    if major < (wanted_major or 0) then return false end
    if major > (wanted_major or 0) then return true end

    if minor < (wanted_minor or 0) then return false end
    if minor > (wanted_minor or 0) then return true end

    if patch < (wanted_patch or 0) then return false end
    if patch > (wanted_patch or 0) then return true end

    return true
end

if tarantool_version_at_least(2, 4, 1) then
    local e1 = box.error.new(box.error.UNKNOWN)
    rawset(_G, 'simple_error', e1)

    local e2 = box.error.new(box.error.TIMEOUT)
    e2:set_prev(e1)
    rawset(_G, 'chained_error', e2)

    local user = box.session.user()
    box.schema.func.create('forbidden_function', {body = 'function() end'})
    box.session.su('no_grants')
    local _, access_denied_error = pcall(function() box.func.forbidden_function:call() end)
    box.session.su(user)
    rawset(_G, 'access_denied_error', access_denied_error)

    -- cdata structure is as follows:
    --
    -- tarantool> err:unpack()
    -- - code: val
    --   base_type: val
    --   type: val
    --   message: val
    --   field1: val
    --   field2: val
    --   trace:
    --   - file: val
    --     line: val

    local function compare_box_error_attributes(expected, actual, attr_provider)
        for attr, _ in pairs(attr_provider:unpack()) do
            if (attr ~= 'prev') and (attr ~= 'trace') then
                if expected[attr] ~= actual[attr] then
                    error(('%s expected %s is not equal to actual %s'):format(
                           attr, expected[attr], actual[attr]))
                end
            end
        end
    end

    local function compare_box_errors(expected, actual)
        if (expected == nil) and (actual ~= nil) then
            error(('Expected error stack is empty, but actual error ' ..
                   'has previous %s (%s) error'):format(
                   actual.type, actual.message))
        end

        if (expected ~= nil) and (actual == nil) then
            error(('Actual error stack is empty, but expected error ' ..
                   'has previous %s (%s) error'):format(
                   expected.type, expected.message))
        end

        compare_box_error_attributes(expected, actual, expected)
        compare_box_error_attributes(expected, actual, actual)

        if (expected.prev ~= nil) or (actual.prev ~= nil) then
            return compare_box_errors(expected.prev, actual.prev)
        end

        return true
    end

    rawset(_G, 'compare_box_errors', compare_box_errors)
end

box.space.test:truncate()

--box.schema.user.revoke('guest', 'read,write,execute', 'universe')

-- Set listen only when every other thing is configured.
box.cfg{
    listen = os.getenv("TEST_TNT_LISTEN"),
}
