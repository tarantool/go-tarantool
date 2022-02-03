redefined = false

globals = {
    'box',
    'utf8',
    'checkers',
    '_TARANTOOL'
}

include_files = {
    '**/*.lua',
    '*.luacheckrc',
    '*.rockspec'
}

exclude_files = {
    '**/*.rocks/'
}

max_line_length = 120

ignore = {
    "212/self",   -- Unused argument <self>.
    "411",        -- Redefining a local variable.
    "421",        -- Shadowing a local variable.
    "431",        -- Shadowing an upvalue.
    "432",        -- Shadowing an upvalue argument.
}
