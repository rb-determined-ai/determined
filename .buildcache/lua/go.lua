-- match(go)

-- Function                     Returns                                                 Default
-- -----------------------------------------------------------------------------------------------------
-- can_handle_command()         Can the wrapper handle this program?                    true
-- resolve_args()               (nothing)                                               -
-- get_capabilities()           A list of supported capabilities                        An empty table
-- get_build_files()            A table of build result files                           An empty table
-- get_program_id()             A unique program identification                         The MD4 hash of the program binary
-- get_relevant_arguments()     Arguments that can affect the build output              All arguments
-- get_relevant_env_vars()      Environment variables that can affect the build output  An empty table
-- get_input_files()*           Get the paths to the input files for the command        And empty table
-- preprocess_source()          The preprocessed source code (e.g. for C/C++)           An empty string
-- get_implicit_input_files()*  Get a list of paths to implicit input files (includes)  And empty table

require_std("bcache")
require_std("string")

function get_capabilities()
    return {"direct_mode"}
end

function get_program_id()
    print("getting program id")
    -- get the version string for go
    local result = bcache.run({ARGS[1], "version"})
    if result.return_code ~= 0 then
        error("failed to get program id")
    end

    return result.std_out
end

function preprocess_source()
    -- for go, we only want "direct_mode", so force this to be a cache miss
    local result = bcache.run({"/bin/sh", "-c", "head -c 32 /dev/urandom | base64"})
    if result.return_code ~= 0 then
        error("failed to read /dev/urandom")
    end
    return result.std_out
end

function get_input_files()
    local files = {"go.mod", "go.sum"}
    local result = bcache.run({"/bin/find", ".", "-name", "*.go", "-type", "f", "-print0"})
    if result.return_code ~= 0 then
        error("failed to find go files")
    end
    for file in string.gmatch(result.std_out, "[^\0]+") do
        files[#files+1] = file
    end
    return files
end

function get_relevant_arguments()
    -- copy all of ARGS
    local relevant = {}
    for i = 1, #ARGS do
        relevant[i] = ARGS[i]
    end

    -- include `go env GOOS GOARCH` settings even though they're not true args
    local result = bcache.run({ARGS[1], "env", "GOOS", "GOARCH"})
    if result.return_code ~= 0 then
        error("failed to run go env")
    end
    relevant[#relevant+1] = result.std_out
    return relevant
end

function get_build_files()
    out = {binary="build/determined-master"}
    return out
end
