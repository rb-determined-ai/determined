-- match(build-webui)

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
require_std("os")

function get_capabilities()
    return {"direct_mode"}
end

function get_program_id()
    -- what versions would I need to include??
    return "craco"
end

function get_relevant_env_vars()
    -- return a table of keys and values (this is not documented by buildcache)
    return {
        PUBLIC_URL = os.getenv("PUBLIC_URL"),
        ESLINT_NO_DEV_ERRORS = os.getenv("ESLINT_NO_DEV_ERRORS"),
    }
end

function preprocess_source()
    -- for go, we only want "direct_mode", so force this to be a cache miss
    local result = bcache.run({"/bin/sh", "-c", "head -c 32 /dev/urandom | base64"})
    if result.return_code ~= 0 then
        error("failed to read /dev/urandom")
    end
    return result.std_out
end

function list_files(path, files)
    local result = bcache.run({"/bin/find", path, "-type", "f", "-print0"})
    if result.return_code ~= 0 then
        error("failed to find src files")
    end
    for file in string.gmatch(result.std_out, "[^\0]+") do
        files[#files+1] = file
    end
end

function get_input_files()
    local files = {
        "craco.config.js",
        "jest.config.js",
        "package.json",
        "package-lock.json",
        "tsconfig.json",
    }

    list_files("public", files)
    list_files("src", files)

    return files
end

---- This doesn't work for 2 reasons:
---- 1) you can't do math on strings (std_out = std_out + "more std out")
---- 2) bcache.run hides the output so the build becomes silent
-- function run_for_miss()
--     -- result must be a table of std_out, std_err, and return_code
--     result = bcache.run(ARGS)
--     if result.return_code ~= 0 then
--         return result
--     end
--
--     -- HACK: to make the output cacheable, we make a tarball out of it.
--
--     temp = bcache.run({"/bin/tar", "-cf", "build.tar", "build"})
--     -- combine results
--     result.std_out = result.std_out + temp.std_out
--     result.std_err = result.std_err + temp.std_err
--     result.return_code = temp.return_code
--     if result.return_code ~= 0 then
--         return result
--     end
--
--     temp = bcache.run({"/bin/rm", "-rf", "build"})
--     -- combine results
--     result.std_out = result.std_out + temp.std_out
--     result.std_err = result.std_err + temp.std_err
--     result.return_code = temp.return_code
--     if result.return_code ~= 0 then
--         return result
--     end
--
--     return result
-- end

function get_build_files()
    return {binary="build.tar"}
end
