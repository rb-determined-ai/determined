-- match(echo)

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

function get_program_id()
    print(ARGS)
    return ARGS[1]
end
