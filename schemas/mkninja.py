from root import tools

schemafiles = tools.manifest(
    where=SRC,
    cmd="find expconf -type f",
    out=BLD/"schemafiles"
)
