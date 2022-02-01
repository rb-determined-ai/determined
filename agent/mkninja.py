from root import tools, proto, master

gofiles = tools.manifest(
    where=SRC,
    cmd="find . -type f -name *.go",
    out=BLD/"gofiles",
)

agent_bin = BLD/"determined-agent"
determined_agent = add_target(
    outputs=[agent_bin],
    # note that the agent will read some files straight from the master
    inputs=[gofiles, proto.protobuild, master.gofiles],
    command=f"go build -o '{agent_bin}' ./cmd/determined-agent",
)
