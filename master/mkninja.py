from root import tools, proto

gogenfiles = tools.manifest(
    where=SRC,
    cmd="git grep -l //go:generate pkg/schemas",
    out=BLD/"gogenfiles",
)

gogen_stamp = BLD/"gogen.stamp"
gogen = add_target(
    inputs=[gogenfiles, SRC/".."/"schemas"/"gen.py"],
    outputs=[gogen_stamp],
    command=f"go generate ./pkg/schemas/... && touch {gogen_stamp}",
    workdir=SRC,
)

gofiles = tools.manifest(
    where=SRC,
    cmd="find . -type f -name *.go",
    out=BLD/"gofiles",
    after=[gogen],
)

master_bin = BLD/"determined-master"
determined_master = add_target(
    outputs=[master_bin],
    inputs=[gofiles, proto.protobuild],
    command=f"go build -o '{master_bin}' ./cmd/determined-master",
)
