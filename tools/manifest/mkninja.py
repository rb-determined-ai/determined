import shlex

manifest_bin = BLD/"manifest"

add_target(
    inputs=[SRC/"manifest.go", SRC/"go.mod"],
    outputs=[manifest_bin],
    command=["go", "build", "-o", manifest_bin],
    display="building manifest binary",
)

def manifest(where, cmd, out, after=()):
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)
    return add_target(
        inputs=[manifest_bin],
        command=["(", *cmd, ")", "|", manifest_bin, out],
        outputs=[out],
        workdir=where,
        display=f"listing files: {' '.join(cmd)}",
        phony=True,
        after=after,
    )
