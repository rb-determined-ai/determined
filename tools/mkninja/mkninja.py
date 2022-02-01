import argparse
import importlib
import io
import os
import pathlib
import shlex
import sys
import textwrap
from importlib import machinery, util

_aliases = []
_proj = []
_src = []
_bld = []

def get_cur_src():
    return _src[-1]

def get_cur_bld():
    return _bld[-1]

def add_target_object(target):
    proj = _proj[-1]
    proj.targets.append(target)
    return target

def add_target(
    *,
    command=(),
    outputs=(),
    inputs=(),
    after=(),
    phony=False,
    workdir=None,
    dyndep=None,
    display=None,
):
    if workdir is None:
        workdir = get_cur_src()

    target = Target(
        command=command,
        outputs=outputs,
        inputs=inputs,
        after=after,
        phony=phony,
        workdir=workdir,
        dyndep=dyndep,
        display=display,
    )

    add_target_object(target)
    return target

## add_subproject needs more support from ninja itself before it is a good
## idea; currently the subninja command does not provide sufficient insulation
## to run a subproject in isolation and expect it to be unaffected.
# def add_subproject(name, alias="root"):
#     proj = _proj[-1]
#     src = _src[-1]/name
#     bld = _bld[-1]/name
#     root_bld = _bld[0]
#     with Project(src, bld, name, alias) as p:
#         module = importlib.import_module(name)
#
#     proj.mkninja_files += p.mkninja_files
#
#     ninjafile = bld/"build.ninja"
#     with ninjafile.open("w") as f:
#         print(p.gen(root_bld, isroot=False), file=f)
#
#     proj.subninjas.append(ninjafile)
#
#     return module


class _Loader(machinery.SourceFileLoader):
    """
    Our Loader extends the built-in SourceFileLoader.  The SourceFileLoader
    is able to load and execute a python script from a file, but we want to
    expose specific values just before executing the module, specifically:
      - SRC: the current source file
      - BLD: the current build file
      - add_target(): adds a build target into the generated ninja file
        know the top-level module name to import it)
    """

    def __init__(self, fullname, path, proj, alias):
        self.proj = proj
        self.alias = alias
        super().__init__(fullname, path)

    def exec_module(self, module):
        if self.alias is not None:
            sys.modules[self.alias] = module
            _aliases[-1][self.alias] = module
        relpath = "/".join(module.__name__.split(".")[1:])
        # set SRC
        src = self.proj.src/relpath
        setattr(module, "SRC", src)
        # set BLD
        bld = self.proj.bld/relpath
        setattr(module, "BLD", bld)
        # expose mkninja builtins
        setattr(module, "add_target", add_target)
        # setattr(module, "add_subproject", add_subproject)
        try:
            # while executing this module, the default workdir should be src
            _bld.append(bld)
            _src.append(src)
            return super().exec_module(module)
        finally:
            _bld.pop()
            _src.pop()

    def set_data(self, *args, **kwarg):
        # We don't want to generate annoying __pycache__ directories in the
        # source tree, so we will disable caching of our mkninja.py files.
        raise NotImplementedError()


class _Finder:
    """
    When `import project.some.subdir` is encountered, load the file at
    project/some/subdir/mkninja.py instead of the normal import behavior.
    """
    def __init__(self, proj):
        self.proj = proj
        self.loaders = {}

    def find_spec(self, name, path=None, target=None):
        nameparts = name.split(".")
        base = nameparts[0]
        subs = nameparts[1:]
        if base not in (self.proj.truename, self.proj.alias):
            # Return None to let the next Finder in sys.meta_path take over.
            return None

        # target is None in the normal import case
        assert target is None, f"can't handle target={target}"

        truename = ".".join([self.proj.truename] + subs)
        if self.proj.alias is not None:
            alias = ".".join([self.proj.alias] + subs)
        else:
            alias = None


        # We want normal source-loading mechanics, with these exceptions:
        #  - we want to load the wrong source (fullname != path)
        #  - we want to pretend it's a package and not a module
        #    (submodule_search_locations is a list)

        # The code we use for the module root.sub is at root/sub/mkninja.py.
        submodule_path = "/".join(subs)
        src = str(self.proj.src / submodule_path / "mkninja.py")

        self.proj.mkninja_files.append(src)

        loader = _Loader(truename, src, self.proj, alias)

        return util.spec_from_file_location(
            truename, src, loader=loader, submodule_search_locations=[]
        )


def ninjify(s):
    """apply ninja syntax escapes"""
    s = str(s)
    s = s.replace("$", "$$")
    s = s.replace("\n", "$\n")
    s = s.replace(" ", "$ ")
    s = s.replace(":", "$:")
    return s


class Target:
    def __init__(
        self, *, command, outputs, inputs, after, phony, workdir, dyndep, display
    ):
        assert isinstance(outputs, (list, tuple)), type(outputs)
        assert isinstance(inputs, (list, tuple)), type(inputs)
        assert isinstance(after, (list, tuple)), type(after)

        temp = []
        for i in inputs:
            if hasattr(i, "as_input"):
                temp += i.as_input()
            else:
                temp.append(i)
        inputs = temp

        temp = []
        for a in after:
            if hasattr(a, "as_after"):
                temp += a.as_after()
            else:
                temp.append(a)
        after = temp

        if isinstance(command, str):
            command = shlex.split(command)

        if hasattr(dyndep, "as_dyndep"):
            dyndep = dyndep.as_dyndep()

        self.inputs = inputs
        self.after = after
        self.command = command
        self.outputs = outputs
        self.workdir = workdir
        self.phony = phony
        self.dyndep = dyndep
        self.display = display

    def as_after(self):
        return self.outputs

    def as_input(self):
        return self.outputs

    def as_dyndep(self):
        assert len(self.outputs) == 1, (
            "passing a Target as a dyndep requires that the target have "
            "exactly one output"
        )
        return self.outputs[0]

    def gen(self, bld):

        def relbld(s):
            s = str(s)
            if str(bld) in s:
                s = os.path.relpath(s, str(bld))
            return s

        out = f"build"
        if self.outputs:
            out += ' ' + ' '.join(ninjify(relbld(o)) for o in self.outputs)
        out += ": TARGET |"
        if self.inputs:
            out += ' ' + ' '.join(ninjify(relbld(i)) for i in self.inputs)
        if self.phony:
            out += " PHONY"
        out += " ||"
        if self.after:
            out += ' ' + ' '.join(ninjify(relbld(a)) for a in self.after)
        # if self.dyndep:
        #     out += " " + ninjify(self.dyndep)
        out += "\n CMD = " + " ".join(ninjify(c) for c in self.command)
        out += "\n WORKDIR = " + ninjify(self.workdir)
        if self.dyndep:
            out += "\n dyndep = " + ninjify(relbld(self.dyndep))
        if self.display:
            out += "\n DISPLAY = " + ninjify(relbld(self.display))
        return out



class Project:
    def __init__(self, src, bld, truename, alias=None):
        self.src = pathlib.Path(src).absolute()
        self.bld = pathlib.Path(bld).absolute()
        self.truename = truename
        self.alias = alias
        self.finder = _Finder(self)
        self.targets = []
        self.mkninja_files = []
        self.subninjas = []

    def __enter__(self):
        sys.meta_path = [self.finder] + sys.meta_path
        _proj.append(self)
        # remove the current set of aliases
        if _aliases:
            for alias in _aliases[-1]:
                sys.modules.pop(alias)
        _aliases.append({})
        return self

    def __exit__(self, *_):
        sys.meta_path.remove(self.finder)
        _proj.pop()
        # remove any aliases we created
        for alias in _aliases[-1]:
            sys.modules.pop(alias)
        _aliases.pop()
        if _aliases:
            # restore our parent's aliases
            for alias, module in _aliases[-1].items():
                sys.modules[alias] = module

    def gen(self, bld, rerun_script=None, isroot=True):
        f = io.StringIO()

        for subninja in self.subninjas:
            print("subninja ", ninjify(subninja), file=f)

        print(textwrap.dedent("""
            rule TARGET
             command = cd $WORKDIR && $CMD
             description = $DISPLAY
             restat = 1
        """).lstrip(), file=f)

        if isroot:
            print(textwrap.dedent("""
                # phony target is always out of date
                build PHONY: phony
            """).lstrip(), file=f)

        if rerun_script:
            mkninja_deps = [__file__]
            mkninja_deps += [ninjify(f) for f in self.mkninja_files]
            mkninja_deps = " ".join(mkninja_deps)
            print(textwrap.dedent(f"""
                # regenerate ninja files based on the original command line
                build build.ninja: TARGET {rerun_script} {mkninja_deps}
                 CMD = sh {rerun_script}
                 WORKDIR = .
                 DISPLAY = regenerating ninja files
                 generator = 1
            """).lstrip(), file=f)

        for target in self.targets:
            print(target.gen(bld), file=f, end="\n\n")

        # post-process the text to use $SRC and $BLD
        text = f.getvalue()
        bld = str(self.bld)
        src = str(self.src)
        if bld in src:
            text = text.replace(src, "$SRC")
            text = text.replace(bld, "$BLD")
        else:
            text = text.replace(bld, "$BLD")
            text = text.replace(src, "$SRC")

        return f"# global path variables\nBLD={bld}\nSRC={src}\n\n{text}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser("mkninja")
    parser.add_argument("src")
    args = parser.parse_args(sys.argv[1:])

    src = pathlib.Path(args.src).absolute()
    bld = pathlib.Path(".").absolute()

    truename = "arbitrary_module_name"
    alias = "root"

    # store the exact command we ran with
    rerun_script = bld / ".rerun_mkninja.sh"
    with rerun_script.open("w") as f:
        print("#!/bin/sh", file=f)
        quoted = [shlex.quote(sys.executable)]
        quoted += [shlex.quote(arg) for arg in sys.argv]
        print(*quoted, file=f)

    # We don't need the src directory in our sys.path because we have a custom
    # Finder on the sys.metapath.

    with Project(src, bld, truename, alias) as p:
        importlib.import_module(truename)

    with open("build.ninja", "w") as f:
        print(p.gen(bld, rerun_script), file=f)
