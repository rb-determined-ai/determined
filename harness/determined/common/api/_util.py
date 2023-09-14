import enum
from typing import Callable, Dict, Iterator, List, Optional, Set, Tuple, TypeVar, Union

import urllib3

from determined.common import api, util
from determined.common.api import bindings

# from determined.cli.render import Animator


class PageOpts(str, enum.Enum):
    single = "1"
    all = "all"


# HTTP status codes that will force request retries.
RETRY_STATUSES = [502, 503, 504]  # Bad Gateway, Service Unavailable, Gateway Timeout

# Default max number of times to retry a request.
MAX_RETRIES = 5

# Not that read_paginated requires the output of get_with_offset to be a Paginated type to work.
# The Paginated union type is generated based on response objects with a .pagination attribute.
T = TypeVar("T", bound=bindings.Paginated)

# Map of launch warnings to the warning message shown to users.
WARNING_MESSAGE_MAP = {
    bindings.v1LaunchWarning.CURRENT_SLOTS_EXCEEDED: (
        "Warning: The requested job requires more slots than currently available. "
        "You may need to increase cluster resources in order for the job to run."
    )
}


def read_paginated(
    get_with_offset: Callable[[int], T],
    offset: int = 0,
    pages: PageOpts = PageOpts.all,
) -> Iterator[T]:
    while True:
        resp = get_with_offset(offset)
        pagination = resp.pagination
        assert pagination is not None
        assert pagination.endIndex is not None
        assert pagination.total is not None
        yield resp
        if pagination.endIndex >= pagination.total or pages == PageOpts.single:
            break
        assert pagination.endIndex is not None
        offset = pagination.endIndex


def default_retry(max_retries: int = MAX_RETRIES) -> urllib3.util.retry.Retry:
    retry = urllib3.util.retry.Retry(
        total=max_retries,
        backoff_factor=0.5,  # {backoff factor} * (2 ** ({number of total retries} - 1))
        status_forcelist=RETRY_STATUSES,
    )
    return retry


# Literal["notebook", "tensorboard", "shell", "command"]
class NTSC_Kind(enum.Enum):
    notebook = "notebook"
    tensorboard = "tensorboard"
    shell = "shell"
    command = "command"


AnyNTSC = Union[bindings.v1Notebook, bindings.v1Tensorboard, bindings.v1Shell, bindings.v1Command]

all_ntsc: Set[NTSC_Kind] = {
    NTSC_Kind.notebook,
    NTSC_Kind.shell,
    NTSC_Kind.command,
    NTSC_Kind.tensorboard,
}
proxied_ntsc: Set[NTSC_Kind] = {NTSC_Kind.notebook, NTSC_Kind.tensorboard}


def get_ntsc_details(session: api.Session, typ: NTSC_Kind, ntsc_id: str) -> AnyNTSC:
    if typ == NTSC_Kind.notebook:
        return bindings.get_GetNotebook(session, notebookId=ntsc_id).notebook
    elif typ == NTSC_Kind.tensorboard:
        return bindings.get_GetTensorboard(session, tensorboardId=ntsc_id).tensorboard
    elif typ == NTSC_Kind.shell:
        return bindings.get_GetShell(session, shellId=ntsc_id).shell
    elif typ == NTSC_Kind.command:
        return bindings.get_GetCommand(session, commandId=ntsc_id).command
    else:
        raise ValueError("unknown type")


def wait_for_ntsc_state(
    session: api.Session,
    typ: NTSC_Kind,
    ntsc_id: str,
    predicate: Callable[[bindings.taskv1State], bool],
    timeout: int = 10,  # seconds
) -> bindings.taskv1State:
    """wait for ntsc to reach a state that satisfies the predicate"""

    def get_state() -> Tuple[bool, bindings.taskv1State]:
        last_state = get_ntsc_details(session, typ, ntsc_id).state
        return predicate(last_state), last_state

    return util.wait_for(get_state, timeout)


def task_is_ready(
    session: api.Session, task_id: str, progress_report: Optional[Callable] = None
) -> Optional[str]:
    """
    wait until a task is ready
    return: None if task is ready, otherwise return an error message
    """

    def _task_is_done_loading() -> Tuple[bool, Optional[str]]:
        task = bindings.get_GetTask(session, taskId=task_id).task
        if progress_report:
            progress_report()
        assert task is not None, "task must be present."
        if len(task.allocations) == 0:
            return False, None

        is_ready = task.allocations[0].isReady
        if is_ready:
            return True, None

        if task.endTime is not None:
            return True, "task has been terminated."

        return False, ""

    err_msg = util.wait_for(_task_is_done_loading, timeout=300, interval=1)
    return err_msg


def role_name_to_role_id(session: api.Session, role_name: str) -> int:
    # XXX: is 499 guaranteed to be enough?  Why not read_paginated?
    req = bindings.v1ListRolesRequest(limit=499, offset=0)
    resp = bindings.post_ListRoles(session=session, body=req)
    for r in resp.roles:
        if r.name == role_name and r.roleId is not None:
            return r.roleId
    raise api.errors.BadRequestException(f"could not find role name {role_name}")


def create_assignment_request(
    session: api.Session,
    role_name: str,
    workspace_name: Optional[str] = None,
    username_to_assign: Optional[str] = None,
    group_name_to_assign: Optional[str] = None,
) -> Tuple[List[bindings.v1UserRoleAssignment], List[bindings.v1GroupRoleAssignment]]:
    if (username_to_assign is None) == (group_name_to_assign is None):
        # XXX: very weird that we use api.errors.BadRequestException here!
        raise api.errors.BadRequestException(
            "must provide exactly one of --username-to-assign or --group-name-to-assign"
        )

    role = bindings.v1Role(roleId=role_name_to_role_id(session, role_name))

    workspace_id = None
    if workspace_name is not None:
        workspace_id = workspace_by_name(session, workspace_name).id
    role_assign = bindings.v1RoleAssignment(role=role, scopeWorkspaceId=workspace_id)

    if username_to_assign is not None:
        user_id = usernames_to_user_ids(session, [username_to_assign])[0]
        return [bindings.v1UserRoleAssignment(userId=user_id, roleAssignment=role_assign)], []

    assert group_name_to_assign is not None
    group_id = group_name_to_group_id(session, group_name_to_assign)
    return [], [bindings.v1GroupRoleAssignment(groupId=group_id, roleAssignment=role_assign)]


def assign_role(
    session: api.Session,
    role_name: str,
    workspace_name: Optional[str] = None,
    username_to_assign: Optional[str] = None,
    group_name_to_assign: Optional[str] = None,
) -> None:
    user_assign, group_assign = create_assignment_request(
        session=session,
        role_name=role_name,
        workspace_name=workspace_name,
        username_to_assign=username_to_assign,
        group_name_to_assign=group_name_to_assign,
    )
    req = bindings.v1AssignRolesRequest(
        userRoleAssignments=user_assign, groupRoleAssignments=group_assign
    )
    bindings.post_AssignRoles(session, body=req)

    scope = " globally"
    if workspace_name:
        scope = f" to workspace {workspace_name}"
    if len(user_assign) > 0:
        role_id = user_assign[0].roleAssignment.role.roleId
        print(
            f"assigned role '{role_name}' with ID {role_id} "
            + f"to user '{username_to_assign}' with ID {user_assign[0].userId}{scope}"
        )
    else:
        role_id = group_assign[0].roleAssignment.role.roleId
        print(
            f"assigned role '{role_name}' with ID {role_id} "
            + f"to group '{group_name_to_assign}' with ID {group_assign[0].groupId}{scope}"
        )


def usernames_to_user_ids(session: api.Session, usernames: List[str]) -> List[int]:
    usernames_to_ids: Dict[str, Optional[int]] = {u: None for u in usernames}
    users = bindings.get_GetUsers(session).users or []
    for user in users:
        if user.username in usernames_to_ids:
            usernames_to_ids[user.username] = user.id

    missing_users = []
    user_ids = []
    for username, user_id in usernames_to_ids.items():
        if user_id is None:
            missing_users.append(username)
        else:
            user_ids.append(user_id)

    if missing_users:
        raise api.errors.BadRequestException(
            f"could not find users for usernames {', '.join(missing_users)}"
        )
    return user_ids


def group_name_to_group_id(session: api.Session, group_name: str) -> int:
    body = bindings.v1GetGroupsRequest(name=group_name, limit=1, offset=0)
    resp = bindings.post_GetGroups(session, body=body)
    groups = resp.groups
    if groups is None or len(groups) != 1 or groups[0].group.groupId is None:
        raise api.errors.BadRequestException(f"could not find user group name {group_name}")
    return groups[0].group.groupId


def workspace_by_name(session: api.Session, name: str) -> bindings.v1Workspace:
    assert name, "workspace name cannot be empty"
    w = bindings.get_GetWorkspaces(session, nameCaseSensitive=name).workspaces
    assert len(w) <= 1, "workspace name is assumed to be unique."
    if len(w) == 0:
        raise not_found_errs("workspace", name, session)
    return bindings.get_GetWorkspace(session, id=w[0].id).workspace


# not_found_errs mirrors NotFoundErrs from the golang api/errors.go. In the cases where
# Python errors override the golang errors, this ensures the error messages stay consistent.
def not_found_errs(
    category: str, name: str, session: api.Session
) -> api.errors.BadRequestException:
    resp = bindings.get_GetMaster(session)
    msg = f"{category} '{name}' not found"
    if not resp.to_json().get("rbacEnabled"):
        return api.errors.NotFoundException(msg)
    return api.errors.NotFoundException(msg + ", please check your permissions.")
