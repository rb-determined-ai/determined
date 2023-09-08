import contextlib
import getpass
import hashlib
import json
import pathlib
from typing import Any, Dict, Iterator, List, Optional, Tuple, cast

import filelock

from determined.common import api, constants, util
from determined.common.api import bindings, certs

PASSWORD_SALT = "GubPEmmotfiK9TMD6Zdw"


def salt_and_hash(password: str) -> str:
    if password:
        return hashlib.sha512((PASSWORD_SALT + password).encode()).hexdigest()
    else:
        return password


class UsernameTokenPair:
    def __init__(self, username: str, token: str):
        self.username = username
        self.token = token


def login(
    master_address: str,
    username: str,
    password: str,
    cert: Optional[certs.Cert] = None,
) -> UsernameTokenPair:
    """
    Log in without considering or affecting the TokenStore on the file system.

    Used as part of login_with_cache, and also useful in tests where you wish to not affect the
    TokenStore.
    """
    password = api.salt_and_hash(password)
    unauth_session = api.UnauthSession(master=master_address, cert=cert)
    login = bindings.v1LoginRequest(username=username, password=password, isHashed=True)
    r = bindings.post_Login(session=unauth_session, body=login)
    return UsernameTokenPair(username, r.token)


def default_load_user_password(
    requested_user: Optional[str],
    password: Optional[str],
    token_store: "TokenStore",
) -> Tuple[Optional[str], Optional[str]]:
    # Always prefer an explicitly provided user/password.
    if requested_user:
        return requested_user, password

    # Next highest priority is user/password from environment.
    # Watch out! We have to check for DET_USER and DET_PASS, because containers will have DET_USER
    # set, but that doesn't overrule the active user in the TokenStore, because if the TokenStore in
    # the container has an active user, that means the user has explicitly ran `det user login`
    # inside the container.
    if (
        util.get_det_username_from_env() is not None
        and util.get_det_password_from_env() is not None
    ):
        return util.get_det_username_from_env(), util.get_det_password_from_env()

    # Last priority is the active user in the token store.
    return token_store.get_active_user(), password


def login_with_cache(
    master_address: Optional[str] = None,
    requested_user: Optional[str] = None,
    password: Optional[str] = None,
    cert: Optional[certs.Cert] = None,
) -> UsernameTokenPair:
    """
    Log in, preferring cached credentials in the TokenStore, if possible.

    This is the login path for nearly all user-facing cases.
    """

    master_address = master_address or util.get_default_master_address()
    token_store = TokenStore(master_address)

    # Get session_user and password given the following priority:
    # 1. User passed in with flag (requested_user)
    # 2. User from environment if DET_PASS is set.
    # 3. Active user from the token store.
    user, password = default_load_user_password(requested_user, password, token_store)

    # For login, we allow falling back to the default username.
    if not user:
        user = constants.DEFAULT_DETERMINED_USER
    assert user is not None

    # Check the token store if this session_user has a cached token. If so, check with the
    # master to verify it has not expired. Otherwise, let the token be None.
    token = token_store.get_token(user)
    if token is not None and not _is_token_valid(master_address, token, cert):
        token_store.drop_user(user)
        token = None

    if token is not None:
        return UsernameTokenPair(user, token)

    # Special case: use token provided from the container environment if:
    # - No token was obtained from the token store already,
    # - There is a token available from the container environment, and
    # - No user was explicitly requested, or the requested user matches the token available in the
    #   container environment.
    if (
        util.get_det_username_from_env() is not None
        and util.get_det_user_token_from_env() is not None
        and requested_user in (None, util.get_det_username_from_env())
    ):
        user = util.get_det_username_from_env()
        assert user
        token = util.get_det_user_token_from_env()
        assert token
        return UsernameTokenPair(user, token)

    # We'll need to create a new token, so we'll need a password. If there was no requested user and
    # we ended up falling back to the default username `determined`, then we can fall back to the
    # default login as well. Otherwise, ask the user for their password.
    fallback_to_default = password is None and user == constants.DEFAULT_DETERMINED_USER
    if fallback_to_default:
        password = constants.DEFAULT_DETERMINED_PASSWORD

    if password is None:
        password = getpass.getpass("Password for user '{}': ".format(user))

    try:
        utp = login(master_address, user, password, cert)
        user, token = utp.username, utp.token
    except api.errors.ForbiddenException:
        if fallback_to_default:
            raise api.errors.UnauthenticatedException()
        raise

    token_store.set_token(user, token)

    return UsernameTokenPair(user, token)


def logout(
    master_address: Optional[str],
    requested_user: Optional[str],
    cert: Optional[certs.Cert],
) -> None:
    """
    Logout if there is an active session for this master/username pair, otherwise do nothing.
    """

    master_address = master_address or util.get_default_master_address()
    token_store = TokenStore(master_address)

    user, _ = default_load_user_password(requested_user, None, token_store)
    # Don't log out of DEFAULT_DETERMINED_USER when it's not specified and not the active user.

    if user is None:
        return

    token = token_store.get_token(user)

    if token is None:
        return

    token_store.drop_user(user)

    utp = UsernameTokenPair(user, token)
    sess = api.Session(master=master_address, utp=utp, cert=cert)
    try:
        bindings.post_Logout(sess)
    except (api.errors.UnauthenticatedException, api.errors.APIException):
        # This session may have expired, but we don't care.
        pass


def logout_all(master_address: Optional[str], cert: Optional[certs.Cert]) -> None:
    master_address = master_address or util.get_default_master_address()
    token_store = TokenStore(master_address)

    users = token_store.get_all_users()

    for user in users:
        logout(master_address, user, cert)


def _is_token_valid(master_address: str, token: str, cert: Optional[certs.Cert]) -> bool:
    """
    Find out whether the given token is valid by attempting to use it
    on the "api/v1/me" endpoint.
    """
    utp = UsernameTokenPair("username-doesnt-matter", token)
    sess = api.Session(master_address, utp, cert)
    try:
        r = sess.get("api/v1/me")
    except (api.errors.UnauthenticatedException, api.errors.APIException):
        return False

    return r.status_code == 200


class TokenStore:
    """
    TokenStore is a class for reading/updating a persistent store of user authentication tokens.
    TokenStore can remember tokens for many users for each of many masters.

    All updates to the file follow a read-modify-write pattern, and use file locks to protect the
    integrity of the underlying file cache.
    """

    def __init__(self, master_address: str, path: Optional[pathlib.Path] = None) -> None:
        self.master_address = master_address
        self.path = path or util.get_config_path().joinpath("auth.json")
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        # Decide on paths for a lock file and a temp files (during writing)
        self.temp = pathlib.Path(str(self.path) + ".temp")
        self.lock = str(self.path) + ".lock"

        with filelock.FileLock(self.lock):
            store = self._load_store_file()

        self._reconfigure_from_store(store)

    def _reconfigure_from_store(self, store: dict) -> None:
        substore = store.get("masters", {}).get(self.master_address, {})
        self._active_user = cast(str, substore.get("active_user"))
        self._tokens = cast(Dict[str, str], substore.get("tokens", {}))

    def get_active_user(self) -> Optional[str]:
        return self._active_user

    def get_all_users(self) -> List[str]:
        return list(self._tokens)

    def get_token(self, user: str) -> Optional[str]:
        token = self._tokens.get(user)
        if token is not None:
            assert isinstance(token, str), "invalid cache; token must be a string"
        return token

    def delete_token_cache(self) -> None:
        with filelock.FileLock(self.lock):
            if self.path.exists():
                self.path.unlink()

    def drop_user(self, username: str) -> None:
        with self._persistent_store() as substore:
            tokens = substore.setdefault("tokens", {})
            if username in tokens:
                del tokens[username]
            if substore.get("active_user") == username:
                del substore["active_user"]

    def set_token(self, username: str, token: str) -> None:
        with self._persistent_store() as substore:
            tokens = substore.setdefault("tokens", {})
            tokens[username] = token

    def set_active(self, username: str) -> None:
        with self._persistent_store() as substore:
            tokens = substore.setdefault("tokens", {})
            if username not in tokens:
                raise api.errors.UnauthenticatedException()
            substore["active_user"] = username

    @contextlib.contextmanager
    def _persistent_store(self) -> Iterator[Dict[str, Any]]:
        """
        Yields the appropriate store[self.master_address] that can be modified, and the modified
        result will be written back to file.

        Whatever updates are made will also be updated on self automatically.
        """
        with filelock.FileLock(self.lock):
            store = self._load_store_file()
            substore = store.setdefault("masters", {}).setdefault(self.master_address, {})

            # No need for try/finally, because we don't update the file after failures.
            yield substore

            # Reconfigure our cached variables.
            self._reconfigure_from_store(store)

            with self.temp.open("w") as f:
                json.dump(store, f, indent=4, sort_keys=True)
            self.temp.replace(self.path)

    def _load_store_file(self) -> Dict[str, Any]:
        """
        Read a token store from a file, shimming it to the most recent version if necessary.

        If a v0 store is found it will be reconfigured as a v1 store based on the master_address
        that is being currently requested.
        """
        try:
            if not self.path.exists():
                return {"version": 1}

            try:
                with self.path.open() as f:
                    store = json.load(f)
            except json.JSONDecodeError:
                raise api.errors.CorruptTokenCacheException()

            if not isinstance(store, dict):
                raise api.errors.CorruptTokenCacheException()

            version = store.get("version", 0)
            if version == 0:
                validate_token_store_v0(store)
                store = shim_store_v0(store, self.master_address)

            validate_token_store_v1(store)

            return cast(dict, store)

        except api.errors.CorruptTokenCacheException:
            # Delete invalid caches before exiting.
            self.path.unlink()
            raise


def shim_store_v0(v0: Dict[str, Any], master_address: str) -> Dict[str, Any]:
    """
    v1 schema is just a bit more nesting to support multiple masters.
    """
    v1 = {"version": 1, "masters": {master_address: v0}}
    return v1


def validate_token_store_v0(store: Any) -> bool:
    """
    Valid v0 schema example:

        {
          "active_user": "user_a",
          "tokens": {
            "user_a": "TOKEN",
            "user_b": "TOKEN"
          }
        }
    """

    if not isinstance(store, dict):
        raise api.errors.CorruptTokenCacheException()

    if len(set(store.keys()).difference({"active_user", "tokens"})) > 0:
        # Extra keys.
        raise api.errors.CorruptTokenCacheException()

    if "active_user" in store:
        if not isinstance(store["active_user"], str):
            raise api.errors.CorruptTokenCacheException()

    if "tokens" in store:
        tokens = store["tokens"]
        if not isinstance(tokens, dict):
            raise api.errors.CorruptTokenCacheException()
        for k, v in tokens.items():
            if not isinstance(k, str):
                raise api.errors.CorruptTokenCacheException()
            if not isinstance(v, str):
                raise api.errors.CorruptTokenCacheException()
    return True


def validate_token_store_v1(store: Any) -> bool:
    """
    Valid v1 schema example:

        {
          "version": 1,
          "masters": {
            "master_url_a": {
              "active_user": "user_a",
              "tokens": {
                "user_a": "TOKEN",
                "user_b": "TOKEN"
              }
            },
            "master_url_b": {
              "active_user": "user_c",
              "tokens": {
                "user_c": "TOKEN",
                "user_d": "TOKEN"
              }
            }
          }
        }

    Note that store["masters"] is a mapping of string url's to valid v0 schemas.
    """
    if not isinstance(store, dict):
        raise api.errors.CorruptTokenCacheException()

    if len(set(store.keys()).difference({"version", "masters"})) > 0:
        # Extra keys.
        raise api.errors.CorruptTokenCacheException()

    # Handle version.
    version = store.get("version")
    if version != 1:
        raise api.errors.CorruptTokenCacheException()

    if "masters" in store:
        masters = store["masters"]
        if not isinstance(masters, dict):
            raise api.errors.CorruptTokenCacheException()

        # Each entry of masters must be a master_url/substore pair.
        for key, val in masters.items():
            if not isinstance(key, str):
                raise api.errors.CorruptTokenCacheException()
            validate_token_store_v0(val)

    return True
