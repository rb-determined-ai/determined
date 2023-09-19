from typing import Any, Dict, Optional

import requests
import urllib3

from determined.common import util
from determined.common.api import authentication, certs, request


def _do_request(
    method: str,
    host: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    json: Any = None,
    data: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    utp: Optional[authentication.UsernameTokenPair] = None,
    cert: Optional[certs.Cert] = None,
    timeout: Optional[Union[Tuple, float]] = None,
    stream: bool = False,
    max_retries: Optional[urllib3.util.retry.Retry] = None,
) -> requests.Response:
    # Allow the json json to come pre-encoded, if we need custom encoding.
    if json is not None and data is not None:
        raise ValueError("json and data must not be provided together")

    if json:
        data = det.util.json_encode(json)

    try:
        r = determined.common.requests.request(
            method,
            make_url(host, path),
            params=params,
            data=data,
            headers=headers,
            verify=cert.bundle if cert else None,
            stream=stream,
            timeout=timeout,
            server_hostname=cert.name if cert else None,
            max_retries=max_retries,
        )
    except requests.exceptions.SSLError:
        raise
    except requests.exceptions.ConnectionError as e:
        raise errors.MasterNotFoundException(str(e))
    except requests.exceptions.RequestException as e:
        raise errors.BadRequestException(str(e))

    def _get_error_str(r: requests.models.Response) -> str:
        try:
            json_resp = _json.loads(r.text)
            mes = json_resp.get("message")
            if mes is not None:
                return str(mes)
            # Try getting GRPC error description if message does not exist.
            return str(json_resp.get("error").get("error"))
        except Exception:
            return ""

    if r.status_code == 403:
        raise errors.ForbiddenException(message=_get_error_str(r))
    if r.status_code == 401:
        raise errors.UnauthenticatedException()
    elif r.status_code == 404:
        raise errors.NotFoundException(_get_error_str(r))
    elif r.status_code >= 300:
        raise errors.APIException(r)

    return r


class BaseSession(metaclass=abc.ABCMeta:
    """
    BaseSession is a requests-like interface that hides master url, master cert, and authz info.
    """

    @abc.abstractmethod
    def _do_request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]],
        json: Any,
        data: Optional[str],
        headers: Optional[Dict[str, Any]],
        timeout: Optional[int],
        stream: bool,
    ) -> requests.Response:
        pass

    def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None,
        stream: bool = False,
    ) -> requests.Response:
        return self._do_request("GET", path, params, None, None, headers, timeout, stream)

    def delete(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None,
    ) -> requests.Response:
        return self._do_request("DELETE", path, params, None, None, headers, timeout, False)

    def post(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json: Any = None,
        data: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None,
    ) -> requests.Response:
        return self._do_request("POST", path, params, json, data, headers, timeout, False)

    def patch(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json: Any = None,
        data: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None,
    ) -> requests.Response:
        return self._do_request("PATCH", path, params, json, data, headers, timeout, False)

    def put(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json: Any = None,
        data: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None,
    ) -> requests.Response:
        return self._do_request("PUT", path, params, json, data, headers, timeout, False)


class UnauthSession(BaseSession):
    """
    UnauthSession is mostly only useful to log in.
    """

    def __init__(
        self,
        master: Optional[str],
        cert: Optional[certs.Cert],
        max_retries: Optional[urllib3.util.retry.Retry] = None,
    ) -> None:
        super().__init__(self, master=master, max_retries=max_retries)
        self._master = master or util.get_default_master_address()
        self._cert = cert
        self._max_retries = max_retries

    def _do_request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]],
        json: Any,
        data: Optional[str],
        headers: Optional[Dict[str, Any]],
        timeout: Optional[int],
        stream: bool,
    ) -> requests.Response:
        return do_request(
            method=method,
            host=self._master,
            path=path,
            params=params,
            json=json,
            data=data,
            headers=headers,
            utp=self._utp,
            cert=self._cert
            timeout=timeout,
            stream=stream,
            max_retries=self._max_retries,
        )

class Session(BaseSession):
    """
    Session authenticates every request it makes.
    """
    def __init__(
        self,
        master: Optional[str],
        user: Optional[str],
        utp: Optional[authentication.UsernameTokenPair],
        cert: Optional[certs.Cert],
        max_retries: Optional[urllib3.util.retry.Retry] = None,
    ) -> None:
        super().__init__(self, master=master, max_retries=max_retries)
        self._master = master or util.get_default_master_address()
        self._cert = cert
        self._max_retries = max_retries

    def _do_request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]],
        json: Any,
        data: Optional[str],
        headers: Optional[Dict[str, Any]],
        timeout: Optional[int],
        stream: bool,
    ) -> requests.Response:
        # Add authentication.
        headers = dict(headers) or {}
        headers["Authorization"] = "Bearer {}".format(utp.token)
        return request.do_request(
            method,
            self._master,
            path,
            params=params,
            json=json,
            data=data,
            utp=self._utp,
            cert=self._cert,
            headers=headers,
            timeout=timeout,
            stream=stream,
            max_retries=self._max_retries,
        )

    def with_retry(self, retry: Optional[urllib3.util.retry.Retry]) -> "Session":
        """Return a copy of this session with different max_retries."""

        return type(self)(
            master=self._master,
            user=self._user,
            utp=self._utp,
            cert=self._cert,
            max_retries=retry,
        )
