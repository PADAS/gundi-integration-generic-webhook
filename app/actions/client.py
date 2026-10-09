"""HTTP client for user-configured webhook endpoints.

Knows nothing about Gundi: it sends one JSON body and turns the endpoint's
answer into an exception whose `retryable` says whether trying again could
help. Messages name hosts only: endpoint paths and queries often embed
secrets (Slack-style webhook URLs, ?token=...).
"""
import email.utils
import time
from typing import Any, Mapping, Optional
from urllib.parse import urlsplit

import httpx

# Answers that may differ on the next attempt besides 429 and 5xx: the
# server timed out waiting for us (408), or will not risk a replay yet (425).
_TRANSIENT_STATUSES = {408, 425}


class EndpointError(Exception):
    retryable = False

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class EndpointConnectionError(EndpointError):
    retryable = True


class EndpointRateLimitError(EndpointError):
    retryable = True

    def __init__(self, message: str, status_code: Optional[int] = None, retry_after: Optional[float] = None):
        super().__init__(message, status_code)
        self.retry_after = retry_after


class EndpointServerError(EndpointError):
    """5xx, 408 or 425: the endpoint may accept the same request later."""
    retryable = True


class EndpointAuthError(EndpointError):
    pass


class EndpointRejectedError(EndpointError):
    pass


class EndpointRequestError(EndpointError):
    """The request could not be built from the given URL or headers (e.g. an
    out-of-range port, a CR/LF or a non-ASCII character in a header value).
    Sending it again fails the same way."""


def _host(url: str) -> str:
    try:
        return urlsplit(url).hostname or "endpoint"
    except ValueError:
        return "endpoint"


def _retry_after_seconds(value: Optional[str]) -> Optional[float]:
    """Retry-After as seconds from now: delta-seconds or an HTTP-date."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        moment = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, moment.timestamp() - time.time())


async def send_json(
        url: str,
        method: str,
        headers: Mapping[str, str],
        body: Any,
        timeout: float,
        client: Optional[httpx.AsyncClient] = None,
) -> httpx.Response:
    """Send `body` as JSON and return the 2xx response, or raise an EndpointError."""
    host = _host(url)
    # InvalidURL is not a TransportError and would escape unclassified. Messages
    # name the host only: the exception text quotes the URL.
    try:
        port = httpx.URL(url).port
    except httpx.InvalidURL as e:
        raise EndpointRequestError(f"Could not build the request to {host}: the URL is invalid") from e
    # httpx parses any number as a port and only fails at connect time, which
    # would read as a retryable connection error.
    if port is not None and not 0 < port < 65536:
        raise EndpointRequestError(f"Could not build the request to {host}: the port is out of range")
    owns_client = client is None
    # Redirects are not followed: the URL was vetted against private addresses
    # before this call, and a redirect target would not have been.
    client = client or httpx.AsyncClient(timeout=timeout, follow_redirects=False)
    try:
        response = await client.request(method, url, headers=dict(headers), json=body, timeout=timeout)
    # Before TransportError, which it subclasses: a header value h11 refuses is
    # our request's fault, not the network's.
    except httpx.LocalProtocolError as e:
        raise EndpointRequestError(f"Could not build the request to {host}: invalid header") from e
    except UnicodeEncodeError as e:
        raise EndpointRequestError(f"Could not build the request to {host}: a header value is not ASCII") from e
    # A TransportError subclass, but no retry can change the URL's scheme.
    except httpx.UnsupportedProtocol as e:
        raise EndpointRequestError(f"Could not build the request to {host}: unsupported URL scheme") from e
    except httpx.TransportError as e:
        raise EndpointConnectionError(f"Could not reach {host}: {type(e).__name__}") from e
    finally:
        if owns_client:
            await client.aclose()

    status = response.status_code
    if 200 <= status < 300:
        return response
    message = f"{method} to {host} answered HTTP {status}"
    if 300 <= status < 400:
        try:
            location_host = urlsplit(response.headers.get("Location", "")).hostname
        except ValueError:
            location_host = None
        target = f" to {location_host}" if location_host else ""
        raise EndpointRejectedError(f"{message} (redirect{target}; redirects are not followed, update the URL)", status)
    if status in (401, 403):
        raise EndpointAuthError(message, status)
    if status == 429:
        raise EndpointRateLimitError(message, status, _retry_after_seconds(response.headers.get("Retry-After")))
    if status >= 500 or status in _TRANSIENT_STATUSES:
        raise EndpointServerError(message, status)
    raise EndpointRejectedError(message, status)
