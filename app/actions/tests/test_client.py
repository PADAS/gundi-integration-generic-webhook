import asyncio
import email.utils
import json
import time

import httpx
import pytest
import pytest_asyncio

from app.actions import client

URL = "https://hooks.example.com/services/T000/B000/secret-path?token=abc"


def _client(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
async def test_sends_json_with_the_given_method_and_headers():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(202)

    async with _client(handler) as http:
        response = await client.send_json(
            URL, "PUT", {"Authorization": "Bearer k"}, [{"a": 1}], timeout=5, client=http,
        )

    assert response.status_code == 202
    assert requests[0].method == "PUT"
    assert requests[0].headers["Authorization"] == "Bearer k"
    assert json.loads(requests[0].content) == [{"a": 1}]


@pytest.mark.asyncio
@pytest.mark.parametrize("status, error, retryable", [
    (401, client.EndpointAuthError, False),
    (403, client.EndpointAuthError, False),
    (429, client.EndpointRateLimitError, True),
    (500, client.EndpointServerError, True),
    (503, client.EndpointServerError, True),
    (400, client.EndpointRejectedError, False),
    (404, client.EndpointRejectedError, False),
    (408, client.EndpointServerError, True),
    (425, client.EndpointServerError, True),
    (302, client.EndpointRejectedError, False),  # redirects are not followed
])
async def test_non_2xx_answers_are_classified(status, error, retryable):
    async with _client(lambda request: httpx.Response(status)) as http:
        with pytest.raises(error) as exc_info:
            await client.send_json(URL, "POST", {}, {}, timeout=5, client=http)

    assert exc_info.value.status_code == status
    assert exc_info.value.retryable is retryable
    # Paths and queries of webhook URLs often embed secrets; only the host is named.
    assert "hooks.example.com" in str(exc_info.value)
    assert "secret-path" not in str(exc_info.value) and "token" not in str(exc_info.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("transport_error", [httpx.ConnectError("refused"), httpx.ReadTimeout("slow")])
async def test_transport_failures_are_retryable_connection_errors(transport_error):
    def handler(request):
        raise transport_error

    async with _client(handler) as http:
        with pytest.raises(client.EndpointConnectionError) as exc_info:
            await client.send_json(URL, "POST", {}, {}, timeout=5, client=http)

    assert exc_info.value.retryable
    assert exc_info.value.status_code is None


@pytest.mark.asyncio
async def test_a_redirect_names_its_target_host_so_the_user_can_fix_the_url():
    location = "https://new.example.org/hooks/secret-path?token=abc"
    async with _client(lambda request: httpx.Response(301, headers={"Location": location})) as http:
        with pytest.raises(client.EndpointRejectedError) as exc_info:
            await client.send_json(URL, "POST", {}, {}, timeout=5, client=http)

    message = str(exc_info.value)
    assert "redirect to new.example.org" in message
    assert "secret-path" not in message and "token" not in message


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_after, expected", [("120", 120.0), (None, None), ("soon", None)])
async def test_rate_limit_carries_retry_after(retry_after, expected):
    headers = {"Retry-After": retry_after} if retry_after else {}
    async with _client(lambda request: httpx.Response(429, headers=headers)) as http:
        with pytest.raises(client.EndpointRateLimitError) as exc_info:
            await client.send_json(URL, "POST", {}, {}, timeout=5, client=http)

    assert exc_info.value.retry_after == expected


def test_retry_after_as_an_http_date():
    when = email.utils.formatdate(time.time() + 300, usegmt=True)
    assert 290 <= client._retry_after_seconds(when) <= 300


@pytest.mark.asyncio
async def test_a_header_value_h11_refuses_is_a_permanent_request_error():
    def handler(request):
        # What httpx's real transport (h11) raises for a CR/LF in a header value.
        raise httpx.LocalProtocolError("Illegal header value")

    async with _client(handler) as http:
        with pytest.raises(client.EndpointRequestError) as exc_info:
            await client.send_json(URL, "POST", {"X-Tenant": "a\r\nX-Injected: 1"}, {}, timeout=5, client=http)

    assert not exc_info.value.retryable


@pytest.mark.asyncio
async def test_a_non_ascii_header_value_is_a_permanent_request_error():
    sent = []

    async with _client(lambda request: sent.append(request) or httpx.Response(200)) as http:
        with pytest.raises(client.EndpointRequestError) as exc_info:
            await client.send_json(URL, "POST", {"X-Tenant": "caf\u00e9\u2713"}, {}, timeout=5, client=http)

    assert not exc_info.value.retryable
    assert sent == []


@pytest_asyncio.fixture
async def local_server():
    """A plain HTTP server on localhost, so requests go through httpx's real transport (h11)."""
    requests = []

    async def handle(reader, writer):
        requests.append(await reader.readuntil(b"\r\n\r\n"))
        writer.write(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    yield f"http://127.0.0.1:{port}/hook", requests
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_the_real_transport_sends_ascii_headers(local_server):
    url, requests = local_server

    response = await client.send_json(url, "POST", {"X-Tenant": "acme"}, {"a": 1}, timeout=5)

    assert response.status_code == 200
    assert b"x-tenant: acme" in requests[0].lower()


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["caf\u00e9", "a\r\nX-Injected: 1"])
async def test_the_real_transport_refuses_headers_the_config_now_rejects(local_server, value):
    url, requests = local_server

    with pytest.raises(client.EndpointRequestError) as exc_info:
        await client.send_json(url, "POST", {"X-Tenant": value}, {}, timeout=5)

    assert not exc_info.value.retryable
    assert requests == []


@pytest.mark.asyncio
@pytest.mark.parametrize("url", [
    "https://hooks.example.com:99999/secret-path?token=abc",  # port out of range (httpx parses it)
    "https://hooks.example.com:-1/secret-path?token=abc",
    "https://hooks.example.com:abc/secret-path?token=abc",  # non-numeric port
    "ftp://hooks.example.com/secret-path?token=abc",  # unsupported scheme (a TransportError subclass)
])
async def test_urls_httpx_cannot_use_are_a_permanent_request_error(url):
    # Real httpx parsing: these fail before any connection is attempted.
    with pytest.raises(client.EndpointRequestError) as exc_info:
        await client.send_json(url, "POST", {}, {}, timeout=5)

    assert not exc_info.value.retryable
    assert "hooks.example.com" in str(exc_info.value)
    assert "secret-path" not in str(exc_info.value) and "token" not in str(exc_info.value)
