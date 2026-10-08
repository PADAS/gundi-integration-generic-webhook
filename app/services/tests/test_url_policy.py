import asyncio

import pytest

from app.services import url_policy
from app.services.url_policy import URLResolutionError, validate_outbound_url


@pytest.mark.asyncio
@pytest.mark.parametrize("resolver_error", [OSError("Temporary failure in name resolution"), asyncio.TimeoutError()])
async def test_resolver_failures_are_a_distinct_still_valueerror_type(mocker, resolver_error):
    mocker.patch.object(url_policy, "_resolve_addresses", mocker.AsyncMock(side_effect=resolver_error))

    with pytest.raises(URLResolutionError) as exc_info:
        await validate_outbound_url("https://hooks.example.com/x")

    assert isinstance(exc_info.value, ValueError)  # existing callers catch ValueError


@pytest.mark.asyncio
async def test_policy_refusals_are_not_resolution_errors(mocker):
    mocker.patch.object(url_policy, "_resolve_addresses", mocker.AsyncMock(return_value=["10.0.0.1"]))

    with pytest.raises(ValueError) as exc_info:
        await validate_outbound_url("https://hooks.example.com/x")

    assert not isinstance(exc_info.value, URLResolutionError)
