"""JQ filters shared by the inbound webhook handler and the outbound deliver actions."""
import json
from typing import Any, List, Tuple

import pydantic
import pyjq

from .errors import IntegrationConfigurationError


def jq_input(payload: pydantic.BaseModel) -> dict:
    """A Gundi payload as an outbound jq filter receives it."""
    # Through .json() so datetimes and UUIDs become JSON values jq can read.
    return json.loads(payload.json())


def jq_all(filter_expression: str, input_data: Any) -> List[Any]:
    """Every output of `filter_expression` run on `input_data`.

    pyjq raises ValueError for a filter that does not compile and
    pyjq.ScriptRuntimeError for one that fails on the data.
    """
    # jq rejects \r; deleting line breaks instead would glue tokens (`.a⏎and` reads field `aand`).
    return pyjq.all(filter_expression.replace("\r\n", "\n").replace("\r", "\n"), input_data)


def outbound_body(filter_expression: str, input_data: Any) -> Tuple[bool, Any]:
    """(send, body) for one outbound request.

    No output means there is nothing to send; one output is the body; several
    are sent together as a JSON array. null outputs are dropped first, so a
    filter can skip a request by yielding null. A filter that fails to compile
    or run is the integration's configuration, so it raises
    IntegrationConfigurationError.
    """
    try:
        results = jq_all(filter_expression, input_data)
    except (ValueError, pyjq.ScriptRuntimeError) as e:
        raise IntegrationConfigurationError(f"The JQ filter failed: {e}") from e
    # httpx sends json=None as an empty body, which no endpoint expects.
    results = [result for result in results if result is not None]
    if not results:
        return False, None
    return True, results[0] if len(results) == 1 else results
