"""JQ filters shared by the inbound webhook handler and the outbound deliver actions."""
from typing import Any, List, Tuple

import pyjq

from .errors import IntegrationConfigurationError


def jq_all(filter_expression: str, input_data: Any) -> List[Any]:
    """Every output of `filter_expression` run on `input_data`.

    pyjq raises ValueError for a filter that does not compile and
    pyjq.ScriptRuntimeError for one that fails on the data.
    """
    # The inbound handler always ran filters with line breaks removed, and saved
    # filters were written against that, so it stays for both directions.
    return pyjq.all(filter_expression.replace("\n", ""), input_data)


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
