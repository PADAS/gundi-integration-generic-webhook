import pytest

from app.services.errors import IntegrationConfigurationError
from app.services.jq_transform import jq_all, outbound_body


def test_no_output_means_nothing_to_send():
    assert outbound_body("select(.keep)", {"keep": False}) == (False, None)
    assert outbound_body(".[] | select(.keep)", [{"keep": False}]) == (False, None)


def test_one_output_is_the_body():
    assert outbound_body("{id: .gundi_id}", {"gundi_id": "x"}) == (True, {"id": "x"})


def test_null_outputs_are_skipped():
    # httpx would send json=None as an empty body.
    assert outbound_body(".missing", {}) == (False, None)
    assert outbound_body(".[] | .id", [{"id": 1}, {}]) == (True, 1)


def test_several_outputs_are_sent_as_an_array():
    assert outbound_body(".[] | .id", [{"id": 1}, {"id": 2}]) == (True, [1, 2])


def test_identity_on_a_batch_sends_the_batch():
    assert outbound_body(".", [{"id": 1}, {"id": 2}]) == (True, [{"id": 1}, {"id": 2}])


@pytest.mark.parametrize("bad_filter", ["{", ".a | error(\"boom\")"])
def test_a_failing_filter_is_a_configuration_error(bad_filter):
    with pytest.raises(IntegrationConfigurationError):
        outbound_body(bad_filter, {"a": 1})


def test_line_breaks_are_removed_as_the_inbound_handler_always_did():
    assert jq_all("{\nid: .id\n}", {"id": 3}) == [{"id": 3}]
