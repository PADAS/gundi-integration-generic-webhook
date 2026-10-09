import pyjq
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


# The last one compiled when line breaks were deleted (`.a.b`); a break right
# after the `.` of a path is now a syntax error.
@pytest.mark.parametrize("bad_filter", ["{", ".a | error(\"boom\")", ".a.\nb"])
def test_a_failing_filter_is_a_configuration_error(bad_filter):
    with pytest.raises(IntegrationConfigurationError):
        outbound_body(bad_filter, {"a": 1})


RECORD = {"a": 1, "b": 2, "x": None, "id": 3, "name": "Collar 7"}

MULTI_LINE = [
    ("if .a == 1 then .b\nelse .a\nend", [2]),
    ("if .a == 1 then .b\r\nelse .a\r\nend", [2]),
    ("if .a == 1 then .b\relse .a\rend", [2]),
    ("[select(.x != null\nor .b == 2) | .id]", [[3]]),
    (".a == 1\nand .b == 2", [True]),
    (".a == 1\r\nand .b == 3", [False]),
    ("{\n  id: .id, # the Gundi id\n  name: .name\n}", [{"id": 3, "name": "Collar 7"}]),
    ('"#\\(.a)#" # a comment after a string holding #', ["#1#"]),
    ('"\\("x#y" | ascii_upcase)"', ["X#Y"]),
    # Failed before too: deleting \n left the \r, which jq rejects.
    ("{\r\n  id: .id,\r\n  name: .name\r\n}", [{"id": 3, "name": "Collar 7"}]),
]

# Filters that compiled when line breaks were deleted: same output now.
UNCHANGED = [
    ("{\nid: .id\n}", [{"id": 3}]),
    (".a\n| . + 1", [2]),
    ("[.a,\n.b]", [[1, 2]]),
    (".name\n| ascii_downcase", ["collar 7"]),
    ("select(.a == 1\nand .b == 2) | .id", [3]),
]


@pytest.mark.parametrize("jq_filter, expected", MULTI_LINE + UNCHANGED)
def test_line_breaks_are_whitespace(jq_filter, expected):
    assert jq_all(jq_filter, RECORD) == expected


@pytest.mark.parametrize("jq_filter, expected", MULTI_LINE + UNCHANGED)
def test_outbound_bodies_use_the_same_rules(jq_filter, expected):
    assert outbound_body(jq_filter, RECORD) == (True, expected[0])


@pytest.mark.parametrize("jq_filter, before, now", [
    # Deleting the break read the field `aand`.
    (".a\nand .b", [None], [True]),
    # A comment used to swallow every later line.
    (".a # one more\n| . + 1", [1], [2]),
    # A raw line break inside a string literal is kept rather than dropped.
    ('"a\nb"', ["ab"], ["a\nb"]),
])
def test_filters_that_compiled_with_the_wrong_meaning_now_do_what_they_say(jq_filter, before, now):
    assert pyjq.all(jq_filter.replace("\n", ""), RECORD) == before
    assert jq_all(jq_filter, RECORD) == now
