"""What the portal's jq transformation editor needs to know about each data type.

DeliverConfig.ui_schema() attaches this to the endpoint's jq_filter as
`gundi:jq_transform`: the JSON schema of every output type and an example
record shaped exactly like the deliver actions' jq input.
"""
from typing import Dict, Type

import pydantic
from gundi_core.schemas import v2

from app.services.jq_transform import jq_input

# Keyed by OutputType value; test_configurations checks it covers every type.
MODELS: Dict[str, Type[pydantic.BaseModel]] = {
    "observation": v2.Observation,
    "event": v2.Event,
    "event_update": v2.EventUpdate,
    "message": v2.TextMessage,
}

_PROVIDER_ID = "ddd0946d-15b0-4308-b93d-e0470b6d33b6"
_LOCATION = {"lat": -1.59083, "lon": 35.43902}


def _example(model: Type[pydantic.BaseModel], field: str):
    return model.__fields__[field].field_info.extra["example"]


def _example_models() -> Dict[str, pydantic.BaseModel]:
    observation = v2.Observation.Config.schema_extra["example"]
    return {
        "observation": v2.Observation.parse_obj({
            **observation,
            "gundi_id": "5b6b3e7a-3b1c-4b7e-9b8e-2f0c6a1d4e01",
            "data_provider_id": _PROVIDER_ID,
            "subject_type": _example(v2.Observation, "subject_type"),
        }),
        "event": v2.Event(
            gundi_id="8a1f2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c02",
            data_provider_id=_PROVIDER_ID,
            source_id="bc14b256-dec0-4363-831d-39d0d2d85d50",
            external_source_id=_example(v2.Event, "external_source_id"),
            recorded_at=_example(v2.Event, "recorded_at"),
            location=_LOCATION,
            title="Animal Sighting",
            event_type="wildlife_sighting_rep",
            event_details={"species": "Elephant", "herd_size": 12},
            status="new",
        ),
        "event_update": v2.EventUpdate(
            gundi_id="8a1f2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c02",
            data_provider_id=_PROVIDER_ID,
            source_id="bc14b256-dec0-4363-831d-39d0d2d85d50",
            external_source_id=_example(v2.EventUpdate, "external_source_id"),
            changes={"status": "resolved", "event_details": {"herd_size": 14}},
        ),
        "message": v2.TextMessage(
            gundi_id="2c4e6a8b-0d1f-4a3b-8c5d-7e9f1a3b5c03",
            data_provider_id=_PROVIDER_ID,
            external_source_id=_example(v2.TextMessage, "external_source_id"),
            sender="300434063929240",
            recipients=["admin@example.org"],
            text="Requesting backup at the north gate.",
            created_at=_example(v2.TextMessage, "created_at"),
            location=_LOCATION,
            additional={"status": "sent", "device": "inReach Mini 2"},
        ),
    }


def jq_transform_annotation(*, output_type_field: str, batch_field: str, samples_action: str) -> dict:
    return {
        "output_type_field": output_type_field,
        "batch_field": batch_field,
        "samples_action": samples_action,
        "schemas": {name: model.schema() for name, model in MODELS.items()},
        "examples": {name: jq_input(model) for name, model in _example_models().items()},
    }
