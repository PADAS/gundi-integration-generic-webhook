from typing import List, Optional, Union
from uuid import UUID

from pydantic import Field
from gundi_core.events import ProviderInfo, SystemEventBaseModel
from gundi_core.events.delivery import GundiPayload
from gundi_core.schemas.v2 import RouteConfiguration


class GundiBatchDelivery(SystemEventBaseModel):
    """A bundle of generic payloads from one provider, delivered to this destination.

    The batch counterpart of gundi_core's GundiDelivery, with the batch
    semantics of its ObservationsBatch envelopes: `batch_id` identifies the
    bundle across Pub/Sub redeliveries (so the publisher must set it), and
    stages may split or shrink a bundle but never merge two. The runner routes it to action_deliver_batch
    by `event_type` (the class name), so the name is part of the contract.

    Lives here until it moves to gundi-core and cdip-routing publishes it
    (see docs/outbound-webhooks.md).
    """

    # Required, unlike ObservationsBatch's: redelivery dedup keys on it, and a
    # default would mint a new id on every redelivered parse.
    batch_id: Union[UUID, str]
    provider: ProviderInfo
    route_configuration: Optional[RouteConfiguration] = None
    payloads: List[GundiPayload] = Field(default_factory=list)

    class Config:
        # As GundiDelivery: pick the payload class by its const observation_type
        # instead of the first union member that happens to validate.
        smart_union = True
