from typing import Optional

from app.services.utils import FieldWithUIOptions, UIOptions
from app.webhooks.core import GenericJsonTransformConfig


class GenericWebhookTransformConfig(GenericJsonTransformConfig):
    """Webhook configuration for this integration.

    The template's GenericJsonTransformConfig describes output_type as applying
    to every record. This integration's handler additionally lets each record
    emitted by the JQ filter choose its own type via a '__gundi_output_type'
    field (stripped before sending), so the portal help text is overridden
    here, in the fork's extension point, rather than in the template model.
    """
    output_type: Optional[str] = FieldWithUIOptions(
        None,
        description=(
            "Default output type for all transformed records: 'obv' (observations) or 'ev' (events). "
            "Individual records can override this with a '__gundi_output_type' field."
        ),
        ui_options=UIOptions(
            widget="text",
        )
    )
