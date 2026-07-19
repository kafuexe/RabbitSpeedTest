"""Public package surface for hs-rabbit-client.

The implementation lives in ``hs_rabbit_client.client``; see ``docs/api.md``
for the full API reference.
"""

from hs_rabbit_client.client import Consumer, ConsumerCancelledError, RabbitClient

__all__ = ["Consumer", "ConsumerCancelledError", "RabbitClient"]
