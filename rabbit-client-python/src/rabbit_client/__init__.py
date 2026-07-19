"""Public package surface for rabbit-client.

The implementation lives in ``rabbit_client.client``; see ``docs/api.md``
for the full API reference.
"""

from rabbit_client.client import Consumer, ConsumerCancelledError, RabbitClient

__all__ = ["Consumer", "ConsumerCancelledError", "RabbitClient"]
