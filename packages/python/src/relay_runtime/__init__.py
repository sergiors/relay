"""Official public interfaces for developing Python handlers on Relay.

The initial release publishes the ``HandlerContext`` Protocol. Relay's Python
bootstrap constructs the actual per-invocation context object.
"""

from relay_runtime.typing import HandlerContext

__all__ = ["HandlerContext"]
__version__ = "0.1.0"
