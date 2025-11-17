"""DSL functions for storage operations.

This module is deprecated. Use fluent API instead:
- storage.create() instead of createo()
- storage.read() instead of reado()
- storage.update() instead of updateo()
- storage.patch() instead of patcho()
- storage.delete() instead of deleteo()

Python operations (filtero, mapo, reduceo, transformo) are available
from haolib.pipelines.

This file is kept for backward compatibility but should not be used in new code.
"""

# Re-export Python operations from pipelines for backward compatibility
from haolib.pipelines import filtero, mapo, reduceo, transformo

__all__ = [
    "filtero",
    "mapo",
    "reduceo",
    "transformo",
]
