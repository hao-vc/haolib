"""DSL for storage operations.

This module is deprecated. Use fluent API instead:
- storage.create() instead of createo()
- storage.read() instead of reado()
- storage.update() instead of updateo()
- storage.patch() instead of patcho()
- storage.delete() instead of deleteo()

Python operations (filtero, mapo, reduceo, transformo) are still available
from haolib.pipelines.

Example:
    ```python
    from haolib.pipelines import filtero, mapo
    from haolib.storages.indexes.params import ParamIndex

    # New fluent API
    result = await (
        storage.read(ParamIndex(User, age=25)).returning()
        | filtero(lambda u: u.age >= 18)
        | mapo(lambda u, _idx: u.name)
    ).execute()
    ```

"""

# Deprecated: CRUD operations removed from public API
# Use fluent API: storage.create(), storage.read(), etc.
# Only Python operations are exported for pipeline composition
from haolib.pipelines import (
    filtero,
    mapo,
    reduceo,
    transformo,
)

__all__ = [
    "filtero",
    "mapo",
    "reduceo",
    "transformo",
]
