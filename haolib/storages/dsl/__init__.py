"""DSL for storage operations.

Provides convenient functions for creating storage operations with minimal code.

Example:
    ```python
    from haolib.storages.dsl import createo, reado, filtero
    from haolib.storages.indexes import index

    # Simple operations
    await storage.execute(createo([user1, user2]))

    user_index = index(User, age=25)
    async for user in await storage.execute(reado(search_index=user_index)):
        print(user)

    # Pipeline
    pipeline = (
        createo([user1, user2])
        | reado(search_index=index(User))
        | filtero(lambda u: u.age >= 18)
    )
    await storage.execute(pipeline)
    ```

"""

from haolib.storages.dsl.operations import (
    createo,
    deleteo,
    filtero,
    mapo,
    reado,
    reduceo,
    transformo,
    updateo,
)

__all__ = [
    "createo",
    "deleteo",
    "filtero",
    "mapo",
    "reado",
    "reduceo",
    "transformo",
    "updateo",
]
