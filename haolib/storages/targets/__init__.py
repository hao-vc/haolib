"""Data targets for storage operations.

Targets are entities that can execute operations, such as storages, ML models, APIs, etc.
The ^ operator semantically means "send data to this target".
"""

from haolib.storages.targets.abstract import AbstractDataTarget

__all__ = [
    "AbstractDataTarget",
]
