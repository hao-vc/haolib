"""Fluent storage API for haolib."""

from haolib.storages.fluent.composites import (
    BaseComposite,
    CreateComposite,
    DeleteComposite,
    PatchComposite,
    ReadComposite,
    ReadDeleteComposite,
    ReadPatchComposite,
    ReadUpdateComposite,
    ReadUpdateDeleteComposite,
    UpdateComposite,
)
from haolib.storages.fluent.protocols import (
    CreateOperatable,
    DeleteOperatable,
    PatchOperatable,
    ReadOperatable,
    UpdateOperatable,
)

__all__ = [
    "BaseComposite",
    "CreateComposite",
    "CreateOperatable",
    "DeleteComposite",
    "DeleteOperatable",
    "PatchComposite",
    "PatchOperatable",
    "ReadComposite",
    "ReadDeleteComposite",
    "ReadPatchComposite",
    "ReadOperatable",
    "ReadUpdateComposite",
    "ReadUpdateDeleteComposite",
    "UpdateComposite",
    "UpdateOperatable",
]

