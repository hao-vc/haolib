"""Batch."""

from typing import TYPE_CHECKING, Self, overload

from haolib.batches.abstract import AbstractBatch

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


class Batch[T_Key, T_Value](AbstractBatch[T_Key, T_Value]):
    """Batch."""

    key_getter: Callable[[T_Value], T_Key]

    def __init__(self, key_getter: Callable[[T_Value], T_Key]) -> None:
        """Initialize the batch."""

        self.key_getter = key_getter

        self._values_dict_indexed: dict[T_Key, int] = {}
        self._values: list[T_Value] = []

        self._index = 0

    def __iter__(self) -> Iterator[T_Value]:
        """Iterate over the batch."""

        self._index = 0

        return self

    def __len__(self) -> int:
        """Get the size of the batch."""

        return len(self._values)

    def __next__(self) -> T_Value:
        """Return the next item in the batch."""

        if self._index >= len(self._values):
            raise StopIteration

        result = self._values[self._index]

        self._index += 1

        return result

    def merge_dict(self, data: dict[T_Key, T_Value]) -> Self:
        """Return the batch from a dict."""

        for key, value in data.items():
            self._values_dict_indexed[key] = len(self._values)
            self._values.append(value)

        return self

    def merge_list(self, data: list[T_Value]) -> Self:
        """Return the batch from a list."""

        for value in data:
            self._values_dict_indexed[self.key_getter(value)] = len(self._values)
            self._values.append(value)

        return self

    def merge_set(self, data: set[T_Value]) -> Self:
        """Return the batch from a set."""
        for value in data:
            self._values_dict_indexed[self.key_getter(value)] = len(self._values)
            self._values.append(value)

        return self

    def to_dict(self) -> dict[T_Key, T_Value]:
        """Return the batch as a dict."""
        return {key: self._values[index] for key, index in self._values_dict_indexed.items()}

    def to_list(self) -> list[T_Value]:
        """Return the batch as a list."""
        return self._values

    def to_set(self) -> set[T_Value]:
        """Return the batch as a set."""
        return set(self._values)

    @overload
    def get_by_index(self, index: int, exception: Exception | type[Exception]) -> T_Value: ...

    @overload
    def get_by_index(self, index: int, exception: None = None) -> T_Value | None: ...

    def get_by_index(self, index: int, exception: Exception | type[Exception] | None = None) -> T_Value | None:
        """Return the first item in the batch."""
        if index < 0 or index >= len(self._values):
            if exception is not None:
                raise exception

            return None

        return self._values[index]

    @overload
    def get_by_key(self, key: T_Key, exception: Exception | type[Exception]) -> T_Value: ...

    @overload
    def get_by_key(
        self,
        key: T_Key,
        exception: None = None,
    ) -> T_Value | None: ...

    def get_by_key(
        self,
        key: T_Key,
        exception: Exception | type[Exception] | None = None,
    ) -> T_Value | None:
        """Return the item by key."""
        if key not in self._values_dict_indexed:
            if exception is not None:
                raise exception

            return None

        return self._values[self._values_dict_indexed[key]]

    def get_keys(self) -> set[T_Key]:
        """Return the keys of the batch."""
        return set(self._values_dict_indexed.keys())
