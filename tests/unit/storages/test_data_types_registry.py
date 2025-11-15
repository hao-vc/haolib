"""Unit tests for DataTypeRegistry."""

import pytest

from haolib.storages.data_types.registry import DataTypeRegistry, TypeRegistration


class TestDataTypeRegistry:
    """Tests for DataTypeRegistry."""

    def test_register_single_mapping(self) -> None:
        """Test registering a single type mapping."""
        registry = DataTypeRegistry()

        class User:
            def __init__(self, name: str) -> None:
                self.name = name

        class UserModel:
            def __init__(self, name: str) -> None:
                self.name = name

        registry.register(
            storage_type=UserModel,
            user_type=User,
            to_storage=lambda u: UserModel(u.name),
            from_storage=lambda m: User(m.name),
        )

        # Check registration exists
        reg = registry.get_for_user_type(User)
        assert reg is not None
        assert reg.storage_type == UserModel
        assert reg.user_type == User

        reg2 = registry.get_for_storage_type(UserModel)
        assert reg2 is not None
        assert reg2.storage_type == UserModel
        assert reg2.user_type == User

    def test_register_multiple_mappings_same_user_type(self) -> None:
        """Test registering multiple storage types for same user type."""
        registry = DataTypeRegistry()

        class User:
            def __init__(self, name: str) -> None:
                self.name = name

        class UserModel1:
            def __init__(self, name: str) -> None:
                self.name = name

        class UserModel2:
            def __init__(self, name: str) -> None:
                self.name = name

        registry.register(
            storage_type=UserModel1,
            user_type=User,
            to_storage=lambda u: UserModel1(u.name),
            from_storage=lambda m: User(m.name),
        )

        registry.register(
            storage_type=UserModel2,
            user_type=User,
            to_storage=lambda u: UserModel2(u.name),
            from_storage=lambda m: User(m.name),
        )

        # Should raise error when multiple mappings and storage_type not specified
        with pytest.raises(ValueError, match="Multiple mappings found"):
            registry.get_for_user_type(User)

        # Should return specific when storage_type specified
        reg1 = registry.get_for_user_type(User, storage_type=UserModel1)
        assert reg1 is not None
        assert reg1.storage_type == UserModel1

        reg2 = registry.get_for_user_type(User, storage_type=UserModel2)
        assert reg2 is not None
        assert reg2.storage_type == UserModel2

    def test_register_multiple_mappings_same_storage_type(self) -> None:
        """Test registering multiple user types for same storage type."""
        registry = DataTypeRegistry()

        class User:
            def __init__(self, name: str) -> None:
                self.name = name

        class Admin:
            def __init__(self, name: str) -> None:
                self.name = name

        class UserModel:
            def __init__(self, name: str) -> None:
                self.name = name

        registry.register(
            storage_type=UserModel,
            user_type=User,
            to_storage=lambda u: UserModel(u.name),
            from_storage=lambda m: User(m.name),
        )

        registry.register(
            storage_type=UserModel,
            user_type=Admin,
            to_storage=lambda a: UserModel(a.name),
            from_storage=lambda m: Admin(m.name),
        )

        # Should raise error when multiple mappings and user_type not specified
        with pytest.raises(ValueError, match="Multiple mappings found"):
            registry.get_for_storage_type(UserModel)

        # Should return specific when user_type specified
        reg1 = registry.get_for_storage_type(UserModel, user_type=User)
        assert reg1 is not None
        assert reg1.user_type == User

        reg2 = registry.get_for_storage_type(UserModel, user_type=Admin)
        assert reg2 is not None
        assert reg2.user_type == Admin

    def test_get_for_user_type_not_found(self) -> None:
        """Test getting registration for non-existent user type."""
        registry = DataTypeRegistry()

        class User:
            pass

        reg = registry.get_for_user_type(User)
        assert reg is None

    def test_get_for_storage_type_not_found(self) -> None:
        """Test getting registration for non-existent storage type."""
        registry = DataTypeRegistry()

        class UserModel:
            pass

        reg = registry.get_for_storage_type(UserModel)
        assert reg is None

    def test_get_for_user_type_wrong_storage_type(self) -> None:
        """Test getting registration with wrong storage_type."""
        registry = DataTypeRegistry()

        class User:
            pass

        class UserModel1:
            pass

        class UserModel2:
            pass

        class UserModel3:
            pass

        registry.register(
            storage_type=UserModel1,
            user_type=User,
            to_storage=lambda u: UserModel1(),
            from_storage=lambda m: User(),
        )

        registry.register(
            storage_type=UserModel2,
            user_type=User,
            to_storage=lambda u: UserModel2(),
            from_storage=lambda m: User(),
        )

        # Now with multiple mappings, wrong storage_type should raise error
        with pytest.raises(ValueError, match="No mapping found"):
            registry.get_for_user_type(User, storage_type=UserModel3)

    def test_get_for_storage_type_wrong_user_type(self) -> None:
        """Test getting registration with wrong user_type."""
        registry = DataTypeRegistry()

        class User:
            pass

        class Admin:
            pass

        class Guest:
            pass

        class UserModel:
            pass

        registry.register(
            storage_type=UserModel,
            user_type=User,
            to_storage=lambda u: UserModel(),
            from_storage=lambda m: User(),
        )

        registry.register(
            storage_type=UserModel,
            user_type=Admin,
            to_storage=lambda a: UserModel(),
            from_storage=lambda m: Admin(),
        )

        # Now with multiple mappings, wrong user_type should raise error
        with pytest.raises(ValueError, match="No mapping found"):
            registry.get_for_storage_type(UserModel, user_type=Guest)

    def test_maps_to_decorator_with_functions(self) -> None:
        """Test maps_to decorator with custom conversion functions."""
        registry = DataTypeRegistry()

        class User:
            def __init__(self, name: str) -> None:
                self.name = name

        @registry.maps_to(
            User,
            to_storage=lambda u: UserModel(u.name),
            from_storage=lambda m: User(m.name),
        )
        class UserModel:
            def __init__(self, name: str) -> None:
                self.name = name

        # Check registration exists
        reg = registry.get_for_user_type(User)
        assert reg is not None
        assert reg.storage_type == UserModel
        assert reg.user_type == User

    def test_maps_to_decorator_without_functions(self) -> None:
        """Test maps_to decorator without conversion functions raises error."""
        registry = DataTypeRegistry()

        class User:
            pass

        with pytest.raises(ValueError, match="Cannot automatically determine conversion"):

            @registry.maps_to(User)
            class UserModel:
                pass

    def test_type_registration_immutability(self) -> None:
        """Test that TypeRegistration is immutable (frozen dataclass)."""
        registry = DataTypeRegistry()

        class User:
            pass

        class UserModel:
            pass

        reg = TypeRegistration(
            storage_type=UserModel,
            user_type=User,
            to_storage=lambda u: UserModel(),
            from_storage=lambda m: User(),
        )

        # Should not be able to modify
        with pytest.raises(Exception):  # noqa: B017
            reg.storage_type = User  # type: ignore[misc]
