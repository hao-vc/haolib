"""Tests for PredicateAnalyzer."""

from typing import Any

import pytest
from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from haolib.database.models.base.sqlalchemy import SQLAlchemyBaseModel
from haolib.storages.operations.concrete import FilterOperation
from haolib.storages.operations.optimizer.predicate_analyzer import PredicateAnalyzer


class UserModel(SQLAlchemyBaseModel):
    """Test user model."""

    __tablename__ = "test_users_predicate"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    age: Mapped[int] = mapped_column()
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)


# Define predicate functions at module level so inspect.getsource works
def predicate_equals(u: Any) -> bool:
    """Predicate for equals test."""
    return u.age == 25


def predicate_greater_than(u: Any) -> bool:
    """Predicate for greater than test."""
    return u.age > 18


def predicate_greater_than_or_equal(u: Any) -> bool:
    """Predicate for greater than or equal test."""
    return u.age >= 18


def predicate_less_than(u: Any) -> bool:
    """Predicate for less than test."""
    return u.age < 65


def predicate_less_than_or_equal(u: Any) -> bool:
    """Predicate for less than or equal test."""
    return u.age <= 65


def predicate_bool_and(u: Any) -> bool:
    """Predicate for boolean AND test."""
    return u.age >= 18 and u.age <= 65


def predicate_complex(u: Any) -> bool:
    """Predicate for complex expression test."""
    return u.age > 18 and u.age < 65 and u.name == "John"


def predicate_invalid_field(u: Any) -> bool:
    """Predicate with invalid field."""
    return u.invalid_field == 25


def predicate_non_attribute_left(u: Any) -> bool:
    """Predicate with non-attribute left side."""
    return 25 == u.age  # Constant on left, attribute on right


def predicate_non_constant_right(u: Any) -> bool:
    """Predicate with non-constant right side."""
    return u.age == u.name


def predicate_string_value(u: Any) -> bool:
    """Predicate with string value."""
    return u.name == "John"


def predicate_can_convert(u: Any) -> bool:
    """Predicate that can be converted to SQL."""
    return u.age >= 18


def predicate_cannot_convert(u: Any) -> bool:
    """Predicate that cannot be converted to SQL."""
    return u.name.startswith("A")


class TestPredicateAnalyzer:
    """Tests for PredicateAnalyzer."""

    def test_extract_conditions_equals(self) -> None:
        """Test extracting equals condition."""
        analyzer = PredicateAnalyzer()
        conditions = analyzer.extract_conditions(predicate_equals, UserModel)

        assert conditions is not None
        assert len(conditions) == 1
        # Check that condition is SQLAlchemy binary expression
        assert hasattr(conditions[0], "left")
        assert hasattr(conditions[0], "right")
        assert conditions[0].right.value == 25  # type: ignore[attr-defined]

    def test_extract_conditions_greater_than(self) -> None:
        """Test extracting greater than condition."""
        analyzer = PredicateAnalyzer()
        conditions = analyzer.extract_conditions(predicate_greater_than, UserModel)

        assert conditions is not None
        assert len(conditions) == 1

    def test_extract_conditions_greater_than_or_equal(self) -> None:
        """Test extracting greater than or equal condition."""
        analyzer = PredicateAnalyzer()
        conditions = analyzer.extract_conditions(predicate_greater_than_or_equal, UserModel)

        assert conditions is not None
        assert len(conditions) == 1

    def test_extract_conditions_less_than(self) -> None:
        """Test extracting less than condition."""
        analyzer = PredicateAnalyzer()
        conditions = analyzer.extract_conditions(predicate_less_than, UserModel)

        assert conditions is not None
        assert len(conditions) == 1

    def test_extract_conditions_less_than_or_equal(self) -> None:
        """Test extracting less than or equal condition."""
        analyzer = PredicateAnalyzer()
        conditions = analyzer.extract_conditions(predicate_less_than_or_equal, UserModel)

        assert conditions is not None
        assert len(conditions) == 1

    def test_extract_conditions_bool_and(self) -> None:
        """Test extracting boolean AND conditions."""
        analyzer = PredicateAnalyzer()
        conditions = analyzer.extract_conditions(predicate_bool_and, UserModel)

        assert conditions is not None
        assert len(conditions) == 2

    def test_extract_conditions_invalid_field(self) -> None:
        """Test extracting condition with invalid field."""
        analyzer = PredicateAnalyzer()
        conditions = analyzer.extract_conditions(predicate_invalid_field, UserModel)

        assert conditions is None

    def test_extract_conditions_non_callable(self) -> None:
        """Test extracting conditions from non-callable."""
        analyzer = PredicateAnalyzer()
        conditions = analyzer.extract_conditions("not a function", UserModel)

        assert conditions is None

    def test_extract_conditions_non_lambda(self) -> None:
        """Test extracting conditions from non-lambda function."""
        analyzer = PredicateAnalyzer()

        def regular_function(x: Any) -> bool:
            return x.age > 18

        conditions = analyzer.extract_conditions(regular_function, UserModel)

        # Should return None because it's not a lambda
        assert conditions is None

    def test_extract_conditions_complex_expression(self) -> None:
        """Test extracting conditions from complex expression."""
        analyzer = PredicateAnalyzer()
        conditions = analyzer.extract_conditions(predicate_complex, UserModel)

        assert conditions is not None
        assert len(conditions) == 3

    def test_extract_conditions_non_attribute_left(self) -> None:
        """Test extracting condition with non-attribute left side."""
        analyzer = PredicateAnalyzer()
        conditions = analyzer.extract_conditions(predicate_non_attribute_left, UserModel)

        # Should return None because left side is not attribute
        assert conditions is None

    def test_extract_conditions_non_constant_right(self) -> None:
        """Test extracting condition with non-constant right side."""
        analyzer = PredicateAnalyzer()
        conditions = analyzer.extract_conditions(predicate_non_constant_right, UserModel)

        # Should return None because right side is not constant
        assert conditions is None

    def test_extract_conditions_multiple_ops(self) -> None:
        """Test extracting condition with multiple operators."""
        analyzer = PredicateAnalyzer()

        def predicate(u: Any) -> bool:
            return 18 < u.age < 65

        conditions = analyzer.extract_conditions(predicate, UserModel)

        # Should return None because multiple ops not supported
        assert conditions is None

    def test_extract_conditions_bool_or(self) -> None:
        """Test extracting condition with OR operator."""
        analyzer = PredicateAnalyzer()

        def predicate_bool_or(u: Any) -> bool:
            return u.age < 18 or u.age > 65

        conditions = analyzer.extract_conditions(predicate_bool_or, UserModel)

        # Should return None because OR not supported yet
        assert conditions is None

    def test_can_convert_to_sql_valid(self) -> None:
        """Test can_convert_to_sql with valid predicate."""
        analyzer = PredicateAnalyzer()
        filter_op = FilterOperation(predicate=predicate_can_convert)
        result = analyzer.can_convert_to_sql(filter_op)

        assert result is True

    def test_can_convert_to_sql_invalid(self) -> None:
        """Test can_convert_to_sql with invalid predicate."""
        analyzer = PredicateAnalyzer()
        filter_op = FilterOperation(predicate="not a function")
        result = analyzer.can_convert_to_sql(filter_op)

        assert result is False

    def test_can_convert_to_sql_non_lambda(self) -> None:
        """Test can_convert_to_sql with non-lambda function."""
        analyzer = PredicateAnalyzer()

        def regular_function(x: Any) -> bool:
            return x.age > 18

        filter_op = FilterOperation(predicate=regular_function)
        result = analyzer.can_convert_to_sql(filter_op)

        assert result is False

    def test_can_convert_to_sql_complex_lambda(self) -> None:
        """Test can_convert_to_sql with complex lambda."""
        analyzer = PredicateAnalyzer()
        filter_op = FilterOperation(predicate=predicate_cannot_convert)
        result = analyzer.can_convert_to_sql(filter_op)

        assert result is False

    def test_extract_conditions_string_value(self) -> None:
        """Test extracting condition with string value."""
        analyzer = PredicateAnalyzer()
        conditions = analyzer.extract_conditions(predicate_string_value, UserModel)

        assert conditions is not None
        assert len(conditions) == 1

    def test_extract_conditions_none_value(self) -> None:
        """Test extracting condition with None value."""
        analyzer = PredicateAnalyzer()

        def predicate(u: Any) -> bool:
            return u.email is None

        conditions = analyzer.extract_conditions(predicate, UserModel)

        # is None uses Is operator, not Eq, so it won't be converted
        assert conditions is None

    def test_extract_conditions_invalid_syntax(self) -> None:
        """Test extracting conditions with invalid syntax."""
        analyzer = PredicateAnalyzer()
        # This will cause SyntaxError when trying to get source
        # We need to mock inspect.getsource to raise OSError
        from unittest.mock import patch

        with patch("inspect.getsource", side_effect=OSError("Cannot get source")):
            predicate = lambda u: u.age >= 18  # noqa: E731
            conditions = analyzer.extract_conditions(predicate, UserModel)

            assert conditions is None

    def test_extract_conditions_invalid_ast(self) -> None:
        """Test extracting conditions with invalid AST."""
        analyzer = PredicateAnalyzer()
        from unittest.mock import patch

        with patch("ast.parse", side_effect=ValueError("Invalid syntax")):
            predicate = lambda u: u.age >= 18  # noqa: E731
            conditions = analyzer.extract_conditions(predicate, UserModel)

            assert conditions is None

    def test_extract_conditions_non_expr_body(self) -> None:
        """Test extracting conditions when body is not Expr."""
        analyzer = PredicateAnalyzer()
        from unittest.mock import MagicMock, patch

        # Mock AST tree with non-Expr body
        mock_tree = MagicMock()
        mock_tree.body = [MagicMock()]  # Not an Expr node
        with patch("ast.parse", return_value=mock_tree):
            predicate = lambda u: u.age >= 18  # noqa: E731
            conditions = analyzer.extract_conditions(predicate, UserModel)

            assert conditions is None

    def test_extract_conditions_non_lambda_value(self) -> None:
        """Test extracting conditions when value is not Lambda."""
        analyzer = PredicateAnalyzer()
        from unittest.mock import MagicMock, patch

        # Mock AST tree with non-Lambda value
        mock_tree = MagicMock()
        mock_expr = MagicMock()
        mock_expr.value = MagicMock()  # Not a Lambda node
        mock_tree.body = [mock_expr]
        with patch("ast.parse", return_value=mock_tree):
            predicate = lambda u: u.age >= 18  # noqa: E731
            conditions = analyzer.extract_conditions(predicate, UserModel)

            assert conditions is None

    def test_analyze_lambda_non_compare_non_boolop(self) -> None:
        """Test _analyze_lambda with non-Compare, non-BoolOp body."""
        analyzer = PredicateAnalyzer()
        from unittest.mock import MagicMock

        lambda_node = MagicMock()
        lambda_node.body = MagicMock()  # Not Compare or BoolOp

        result = analyzer._analyze_lambda(lambda_node, UserModel)

        assert result is None

    def test_analyze_bool_op_empty_conditions(self) -> None:
        """Test _analyze_bool_op with empty conditions."""
        analyzer = PredicateAnalyzer()
        import ast

        # Create BoolOp with empty values
        bool_op = ast.BoolOp(op=ast.And(), values=[])

        result = analyzer._analyze_bool_op(bool_op, UserModel)

        assert result is None

    def test_analyze_compare_multiple_ops(self) -> None:
        """Test _analyze_compare with multiple operators."""
        analyzer = PredicateAnalyzer()
        import ast

        # Create Compare with multiple ops
        compare_node = ast.Compare(
            left=ast.Attribute(value=ast.Name(id="u", ctx=ast.Load()), attr="age", ctx=ast.Load()),
            ops=[ast.Lt(), ast.Lt()],
            comparators=[ast.Constant(value=18), ast.Constant(value=65)],
        )

        result = analyzer._analyze_compare(compare_node, UserModel)

        assert result is None

    def test_analyze_compare_multiple_comparators(self) -> None:
        """Test _analyze_compare with multiple comparators."""
        analyzer = PredicateAnalyzer()
        import ast

        # Create Compare with multiple comparators
        compare_node = ast.Compare(
            left=ast.Attribute(value=ast.Name(id="u", ctx=ast.Load()), attr="age", ctx=ast.Load()),
            ops=[ast.Eq()],
            comparators=[ast.Constant(value=18), ast.Constant(value=65)],
        )

        result = analyzer._analyze_compare(compare_node, UserModel)

        assert result is None

    def test_analyze_compare_non_attribute_left(self) -> None:
        """Test _analyze_compare with non-attribute left side."""
        analyzer = PredicateAnalyzer()
        import ast

        # Create Compare with non-attribute left
        compare_node = ast.Compare(
            left=ast.Constant(value=18),
            ops=[ast.Eq()],
            comparators=[ast.Attribute(value=ast.Name(id="u", ctx=ast.Load()), attr="age", ctx=ast.Load())],
        )

        result = analyzer._analyze_compare(compare_node, UserModel)

        assert result is None

    def test_analyze_compare_non_constant_right(self) -> None:
        """Test _analyze_compare with non-constant right side."""
        analyzer = PredicateAnalyzer()
        import ast

        # Create Compare with non-constant right
        compare_node = ast.Compare(
            left=ast.Attribute(value=ast.Name(id="u", ctx=ast.Load()), attr="age", ctx=ast.Load()),
            ops=[ast.Eq()],
            comparators=[ast.Attribute(value=ast.Name(id="u", ctx=ast.Load()), attr="name", ctx=ast.Load())],
        )

        result = analyzer._analyze_compare(compare_node, UserModel)

        assert result is None

    def test_operator_to_condition_unknown_operator(self) -> None:
        """Test _operator_to_condition with unknown operator."""
        analyzer = PredicateAnalyzer()
        import ast

        # Create unknown operator (e.g., Is, IsNot, In, NotIn)
        # We'll use a mock operator
        from unittest.mock import MagicMock

        unknown_op = MagicMock()
        result = analyzer._operator_to_condition(unknown_op, "age", 25, UserModel)

        assert result is None

    def test_operator_to_condition_attribute_error(self) -> None:
        """Test _operator_to_condition with AttributeError."""
        analyzer = PredicateAnalyzer()
        import ast

        # Try to get non-existent field
        result = analyzer._operator_to_condition(ast.Eq(), "non_existent_field", 25, UserModel)

        assert result is None
