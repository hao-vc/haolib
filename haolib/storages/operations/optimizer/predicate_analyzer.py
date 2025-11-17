"""Predicate analyzer for converting lambda functions to SQL conditions.

Analyzes Python lambda functions and converts them to SQLAlchemy query conditions
when possible. This allows FilterOperation predicates to be executed in SQL.
"""

import ast
import inspect
from typing import Any

from sqlalchemy.orm import DeclarativeBase, InstrumentedAttribute

# Import FilterOperation lazily to avoid circular import
# FilterOperation is imported in methods that use it
from haolib.utils.rattrs import rgetattr


class PredicateAnalyzer:
    """Analyzes predicate functions and converts them to SQL conditions.

    Currently supports simple comparisons like:
    - lambda x: x.field == value
    - lambda x: x.field > value
    - lambda x: x.field >= value
    - lambda x: x.field < value
    - lambda x: x.field <= value

    Example:
        ```python
        analyzer = PredicateAnalyzer()
        predicate = lambda u: u.age >= 18
        conditions = analyzer.extract_conditions(predicate, UserModel)
        # Returns [UserModel.age >= 18] (SQLAlchemy condition)
        ```

    """

    def extract_conditions(self, predicate: Any, model: type[DeclarativeBase]) -> list[Any] | None:
        """Extract SQL conditions from predicate function.

        Args:
            predicate: Lambda function or callable to analyze.
            model: SQLAlchemy model class to get columns from.

        Returns:
            List of SQLAlchemy conditions if predicate can be converted to SQL,
            None otherwise.

        """
        if not callable(predicate):
            return None

        try:
            # Get source code of the predicate
            source = inspect.getsource(predicate)
            # Parse AST
            tree = ast.parse(source)

            # Handle lambda expression
            if isinstance(tree.body[0], ast.Expr) and isinstance(tree.body[0].value, ast.Lambda):
                lambda_node = tree.body[0].value
                return self._analyze_lambda(lambda_node, model)

            # Handle function definition
            if isinstance(tree.body[0], ast.FunctionDef):
                func_def = tree.body[0]
                # Find return statement (skip docstrings and other statements)
                return_stmt = None
                for stmt in func_def.body:
                    if isinstance(stmt, ast.Return):
                        return_stmt = stmt
                        break

                if return_stmt is not None and return_stmt.value is not None:
                    return_expr = return_stmt.value
                    # Handle comparison in return
                    if isinstance(return_expr, ast.Compare):
                        return self._analyze_compare(return_expr, model)
                    # Handle boolean operation in return
                    if isinstance(return_expr, ast.BoolOp):
                        return self._analyze_bool_op(return_expr, model)
        except (OSError, SyntaxError, ValueError):
            # Cannot analyze - return None
            # OSError happens when inspect.getsource can't find source (e.g., in tests)
            return None

        return None

    def _analyze_lambda(self, lambda_node: ast.Lambda, model: type[DeclarativeBase]) -> list[Any] | None:
        """Analyze lambda AST node.

        Args:
            lambda_node: AST node representing lambda function.
            model: SQLAlchemy model class.

        Returns:
            List of SQLAlchemy conditions or None if cannot convert.

        """
        body = lambda_node.body

        # Handle comparison operations
        if isinstance(body, ast.Compare):
            return self._analyze_compare(body, model)

        # Handle boolean operations (and/or)
        if isinstance(body, ast.BoolOp):
            return self._analyze_bool_op(body, model)

        return None

    def _analyze_compare(self, compare_node: ast.Compare, model: type[DeclarativeBase]) -> list[Any] | None:
        """Analyze comparison AST node.

        Args:
            compare_node: AST node representing comparison.
            model: SQLAlchemy model class.

        Returns:
            List of SQLAlchemy conditions or None.

        """
        # Only handle single comparison for now
        if len(compare_node.ops) != 1:
            return None

        left = compare_node.left
        op = compare_node.ops[0]
        comparators = compare_node.comparators

        if len(comparators) != 1:
            return None

        right = comparators[0]

        # Left side must be attribute access (e.g., x.field)
        if not isinstance(left, ast.Attribute):
            return None

        # Get field name
        field_name = left.attr

        # Right side must be a constant value
        if not isinstance(right, ast.Constant):
            return None

        value = right.value

        # Convert operator to SQL condition
        condition = self._operator_to_condition(op, field_name, value, model)
        if condition is not None:
            return [condition]

        return None

    def _operator_to_condition(  # noqa: PLR0911
        self, op: ast.cmpop, field_name: str, value: Any, model: type[DeclarativeBase]
    ) -> Any | None:
        """Convert AST comparison operator to SQLAlchemy condition.

        Args:
            op: AST comparison operator.
            field_name: Name of the field being compared.
            value: Value to compare against.
            model: SQLAlchemy model class.

        Returns:
            SQLAlchemy condition or None.

        """
        # Get column from model
        try:
            column: InstrumentedAttribute = rgetattr(model, field_name)
        except AttributeError:
            return None

        if isinstance(op, ast.Eq):
            return column == value
        if isinstance(op, ast.Gt):
            return column > value
        if isinstance(op, ast.GtE):
            return column >= value
        if isinstance(op, ast.Lt):
            return column < value
        if isinstance(op, ast.LtE):
            return column <= value
        return None

    def _analyze_bool_op(self, bool_op_node: ast.BoolOp, model: type[DeclarativeBase]) -> list[Any] | None:
        """Analyze boolean operation (and/or).

        Args:
            bool_op_node: AST node representing boolean operation.
            model: SQLAlchemy model class.

        Returns:
            List of SQLAlchemy conditions or None.

        """
        # For now, only handle AND operations
        if not isinstance(bool_op_node.op, ast.And):
            return None

        conditions: list[Any] = []
        for value in bool_op_node.values:
            if isinstance(value, ast.Compare):
                compare_conditions = self._analyze_compare(value, model)
                if compare_conditions:
                    conditions.extend(compare_conditions)
                else:
                    # Cannot convert one of the comparisons - fail
                    return None
            else:
                # Cannot convert - fail
                return None

        return conditions if conditions else None

    def can_convert_to_sql(self, filter_operation: FilterOperation[Any]) -> bool:
        """Check if FilterOperation predicate can be converted to SQL.

        This method checks if the predicate structure can be analyzed,
        without actually building SQL conditions.

        Args:
            filter_operation: FilterOperation to check.

        Returns:
            True if predicate can be converted to SQL, False otherwise.

        """
        if not callable(filter_operation.predicate):
            return False

        try:
            source = inspect.getsource(filter_operation.predicate)
            tree = ast.parse(source)

            # Handle lambda expression
            if isinstance(tree.body[0], ast.Expr) and isinstance(tree.body[0].value, ast.Lambda):
                lambda_node = tree.body[0].value
                # Check if it's a simple comparison or boolean op
                return isinstance(lambda_node.body, (ast.Compare, ast.BoolOp))

            # Handle function definition
            if isinstance(tree.body[0], ast.FunctionDef):
                func_def = tree.body[0]
                # Find return statement (skip docstrings and other statements)
                return_stmt = None
                for stmt in func_def.body:
                    if isinstance(stmt, ast.Return):
                        return_stmt = stmt
                        break

                if return_stmt is not None and return_stmt.value is not None:
                    return_expr = return_stmt.value
                    # Check if return expression is comparison or boolean op
                    return isinstance(return_expr, (ast.Compare, ast.BoolOp))
        except (OSError, SyntaxError, ValueError):
            return False

        return False
