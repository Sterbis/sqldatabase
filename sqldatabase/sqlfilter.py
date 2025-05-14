from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable

from .sqlcondition import SQLCondition
from .sqloperator import ESQLComparisonOperator

if TYPE_CHECKING:
    from .sqlcolumn import SQLColumn
    from .sqlfunction import SQLFunction


class SQLFilter(SQLCondition):
    """Represents a filter applied to a SQL column.

    Attributes:
        operator (ESQLComparisonOperator): The comparison operator used in the filter.
        item (SQLColumn | SQLFunction): The column or function to which the filter is applied.
    """

    operator: ESQLComparisonOperator

    def __init__(self, item: SQLColumn | SQLFunction, *values) -> None:
        """
        Initializes a SQLFilter instance.

        Args:
            item (SQLColumn | SQLFunction): The column or function to which the filter is applied.
            *values: The values used in the filter.
        """
        SQLCondition.__init__(self, item, self.operator, *values)
        self.item = item


class SQLValueFilter(SQLFilter):
    """Represents a filter that uses a single value for comparison.

    Attributes:
        value (Any): The value used in the filter.
    """

    def __init__(self, item: SQLColumn | SQLFunction, value: Any) -> None:
        """
        Initializes a SQLValueFilter instance.

        Args:
            item (SQLColumn | SQLFunction): The column or function to which the filter is applied.
            value (Any): The value used in the filter.
        """
        SQLFilter.__init__(self, item, value)
        self.value = value


class SQLEqualFilter(SQLValueFilter):
    """Represents a filter that checks for equality."""

    operator = ESQLComparisonOperator.EQUAL


class SQLNotEqualFilter(SQLValueFilter):
    """Represents a filter that checks for inequality."""

    operator = ESQLComparisonOperator.NOT_EQUAL


class SQLLessThanFilter(SQLValueFilter):
    """Represents a filter that checks if a value is less than another."""

    operator = ESQLComparisonOperator.LESS_THAN


class SQLLessThanOrEqualFilter(SQLValueFilter):
    """Represents a filter that checks if a value is less than or equal to another."""

    operator = ESQLComparisonOperator.LESS_THAN_OR_EQUAL


class SQLGreaterThanFilter(SQLValueFilter):
    """Represents a filter that checks if a value is greater than another."""

    operator = ESQLComparisonOperator.GREATER_THAN


class SQLGreaterThanOrEqualFilter(SQLValueFilter):
    """Represents a filter that checks if a value is greater than or equal to another."""

    operator = ESQLComparisonOperator.GREATER_THAN_OR_EQUAL


class SQLLikeFilter(SQLValueFilter):
    """Represents a filter that checks if a value matches a pattern."""

    operator = ESQLComparisonOperator.LIKE


class SQLNotLikeFilter(SQLValueFilter):
    """Represents a filter that checks if a value does not match a pattern."""

    operator = ESQLComparisonOperator.NOT_LIKE


class SQLValuesFilter(SQLFilter):
    """Represents a filter that uses multiple values for comparison.

    Attributes:
        values (Iterable): The values used in the filter.
    """

    def __init__(self, item: SQLColumn | SQLFunction, values: Iterable) -> None:
        """
        Initializes a SQLValuesFilter instance.

        Args:
            item (SQLColumn | SQLFunction): The column or function to which the filter is applied.
            values (Iterable): The values used in the filter.
        """
        SQLFilter.__init__(self, item, *values)


class SQLInFilter(SQLValuesFilter):
    """Represents a filter that checks if a value is in a set of values."""

    operator = ESQLComparisonOperator.IN


class SQLNotInFilter(SQLValuesFilter):
    """Represents a filter that checks if a value is not in a set of values."""

    operator = ESQLComparisonOperator.NOT_IN


class SQLBetweenBaseFilter(SQLFilter):
    """Represents a filter that checks if a value is between two bounds.

    Attributes:
        lower_value (Any): The lower bound value.
        upper_value (Any): The upper bound value.
    """

    def __init__(
        self, item: SQLColumn | SQLFunction, lower_value: Any, upper_value: Any
    ):
        """
        Initializes a SQLBetweenBaseFilter instance.

        Args:
            item (SQLColumn | SQLFunction): The column or function to which the filter is applied.
            lower_value (Any): The lower bound value.
            upper_value (Any): The upper bound value.
        """
        SQLFilter.__init__(self, item, lower_value, upper_value)
        self.lower_value = lower_value
        self.upper_value = upper_value


class SQLBetweenFilter(SQLBetweenBaseFilter):
    """Represents a filter that checks if a value is between two bounds."""

    operator = ESQLComparisonOperator.BETWEEN


class SQLNotBetweenFilter(SQLBetweenBaseFilter):
    """Represents a filter that checks if a value is not between two bounds."""

    operator = ESQLComparisonOperator.NOT_BETWEEN


class SQLIsNullBaseFilter(SQLFilter):
    """Represents a filter that checks for null values."""

    def __init__(self, item: SQLColumn | SQLFunction):
        """
        Initializes a SQLIsNullBaseFilter instance.

        Args:
            item (SQLColumn | SQLFunction): The column or function to which the filter is applied.
        """
        SQLFilter.__init__(self, item)


class SQLIsNullFilter(SQLIsNullBaseFilter):
    """Represents a filter that checks if a value is null."""

    operator = ESQLComparisonOperator.IS_NULL


class SQLIsNotNullFilter(SQLIsNullBaseFilter):
    """Represents a filter that checks if a value is not null."""

    operator = ESQLComparisonOperator.IS_NOT_NULL


class SQLFilters:
    """Represents filters that can be applied to a SQL column or function.

    Attributes:
        item (SQLColumn | SQLFunction): The column or function to which the filters are applied.
    """

    def __init__(self, item: SQLColumn | SQLFunction):
        """
        Initializes a SQLFilters instance.

        Args:
            item (SQLColumn | SQLFunction): The column or function to which the filters are applied.
        """
        self.item = item

    def BETWEEN(self, lower_value: Any, upper_value: Any) -> SQLBetweenFilter:
        """
        Creates a BETWEEN filter.

        Args:
            lower_value (Any): The lower bound value.
            upper_value (Any): The upper bound value.

        Returns:
            SQLBetweenFilter: The created filter.
        """
        return SQLBetweenFilter(self.item, lower_value, upper_value)

    def EQUAL(self, value: Any) -> SQLEqualFilter:
        """
        Creates an EQUAL filter.

        Args:
            value (Any): The value to compare.

        Returns:
            SQLEqualFilter: The created filter.
        """
        return SQLEqualFilter(self.item, value)

    def GREATER_THAN(self, value: Any) -> SQLGreaterThanFilter:
        """
        Creates a GREATER_THAN filter.

        Args:
            value (Any): The value to compare.

        Returns:
            SQLGreaterThanFilter: The created filter.
        """
        return SQLGreaterThanFilter(self.item, value)

    def GREATER_THAN_OR_EQUAL(self, value: Any) -> SQLGreaterThanOrEqualFilter:
        """
        Creates a GREATER_THAN_OR_EQUAL filter.

        Args:
            value (Any): The value to compare.

        Returns:
            SQLGreaterThanOrEqualFilter: The created filter.
        """
        return SQLGreaterThanOrEqualFilter(self.item, value)

    def IN(self, values: Iterable) -> SQLInFilter:
        """
        Creates an IN filter.

        Args:
            values (Iterable): The values to compare.

        Returns:
            SQLInFilter: The created filter.
        """
        return SQLInFilter(self.item, values)

    def IS_NOT_NULL(self) -> SQLIsNotNullFilter:
        """
        Creates an IS_NOT_NULL filter.

        Returns:
            SQLIsNotNullFilter: The created filter.
        """
        return SQLIsNotNullFilter(self.item)

    def IS_NULL(self) -> SQLIsNullFilter:
        """
        Creates an IS_NULL filter.

        Returns:
            SQLIsNullFilter: The created filter.
        """
        return SQLIsNullFilter(self.item)

    def LESS_THAN(self, value: Any) -> SQLLessThanFilter:
        """
        Creates a LESS_THAN filter.

        Args:
            value (Any): The value to compare.

        Returns:
            SQLLessThanFilter: The created filter.
        """
        return SQLLessThanFilter(self.item, value)

    def LESS_THAN_OR_EQUAL(self, value: Any) -> SQLLessThanOrEqualFilter:
        """
        Creates a LESS_THAN_OR_EQUAL filter.

        Args:
            value (Any): The value to compare.

        Returns:
            SQLLessThanOrEqualFilter: The created filter.
        """
        return SQLLessThanOrEqualFilter(self.item, value)

    def LIKE(self, value: Any) -> SQLLikeFilter:
        """
        Creates a LIKE filter.

        Args:
            value (Any): The value to compare.

        Returns:
            SQLLikeFilter: The created filter.
        """
        return SQLLikeFilter(self.item, value)

    def NOT_BETWEEN(self, lower_value: Any, upper_value: Any) -> SQLNotBetweenFilter:
        """
        Creates a NOT_BETWEEN filter.

        Args:
            lower_value (Any): The lower bound value.
            upper_value (Any): The upper bound value.

        Returns:
            SQLNotBetweenFilter: The created filter.
        """
        return SQLNotBetweenFilter(self.item, lower_value, upper_value)

    def NOT_EQUAL(self, value: Any) -> SQLNotEqualFilter:
        """
        Creates a NOT_EQUAL filter.

        Args:
            value (Any): The value to compare.

        Returns:
            SQLNotEqualFilter: The created filter.
        """
        return SQLNotEqualFilter(self.item, value)

    def NOT_IN(self, values: Iterable) -> SQLNotInFilter:
        """
        Creates a NOT_IN filter.

        Args:
            values (Iterable): The values to compare.

        Returns:
            SQLNotInFilter: The created filter.
        """
        return SQLNotInFilter(self.item, values)

    def NOT_LIKE(self, value: Any) -> SQLNotLikeFilter:
        """
        Creates a NOT_LIKE filter.

        Args:
            value (Any): The value to compare.

        Returns:
            SQLNotLikeFilter: The created filter.
        """
        return SQLNotLikeFilter(self.item, value)
