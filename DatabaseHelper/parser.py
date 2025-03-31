import logging
import sqlglot
import sqlglot.errors

from sqlglot.expressions import (
    Column,
    Alias,
    Func,
    Table,
    Union,
    Group,
    Column,
    Subquery,
    Select,
    Star,
    Literal,
    Mul,
    Count,
    From,
    Sum,
    Join,
)


class ParserValidator:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def transformQuery(self, query):
        try:
            parsed = sqlglot.parse_one(query)
            modified_query = self.modifyQuery(parsed)
            return modified_query
        except sqlglot.errors.ParseError as e:
            self.logger.error(f"Failed to parse SQL: {e}")
            raise

    def getTablesNames(self, query):
        """
        Extracts all table names from a SQL query, including those in subqueries.
        """
        try:
            parsed = sqlglot.parse_one(query)
            tables = []

            for table in parsed.find_all(Table):
                tables.append(table.name)

            return tables
        except sqlglot.errors.ParseError as e:
            self.logger.error(f"Failed to parse SQL: {e}")
            raise

    def getAggColumns(self, query):
        """
        Extracts the indices of columns with aggregate functions (SUM, AVG, MIN, MAX, COUNT).
        """
        try:
            parsed = sqlglot.parse_one(query)
            aggregates = {"SUM", "AVG", "MIN", "MAX", "COUNT"}
            indices = []

            select_expressions = parsed.expressions

            for idx, expression in enumerate(select_expressions):
                # Handle both direct functions and aliases
                if isinstance(expression, Alias):
                    expression = (
                        expression.this
                    )  # Get the actual expression inside the alias

                if (
                    isinstance(expression, Func)
                    and expression.key.upper() in aggregates
                ):
                    indices.append(idx)

            return indices

        except sqlglot.errors.ParseError as e:
            self.logger.error(f"Failed to parse SQL: {e}")
            raise

    def addAliasToFunc(self, node, alias_counter=1):
        """
        Adds aliases to functions in a specific SQL AST node.
        """
        for column in node.expressions:
            if isinstance(column, Func):
                alias_name = f"func_{alias_counter}"

                alias_counter += 1

                # Wrap the function in an Alias node
                aliased_function = Alias(this=column.copy(), alias=alias_name)
                column.replace(aliased_function)

        return node

    def transformDistinct(self, query):
        """
        Transforms a DISTINCT query into an equivalent GROUP BY query.
        """
        # Remove DISTINCT
        query.set("distinct", False)

        # Collect all selected columns
        columns = query.expressions

        # Create GROUP BY clause using the same columns
        group_by = Group(expressions=[col.copy() for col in columns])

        # Attach the GROUP BY clause to the SELECT
        query.set("group", group_by)

        return query

    def hasGroupByOrAggregation(self, expression):
        has_group_by = expression.args.get("group") is not None

        if has_group_by:
            return True
        else:
            # Check for aggregation functions
            aggregates = {"SUM", "AVG", "MIN", "MAX", "COUNT"}

            select_expressions = expression.expressions

            for idx, expression in enumerate(select_expressions):
                # Handle both direct functions and aliases
                if isinstance(expression, Alias):
                    expression = (
                        expression.this
                    )  # Get the actual expression inside the alias

                if (
                    isinstance(expression, Func)
                    and expression.key.upper() in aggregates
                ):
                    return True
                else:
                    return False

        return False

    def getFirstSelect(self, expression):
        """Traverses a UNION to find the first SELECT statement."""
        while isinstance(expression, Union):
            expression = expression.this  # Move to the left side of the UNION

        return expression if isinstance(expression, Select) else None

    def transformUnion(self, expression):

        if expression.args.get("distinct"):
            expression.set("distinct", False)

            # Get the first SELECT statement in the UNION
            query = self.getFirstSelect(expression)

            if query is None:
                raise Exception("No SELECT statement found in UNION")

            query = self.addAliasToFunc(query)

            columns = []
            for col in query.expressions:
                if isinstance(col, Alias) and col.alias != "cntprov":
                    columns.append(col.this)
                elif isinstance(col, Column) and col.this != "cntprov":
                    columns.append(col)

            subquery_alias = "_un"

            group_by_columns = [
                Column(this=col.alias_or_name, table=subquery_alias) for col in columns
            ]

            sum_col = Sum(this=Column(this="cntprov", Table="_un"))

            columns.append(Alias(this=sum_col, alias="cntprov"))

            # Wrap the UNION ALL in a subquery with aliases
            subquery = Subquery(
                this=expression.copy(),
                alias=Alias(this=subquery_alias),
            )

            # Build a SELECT * FROM subquery GROUP BY all columns
            select_group_by = Select().from_(subquery)

            # Add SELECT expressions and GROUP BY clause
            select_group_by.set("expressions", columns)
            select_group_by.set("group", Group(expressions=group_by_columns))

            # Replace the original UNION with the new SELECT-GROUP BY block
            query = select_group_by

            return query
        else:
            if not isinstance(expression.left, Select):

                modified_union = Union(
                    this=self.transformUnion(expression.left),
                    expression=expression.right,
                )
                modified_union.set("distinct", False)

                return modified_union
            else:
                return expression

    def modifyQuery(self, expression):
        if isinstance(expression, Union):
            self.modifyQuery(expression.left)
            self.modifyQuery(expression.right)

            # If the union is the top-most one, replace it
            if not isinstance(expression.parent, Union):
                print("Union Query: ", expression.sql())
                print("Transforming Union Query...")

                new_expr = self.transformUnion(expression)

                print("Modified Union Query: ", new_expr.sql())

                # Replace in the parent node properly
                if expression.parent:
                    for key, value in expression.parent.args.items():
                        if value is expression:
                            expression.parent.set(key, new_expr)
                            break
                else:
                    return new_expr
            # expression = transformUnion(expression)
            # print(f"Union Query: {expression.sql()}")
        elif isinstance(expression, Select):
            from_clause = expression.args.get("from")
            if from_clause:
                if isinstance(from_clause.this, Subquery):
                    #        print(from_clause.this)
                    self.modifyQuery(from_clause.this.this)
            joins = expression.args.get("joins", [])
            for join in joins:
                join_table = join.this
                if isinstance(join_table, Subquery):
                    #        print(join_table)
                    self.modifyQuery(join_table.this)

            if expression.args.get("distinct"):
                expression = self.transformDistinct(expression)
            # print("Here", expression)
            if not any(
                isinstance(expr, Alias) and expr.alias == "cntprov"
                for expr in expression.expressions
            ) and not any(
                isinstance(expr, Column) and expr.this == "cntprov"
                for expr in expression.expressions
            ):
                column_mul = self.getSubqueries(expression)
                if self.hasGroupByOrAggregation(expression):
                    if column_mul:
                        count_column = Alias(
                            this=Sum(this=column_mul),
                            alias="cntprov",
                        )
                    else:
                        count_column = Alias(this=Count(this=Star()), alias="cntprov")
                else:
                    if column_mul:
                        count_column = column_mul
                    else:
                        count_column = Alias(
                            this=Column(this=Literal.number(1)), alias="cntprov"
                        )
                expression.expressions.append(count_column)

        return expression

    def getSubqueries(self, expression):
        from_clause = expression.args.get("from")
        subqueries = []
        if from_clause:
            if isinstance(from_clause.this, Subquery):
                alias = (
                    from_clause.this.alias_or_name
                )  # Returns alias if exists, or empty string
                if alias:
                    subqueries.append(alias)
        joins = expression.args.get("joins", [])
        for join in joins:
            join_table = join.this
            if isinstance(join_table, Subquery):
                alias = join_table.alias_or_name
                if alias:
                    subqueries.append(alias)

        if len(subqueries) == 0:
            return None
        else:
            # Initialize the multiplication expression
            multiplication_expr = Column(table=subqueries[0], this="cntprov")

            for subquery in subqueries[1:]:
                multiplication_expr = Mul(
                    this=multiplication_expr,
                    expression=Column(table=subquery, this="cntprov"),
                )
            return multiplication_expr
