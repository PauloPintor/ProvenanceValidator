import logging
import re
import itertools

from Helper.polynomials import solvePolynomials
from Helper.provenance import solveProvenance


class JointValidation:
    def __init__(self, aggCols, resultProv, columnsProv, tables, db):
        self.db = db
        self.resultProv = resultProv
        self.columnsProv = columnsProv
        self.tables = tables
        self.aggCols = aggCols
        self.logger = logging.getLogger(__name__)

    def validate(self):

        if "prov" not in self.columnsProv:
            self.logger.error("Provenance column not found")
            raise ValueError("Provenance column not found")

        idxProv = self.columnsProv.index("prov")
        self.aggCols.append(idxProv)
        columns = [
            col
            for i, col in enumerate(self.columnsProv)
            if i not in self.aggCols and col != "prov" and "_agg" not in col
        ]

        if len(columns) == 0:
            return True
        else:
            for row in self.resultProv:
                polynomial = row[self.columnsProv.index("prov")]
                sp = solvePolynomials()
                polynomial, map = sp.expandPolynomial(polynomial)

                joins = [t.strip() for t in polynomial.split("+")]

                if len(joins) > 0:
                    for join in joins:
                        join = join.replace("(", "").replace(")", "")
                        pattern = r"^\d+\*\s*"
                        join = re.sub(pattern, "", join)

                        tokens = join.split("*")

                        sp = solveProvenance(self.db)

                        if len(tokens) > len(self.tables):
                            self.logger.error(
                                "Number of tokens is greater than number of tables"
                            )
                            raise ValueError(
                                "Number of tokens is greater than number of tables"
                            )
                        elif len(tokens) < len(self.tables):
                            combinations = list(
                                itertools.combinations(self.tables, len(tokens))
                            )

                            for combination in combinations:
                                result, _ = sp.conjuntions(
                                    tokens, combination, columns, map
                                )

                                if len(result) == 1:
                                    result = self.removeProvSQLCol(result, columns)

                                    if not self.compareRows(
                                        result[0], row, self.aggCols
                                    ):
                                        self.logger.error(
                                            f"Rows are not equal {result[0]} != {row}"
                                        )
                                        raise Exception(
                                            f"Rows are not equal {result[0]} != {row}"
                                        )

                            self.logger.error(
                                "There are no combinations that return a result"
                            )
                            raise Exception(
                                "There are no combinations that return a result"
                            )
                        else:
                            result, _ = sp.conjuntions(
                                tokens, self.tables, columns, map
                            )

                            if len(result) == 1:

                                result = self.removeProvSQLCol(result, columns)

                                if not self.compareRows(result[0], row, self.aggCols):
                                    self.logger.error(
                                        f"Rows are not equal {result[0]} != {row}"
                                    )
                                    raise Exception(
                                        f"Rows are not equal {result[0]} != {row}"
                                    )
                            else:
                                self.logger.error(
                                    "Joint result is empty or returned more than one row"
                                )
                                raise Exception(
                                    "Joint result is empty or returned more than one row"
                                )
                else:
                    return False
        return True

    def removeProvSQLCol(self, result, columns):
        if "provsql" in columns:
            # newResult = list(result)
            # print(columns.index("provsql"))
            # newResult.pop(columns.index("provsql"))
            # return list(tuple(newResult))
            resultNew = []
            column_position = columns.index("provsql")
            for row in result:
                # Create new tuple excluding the column at column_position
                new_tuple = row[:column_position] + row[column_position + 1 :]
                resultNew.append(new_tuple)

            return resultNew
        else:
            return result

    def compareRows(self, row, rowProv, aggCols):

        rowProvTemp = list(rowProv)

        newAggCols = aggCols

        for col in aggCols:
            agg_col_name = self.columnsProv[col] + "_agg"
            if agg_col_name in self.columnsProv:
                newAggCols.append(self.columnsProv.index(agg_col_name))

        newAggCols.append(self.columnsProv.index("provsql"))

        rowProvTemp = [
            value for i, value in enumerate(rowProvTemp) if i not in newAggCols
        ]

        rowProvTemp = tuple(rowProvTemp)
        return sorted(map(str, row)) == sorted(map(str, rowProvTemp))
