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
            if i not in self.aggCols and col != "prov"
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

    def compareRows(self, row, rowProv, aggCols):

        rowProvTemp = list(rowProv)

        rowProvTemp = [value for i, value in enumerate(rowProvTemp) if i not in aggCols]

        rowProvTemp = tuple(rowProvTemp)

        return sorted(map(str, row)) == sorted(map(str, rowProvTemp))
