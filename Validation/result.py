from decimal import Decimal
import logging
import re
import pandas as pd
import numpy as np

from Helper.polynomials import solvePolynomials

import time

class ResultValidation:
    def __init__(self, result, columns, resultProv, columnsProv):
        self.result = result
        self.resultProv = resultProv
        self.columns = columns
        self.columnsProv = columnsProv
        self.logger = logging.getLogger(__name__)

    def validate(self, aggColumns):
        start_time = time.time()
        poly = solvePolynomials()
        _aggColumnsIndex = []
        newResult = []
        toDeleteExp = []
        try:
            indexProv = self.columnsProv.index("prov")
        except ValueError:
            self.logger.error("Column 'prov' not found in provenance query")
            raise ValueError("Column 'prov' not found in provenance query")

        try:
            indexCntProv = self.columns.index("cntprov")
        except ValueError:
            self.logger.error("Column 'cntprov' not found in original query")
            raise ValueError("Column 'cntprov' not found in original query")

        div_columns = {}
        for col in aggColumns:
            name = self.columnsProv[col]
            _aggColumns = [i for i, item in enumerate(self.columnsProv) if item == name]
            if len(_aggColumns) > 1:
                div_columns[name] = _aggColumns
            if len(_aggColumns) > 1:
                for _index in _aggColumns:
                    if _index not in aggColumns:
                        _aggColumnsIndex.append(_index)
                        

        aggColumns.extend(_aggColumnsIndex)

        for i, row in enumerate(self.resultProv):
            toDelete = False
            if re.search(r"\[.*\]", row[indexProv]):
                # Regular expression pattern
                pattern = r"\.[^.\[]*\[[^\]]*\]"

                symbolicExpressions = re.findall(pattern, row[indexProv])

                if len(symbolicExpressions) == 1:

                    match = re.search(r"\[([^\]]*)\]", symbolicExpressions[0])
                    if match is not None:
                        if poly.solveSymbolicExpression(match.group(1)):
                            row = list(row)
                            row[indexProv] = row[indexProv].replace(
                                symbolicExpressions[0], ""
                            )
                            row = tuple(row)
                        else:
                            toDelete = True
                elif len(symbolicExpressions) > 1:
                    row = list(row)
                    exp = []
                    for symbolicExpression in symbolicExpressions:

                        match = re.search(r"\[([^\]]*)\]", symbolicExpression)
                        if match is not None:
                            if not poly.solveSymbolicExpression(match.group(1)):
                                toDeleteExp.append(match.group(1))
                                row[indexProv] = row[indexProv].replace(
                                    symbolicExpression, "delete"
                                )
                                pattern = r"\](.*?)delete"
                                row[indexProv] = re.sub(pattern, "]", row[indexProv])
                            else:
                                exp.append(symbolicExpression)

                    for e in exp:
                        row[indexProv] = row[indexProv].replace(e, "")
                    row = tuple(row)
                    if len(exp) == 0:
                        toDelete = True

            if not toDelete:
                if len(aggColumns) > 0:
                    newAggColumns = []
                    for col in aggColumns:
                        if self.columns[col] + "_agg" in self.columnsProv:
                            newAggColumns.append(
                                self.columnsProv.index(self.columnsProv[col] + "_agg")
                            )

                    if len(newAggColumns) > 0:
                        self.resultProv[i] = poly.solveAggRows(
                            row, newAggColumns, toDeleteExp
                        )
                        if not self.validateAggColumns(self.resultProv[i], aggColumns):
                            self.logger.error(
                                "Validation of aggregation columns failed"
                            )
                            raise ValueError("Validation of aggregation columns failed")
                    else:
                        self.resultProv[i] = poly.solveAggRows(
                            row, aggColumns, toDeleteExp, div_columns
                        )
                        
                newResult.append(self.resultProv[i])

        self.resultProv = newResult
        end_time = time.time()
        print(
            f"Validation of symbolic expressions and aggregation columns took {end_time - start_time} seconds"
        )
        if len(_aggColumnsIndex) > 0:
            for index in _aggColumnsIndex:
                self.columnsProv.pop(index)
        return self.compareResults()

    def validateAggColumns(self, result, aggColumns):
        for col in aggColumns:
            indexCol = self.columnsProv.index(self.columns[col])
            indexColProv = self.columnsProv.index(self.columnsProv[col] + "_agg")

            # Find the first number (integer or decimal) in the string
            number = 0
            match = re.search(r"\d+(?:\.\d+)?", result[indexCol])
            if match:
                number = float(match.group())

            if number != result[indexColProv]:
                return False
        return True

    def compareResults(self):
        start_time = time.time()
        self.result = [self.convert_decimals(row) for row in self.result]
        self.resultProv = [self.convert_decimals(row) for row in self.resultProv]

        resOriginal = pd.DataFrame(self.result, columns=self.columns)
        resProv = pd.DataFrame(self.resultProv, columns=self.columnsProv)

        filterCols = ["cntprov"]
        filterColsProv = ["prov"]

        resOriginal_filter = resOriginal.drop(columns=filterCols, errors="ignore")
        resProv_filter = resProv.drop(columns=filterColsProv, errors="ignore")

        resProv_filter = resProv_filter.loc[
            :, ~resProv_filter.columns.str.contains("_agg")
        ]
        resProv_filter = resProv_filter[resOriginal_filter.columns]

        # Get numeric and non-numeric columns
        numeric_cols = resOriginal_filter.select_dtypes(include=[np.number]).columns
        non_numeric_cols = resOriginal_filter.select_dtypes(exclude=[np.number]).columns

        # Convert numeric columns to float
        resOriginal[numeric_cols] = resOriginal_filter[numeric_cols].astype(float)
        resProv_filter[numeric_cols] = resProv_filter[numeric_cols].astype(float)

        if not numeric_cols.empty:
            resOriginal = resOriginal_filter.sort_values(
                by=list(numeric_cols)
            ).reset_index(drop=True)
            resProv_filter = resProv_filter.sort_values(
                by=list(numeric_cols)
            ).reset_index(drop=True)

            # Compare numeric columns using np.allclose()
            numeric_equal = np.allclose(
                resOriginal[numeric_cols], resProv_filter[numeric_cols], equal_nan=True
            )
        else:
            numeric_equal = True

        if not non_numeric_cols.empty:
            resOriginal_filter = resOriginal_filter.sort_values(
                by=list(non_numeric_cols)
            ).reset_index(drop=True)
            resProv_filter = resProv_filter.sort_values(
                by=list(non_numeric_cols)
            ).reset_index(drop=True)
            # Compare non-numeric columns using .equals()
            non_numeric_equal = resOriginal_filter[non_numeric_cols].equals(
                resProv_filter[non_numeric_cols]
            )
        else:
            non_numeric_equal = True

        end_time = time.time()
        print(
            f"Comparison of results took {end_time - start_time} seconds"
        )
        return numeric_equal and non_numeric_equal

    def getProvenanceResult(self):
        return self.resultProv

    def convert_decimals(self, value):
        """Recursively convert Decimal to float in SQL query results"""
        if isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, list):
            return [self.convert_decimals(v) for v in value]
        elif isinstance(value, tuple):
            return tuple(self.convert_decimals(v) for v in value)
        elif isinstance(value, dict):
            return {k: self.convert_decimals(v) for k, v in value.items()}
        else:
            return value
