import logging
import re
import sympy

from Helper.mapTokens import mapTokens


class solvePolynomials:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def solveAggRows(self, row, aggCols, toDelete=[]):
        row = list(row)

        for colAgg in aggCols:
            if len(toDelete) > 0:
                for exp in toDelete:
                    pattern = r"[=<>!]+"
                    mathSymbol = re.findall(pattern, exp)
                    expTemp = exp.split(mathSymbol[0])

                    row[colAgg] = row[colAgg].replace(expTemp[0].strip(), "0")

                    if mathSymbol[0] + " 1" not in exp:
                        row[colAgg] = row[colAgg].replace(expTemp[1].strip(), "0")

            if "min" in row[colAgg]:
                return min(self.extract_numbers(row[colAgg]))
            elif "max" in row[colAgg]:
                return max(self.extract_numbers(row[colAgg]))
            else:
                col = row[colAgg].replace("⊗", "*").replace(" . ", " * ")

                col = re.sub(r"\+\S+", "+", col)

                mt = mapTokens()

                exp = mt.replace_words_with_fixed_number(col)

                exp = exp.replace("(0 )", "0")
                exp = self.replace_parentheses_with_one(exp)

                result = 0
                for expression in exp.split("+"):
                    result += eval(expression)

                row[colAgg] = result

        return tuple(row)

    def extract_numbers(self, expression):
        """
        Extracts all numeric values from the given string and converts them to floats.

        Args:
            expression (str): The input string.

        Returns:
            list: A list of extracted numbers as floats.
        """
        # Regex pattern to match numbers (including decimals)
        pattern = r"⊗\s*(\d+\.?\d*)"

        # Find all matches
        numbers = re.findall(pattern, expression)

        # Convert to float
        return [float(num) for num in numbers]

    def replace_parentheses_with_one(self, expression):
        while "(" in expression:  # Keep replacing until no parentheses remain
            expression = re.sub(r"\([^()]*\)", "1", expression)
        return expression

    def expandPolynomial(self, poly):
        poly = poly.replace("δ", "")

        mt = mapTokens()

        result, map = mt.replace_words_with_tokens(poly)

        if "⊗" in result:
            result = result.replace("⊗", "*")
        elif "." in result:
            result = result.replace(".", "*")

        if "⊕" in result:
            result = result.replace("⊕", "+")

        if "* (" not in result:
            return result, map
        else:
            expr = sympy.expand(result)

            # Regex pattern to find variables with exponents (e.g., x2**2)
            pattern = r"(\w+)\*\*(\d+)"

            # Replacement function to expand exponents
            def replace(match):
                base = match.group(1)  # Variable name (e.g., x2)
                exponent = int(match.group(2))  # Exponent value (e.g., 2)
                return "*".join([base] * exponent)  # Expand x2**2 → x2*x2

            # Replace using regex substitution
            expanded_expr = re.sub(pattern, replace, str(expr))

            return expanded_expr, map

    def solveSymbolicExpression(self, expression):
        try:
            pattern = r"[=<>!]+"
            mathSymbol = re.findall(pattern, expression)
            mt = mapTokens()
            exp = mt.replace_words_with_fixed_number(expression)
            isNotExp = mathSymbol[0] + " 1" in exp

            if isNotExp:
                exp = exp.replace("1k", "1")

            # Regex pattern: Match '.' only when it's surrounded by non-digits (words, spaces, etc.)
            pattern = r"(?<=\D)\.(?=\D)"  # Ensures dot is not part of a float

            # Replace matched dots with '*'
            exp = re.sub(pattern, "*", exp)
            # Split the equation into LHS and RHS
            lhs_str, rhs_str = exp.split(mathSymbol[0])
            rhs = 0
            if "min" in lhs_str:
                lhs = min(self.extract_numbers(lhs_str))

                if not isNotExp:
                    rhs_str = re.sub(r"\+\S+", "+", rhs_str)

                rhs_str = rhs_str.replace("⊗", " * ")
                rhs = eval(rhs_str.strip())
            elif "max" in lhs_str:
                lhs = max(self.extract_numbers(lhs_str))

                if not isNotExp:
                    rhs_str = re.sub(r"\+\S+", "+", rhs_str)

                rhs_str = rhs_str.replace("⊗", " * ")
                rhs = eval(rhs_str.strip())
            else:
                # Evaluate both sides
                lhs_str = lhs_str.replace("⊗", " * ").replace("⊕", "+")

                lhs_str = re.sub(r"\+\S+", "+", lhs_str)
                lhs = eval(lhs_str.strip())  # Strip whitespace and evaluate

                if not isNotExp:
                    rhs_str = re.sub(r"\+\S+", "+", rhs_str)

                rhs_str = rhs_str.replace("⊗", " * ")
                rhs = eval(rhs_str.strip())

            # Check equality
            if mathSymbol[0] == "=":
                return lhs == rhs
            elif mathSymbol[0] == ">":
                return lhs > rhs
            elif mathSymbol[0] == "<":
                return lhs < rhs
            elif mathSymbol[0] == ">=":
                return lhs >= rhs
            elif mathSymbol[0] == "<=":
                return lhs <= rhs
            elif mathSymbol[0] == "!=":
                return lhs != rhs

        except IndexError as e:
            print(f"Error: {e}")  # Output: Error: list index out of range
