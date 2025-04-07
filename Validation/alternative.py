import logging
import re

from Helper.mapTokens import mapTokens


class AlternativeValidation:
    def __init__(self, result, columns, resultProv, columnsProv):
        self.result = result
        self.columns = columns
        self.resultProv = resultProv
        self.columnsProv = columnsProv
        self.logger = logging.getLogger(__name__)

    def validate(self):

        if "prov" not in self.columnsProv:
            self.logger.error("Provenance column not found")
            raise ValueError("Provenance column not found")

        idx = self.columnsProv.index("prov")
        mt = mapTokens()
        for i, row in enumerate(self.resultProv):
            polynomial = row[idx]
            polynomial = polynomial.replace("δ", "")
            polynomial = polynomial.replace("⊗", "*").replace("⊕", "+")
            try:
                # Your code that may trigger recursion error
                result = eval(mt.replace_words_with_fixed_number(polynomial))
            except RecursionError:
                result = 0
                for exp in polynomial.split("+"):
                    exp = mt.remove_unmatched_parentheses(exp)
                    result += eval(mt.replace_words_with_fixed_number(exp))

            if isinstance(self.result[i][self.columns.index("cntprov")], str):
                # Find the first number (integer or decimal) in the string
                number = 0
                match = re.search(
                    r"\d+(?:\.\d+)?", self.result[i][self.columns.index("cntprov")]
                )
                if match:
                    number = float(match.group())

                return result == eval(str(number))
            else:

                return result == eval(
                    str(self.result[i][self.columns.index("cntprov")])
                )

        return False
