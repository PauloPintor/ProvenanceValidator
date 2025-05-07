import logging
import itertools

from Helper.polynomials import solvePolynomials
from collections import defaultdict


class solveProvenance:

    def __init__(self, db):
        self.db = db
        self.logger = logging.getLogger(__name__)

    def conjuntions(self, tokens, tables, columns, map):

        reverse_dict = {v: k for k, v in map.items()}
        for t in tokens:
            if t.strip() not in reverse_dict:
                self.logger.error(f"Token {t} not found in map")
                raise ValueError(f"Token {t} not found in map")
            else:
                tokens[tokens.index(t.strip())] = reverse_dict[t.strip()]

        aliased_list, short_alias_list = self.generate_aliased_arrays(tables)

        # Generate all permutations of Set2
        permutations_of_set2 = itertools.permutations(tokens)

        # Pair each permutation of Set2 with Set1 element-wise
        all_combinations = [
            list(zip(short_alias_list, permutation))
            for permutation in permutations_of_set2
        ]

        # Generate conditions for each tuple group
        for group in all_combinations:
            condition_parts = [
                f"{table.split(' ')[1 if len(table.split(' ')) > 1 else 0]}.prov = '{prov}'"
                for table, prov in group
            ]
            #condition_parts = [
            #    f"{table}.prov = '{prov}'" for table, prov in group
            #]
            condition_str = " and ".join(condition_parts)

            statm = f"""
            SELECT  {(', ').join(columns)}
            FROM    {(', ').join(aliased_list)}  
            WHERE   {condition_str}
            """
            rows, colnames = self.db.fetch_results(statm)

            if rows:
                return rows, colnames
        return [], []

    def generate_aliased_arrays(self, table_list):
        count = defaultdict(int)  # Dictionary to track occurrences
        aliased_list = []
        short_alias_list = []

        for table in table_list:
            count[table] += 1  # Increment occurrence count

            if count[table] == 1:
                # First occurrence, keep original name
                aliased_list.append(table)
                short_alias_list.append(table)
            else:
                # Create alias for duplicate occurrence
                alias = f"{table[0]}{count[table] - 1}"  # Generate alias like "ps1"
                aliased_list.append(f"{table} as {alias}")
                short_alias_list.append(alias)

        return aliased_list, short_alias_list
