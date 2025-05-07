# Input Parameters
# 2 Queries - original and provenance
# Database type, URL, User, Password
# Validating subqueries - boolean


from DatabaseHelper.parser import ParserValidator
from DatabaseHelper.postgres import PostgreSQLConnector
from Validation.alternative import AlternativeValidation
from Validation.result import ResultValidation
from Validation.joint import JointValidation


import getpass
import argparse

import time


def main():

    db = None
    try:

        _sql = []
        # Open and read the file
        with open('100_mb.sql', 'r') as file:
            # Iterate through each line
            for line in file:
                # Strip whitespace and check if line is not empty
                if line.strip():
                    _sql.append(line.strip())

        _sql_prov = []
        
        # Open and read the file
        with open('100_mb_dataprov.sql', 'r') as file:
            # Iterate through each line
            for line in file:
                # Strip whitespace and check if line is not empty
                if line.strip():
                    _sql_prov.append(line.strip())

        db = PostgreSQLConnector("tpch_temp", "paulo", "paulo", "localhost")
    
        db.connect()
        parser = ParserValidator()

        for i in range(len(_sql)):
            print(f"Validating query {i + 1}")
            sql = _sql[i]

            aggColumns = parser.getAggColumns(sql)

            tablesNames = parser.getTablesNames(sql)

            sql = parser.transformQuery(sql)

            original, original_columns = db.fetch_results(sql.sql())

            _original_columns = parser.getAltColumns(parser.getOriginalColumns(sql), original_columns)

            sql_prov = _sql_prov[i]

            prov, prov_columns = db.fetch_results(sql_prov)

            result = ResultValidation(original, original_columns, prov, prov_columns)

            if result.validate(aggColumns):
                print("Results match")
            else:
                print("Results do not match")
                return


            jv = JointValidation(aggColumns, prov, prov_columns, tablesNames, db, _original_columns)

            try:
                start_time = time.time()

                if jv.validate():
                    end_time = time.time()
                    print(f"Provenance tokens obtain the value of the original columns in {end_time - start_time} seconds")
                    print("Provenance tokens obtain the value of the original columns")
                else:
                    print(
                        "Provenance tokens do not obtain the value of the original columns"
                    )
                    return


            
                av = AlternativeValidation(
                    original, original_columns, prov, prov_columns
                )

                start_time = time.time()
                if av.validate():
                    end_time = time.time()
                    print(f"Alternative validation passed in {end_time - start_time} seconds")
                    print("Alternative validation passed")
                else:
                    print("Alternative validation failed")

            except Exception as e:
                print(f"Error: {e}")
                return

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if db is not None:
            db.close()


if __name__ == "__main__":
    main()
