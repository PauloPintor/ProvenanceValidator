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


def main():
    parser = argparse.ArgumentParser(description="Arguments:")

    # Adding arguments
    parser.add_argument(
        "--dbms",
        type=str,
        required=True,
        help="PosgreSQL, MySQL, Oracle, SQLServer, Trino, Cassandra, MongoDB",
    )
    parser.add_argument(
        "--u", metavar="URL", type=str, required=True, help="Database URL"
    )
    parser.add_argument(
        "--p", metavar="PORT", type=str, required=True, help="Database port"
    )
    parser.add_argument(
        "--d", metavar="DATABASE", type=str, required=True, help="Database name"
    )
    parser.add_argument(
        "--q", metavar="QUERY", type=str, required=True, help="Original Query"
    )
    parser.add_argument(
        "--qp", metavar="QUERYPROV", type=str, required=True, help="Query with Prov"
    )
    parser.add_argument(
        "--exc",
        metavar="EXTRACOMMANDS",
        type=str,
        required=False,
        help="Extra commands",
    )

    # Parse arguments
    args = parser.parse_args()

    # Prompt for username and password
    username = input("Username: ")
    password = getpass.getpass("Password: ")  # Hides input while typing

    db = None
    try:

        if args.dbms.lower() == "postgresql":
            db = PostgreSQLConnector(args.d, username, password, args.u, args.p)
            # TODO: Add other DBMS connectors
        if db is None:
            print("Invalid DBMS")
            return
        else:
            db.connect()
            parser = ParserValidator()

            sql = args.q

            aggColumns = parser.getAggColumns(sql)

            tablesNames = parser.getTablesNames(sql)

            sql = parser.transformQuery(args.q)

            if args.exc is not None:
                original, original_columns = db.fetch_results(sql.sql(), args.exc)
            else:
                original, original_columns = db.fetch_results(sql.sql())
            prov, prov_columns = db.fetch_results(args.qp)

            result = ResultValidation(original, original_columns, prov, prov_columns)

            if result.validate(aggColumns):
                print("Results match")
            else:
                print("Results do not match")
                return

            jv = JointValidation(aggColumns, prov, prov_columns, tablesNames, db)

            try:
                if jv.validate():
                    print("Provenance tokens obtain the value of the original columns")
                else:
                    print(
                        "Provenance tokens do not obtain the value of the original columns"
                    )
                    return

                av = AlternativeValidation(
                    original, original_columns, prov, prov_columns
                )
                if av.validate():
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
