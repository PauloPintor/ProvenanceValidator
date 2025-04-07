# Provenance Validator
A solution built in Python to validate Provenance Polynomials and Semimodules

## USAGE	
main.py [-h] --dbms DBMS --u URL --p PORT --d DATABASE --q QUERY --qp QUERYPROV [--exc EXTRACOMMANDS]

Arguments:

options:
  -h, --help           show this help message and exit
  --dbms DBMS          PosgreSQL, MySQL, Oracle, SQLServer, Trino, Cassandra, MongoDB
  --u URL              Database URL
  --p PORT             Database port
  --d DATABASE         Database name
  --q QUERY            Original Query
  --qp QUERYPROV       Query with Prov
  --exc EXTRACOMMANDS  Extra commands

Example:
    python main.py --dbms postgresql --u localhost --p 5431 --d postgres --q "SELECT avg(total_duration) total FROM te_madeira" --qp "SELECT *, aggregation_formula(total,'tm_prov') total_agg, formula(provenance(), 'tm_prov') as prov FROM(SELECT avg(total_duration) total FROM te_madeira) tm;" --exc "SET SEARCH_PATH TO public, provsql;"

## ACKNOWLEDGMENTS
This project was carried out as part of a research grant awarded by the Portuguese public agency for science, technology and innovation FCT - Foundation for Science and Technology - under the reference 2021.06773.BD. This work is partially funded by National Funds through the FCT under the Scientific Employment Stimulus - Institutional Call - CEECIN  - ST/00051/2018, and in the context of the projects UIDB/04524/2020 and UIDB/00127/2020.
