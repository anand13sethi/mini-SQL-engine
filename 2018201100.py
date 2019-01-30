import sys
import csv
import os

import sqlparse

identifiers = []


def read_metadata():
    pass


def parse_query(query):
    parsed_query = sqlparse.parse(query)[0].tokens
    if str(parsed_query[-1]) != ";":
        print("[Query Error]: Invalid Query.\n")
        sys.exit(1)
    if sqlparse.sql.Statement(parsed_query).get_type() != 'SELECT':
        print("[Query Error]: Non - SELECT Query.\n")
        sys.exit(1)
    else:
        id = sqlparse.sql.IdentifierList(parsed_query).get_identifiers()
        for item in id:
            if str(item) != ';':
                identifiers.append(str(item))
        read_metadata()


if len(sys.argv) != 2:
    print("[System Error]: Invalid Number of Arguments.")
    print("Usage - python3 2018201100.py <query;>\n")
    sys.exit(1)

cmd_line_input = sys.argv[1]
parse_query(cmd_line_input)

