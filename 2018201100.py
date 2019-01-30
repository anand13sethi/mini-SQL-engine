import sys
import csv
import os
import sqlparse

identifiers = []
database_dict = {}


def read_metadata():
    if not os.path.exists('./files/metadata.txt'):
        print("[System Error]: Metadata File Not Found.\n")
        sys.exit(1)
    with open("./files/metadata.txt", "r") as file:
        flag = 0
        for line in file:
            line = line.strip()
            if line == "<begin_table>":
                flag = 1
                continue
            if flag:
                table = line
                database_dict[table] = []
                flag = 0
                continue
            if flag == 0 and line != "<end_table>":
                database_dict[table].append(line)
    

def process_query(query):
    # Todo - Check for aggregation functions
    if identifiers[3] in ['where', 'WHERE']:
        print("[Table Error]: No Table Specified.\n")
        sys.exit(1)
    tables_list = identifiers[3].strip().split(',')


def parse_query(query):
    if ';' in query:
        query = query.strip(';')
    else:
        print("[Query Error]: Invalid Query.\n")
        sys.exit(1)
    parsed_query = sqlparse.parse(query)[0].tokens
    if sqlparse.sql.Statement(parsed_query).get_type() != 'SELECT':
        print("[Query Error]: Non - SELECT Query.\n")
        sys.exit(1)
    else:
        id = sqlparse.sql.IdentifierList(parsed_query).get_identifiers()
        for item in id:
            if str(item) != ';':
                identifiers.append(str(item))
        if len(identifiers) < 4:
            print("[Query Error]: Invalid Query.\n")
            sys.exit(1)
        read_metadata()
        process_query(query)


if len(sys.argv) != 2:
    print("[System Error]: Invalid Number of Arguments.")
    print("Usage - python3 2018201100.py <query;>\n")
    sys.exit(1)

cmd_line_input = sys.argv[1]
parse_query(cmd_line_input)
