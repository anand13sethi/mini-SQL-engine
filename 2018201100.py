import sys
import csv
import os
import sqlparse
from collections import OrderedDict

identifiers = []
database_dict = {}
is_distinct = False

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


def validate_columns(column_list, table_list):
    cols_dict = {}
    for table in table_list:
        for col_list in database_dict[table.strip()]:
            for col in col_list:
                if col in list(cols_dict.keys()):
                    cols_dict[col] = 2
                else:
                    cols_dict[col] = 1

    for col in column_list:
        if cols_dict[col] == 2:
            print("[Attribute Error]: Ambigous Attribute.\n")
            sys.exit(1)

    valid_cols = {}
    offset = 0
    for table in table_list:
        for col in column_list:
            if col not in list(valid_cols.keys()):
                valid_cols[col] = -1
            if col in database_dict[table.strip()]:
                index = database_dict[table.strip()].index(col)
                valid_cols[col] = index + offset
        offset += len(database_dict[table.strip()])
    if -1 in list(valid_cols.values()):
        return -1
    else:
        return valid_cols

def check_valid_agg_syntax(attribute_list):
    agg_funcs = ['sum', 'max', 'min', 'avg']
    valid = True
    agg_dict = {}
    for item in attribute_list.split(", "):
        if len(item) > 3 and item[0:3] in agg_funcs:
            agg_dict[item[4:-1]] = item[0:3]
        else:
            valid = False
            break
    if not valid:
        return valid
    else:
        return agg_dict


def max_agg_func(col_name, table_name):
    if not os.path.exists('./files/' + table_name + ".csv"):
        print("[Table Error]: No Table Found.\n")
        sys.exit()
    col_index = database_dict[table_name].index(col_name)
    with open('./files/' + table_name + ".csv") as file:
        row = []
        for line in file:
            row.append(line.strip().split(",")[col_index])
    big = float('-inf')
    for item in row:
        if int(item) > big:
            big = int(item)
    return big

def min_agg_func(col_name, table_name):
    if not os.path.exists('./files/' + table_name + ".csv"):
        print("[Table Error]: No Table Found.\n")
        sys.exit()
    col_index = database_dict[table_name].index(col_name)
    with open('./files/' + table_name + ".csv") as file:
        row = []
        for line in file:
            row.append(line.strip().split(",")[col_index])
    small = float('inf')
    for item in row:
        if int(item) < small:
            small = int(item)
    return small

def sum_agg_func(col_name, table_name):
    if not os.path.exists('./files/' + table_name + ".csv"):
        print("[Table Error]: No Table Found.\n")
        sys.exit()
    col_index = database_dict[table_name].index(col_name)
    with open('./files/' + table_name + ".csv") as file:
        row = []
        for line in file:
            row.append(line.strip().split(",")[col_index])
    summation = 0
    for item in row:
        summation += int(item)
    return summation

def avg_agg_func(col_name, table_name):
    if not os.path.exists('./files/' + table_name + ".csv"):
        print("[Table Error]: No Table Found.\n")
        sys.exit()
    col_index = database_dict[table_name].index(col_name)
    with open('./files/' + table_name + ".csv") as file:
        row = []
        for line in file:
            row.append(line.strip().split(",")[col_index])
    avg = 0
    for item in row:
        avg += int(item)
    return avg/len(row) 


def project_join_query(table, join_cols):
    join_dict = OrderedDict()
    for item in join_cols:
        table_name, col_name = item.split('.')
        join_dict[table_name] = col_name
    indices = []
    ignore_col = [list(join_dict.keys())[1], join_dict[list(join_dict.keys())[1]]]
    offset = 0
    for table_name in join_dict.keys():
        indices.append(database_dict[table_name].index(join_dict[table_name]) + offset)
        offset += len(database_dict[table_name])
    
    temp_table = []
    for row in table:
        if row[indices[0]] == row[indices[1]]:
            row.pop(indices[1])
            temp_table.append(row)

    header = []
    flag = 0
    for key in database_dict:
        if key == ignore_col[0]:
            flag = 1
        for item in database_dict[key]:
            if flag and item == ignore_col[1]:
                continue
            else:
                header.append(key + '.' + item)
    print(",".join(header) + "\n")
    for row in temp_table:
        for item in row:
            if row.index(item) == len(row)-1:
                print(str(item), end = "")
            else:
                print(str(item) + ",", end = "")
        print()



def project_aggregation(agg_dict, table_name):
    valid = True
    for key in list(agg_dict.keys()):
        if key not in database_dict[table_name]:
            valid = False
            break
    if valid:
        final_table = []
        header = []
        for key in agg_dict:
            if agg_dict[key].lower() == 'max':
                temp_table = max_agg_func(key, table_name)
                final_table.append(str(temp_table))
                header.append(table_name + "." + key)
            elif agg_dict[key].lower() == 'min':
                temp_table = min_agg_func(key, table_name)
                final_table.append(str(temp_table))
                header.append(table_name + "." + key)
            elif agg_dict[key].lower() == 'sum':
                temp_table = sum_agg_func(key, table_name)
                final_table.append(str(temp_table))
                header.append(table_name + "." + key)
            else:
                temp_table = avg_agg_func(key, table_name)
                final_table.append(str(temp_table))
                header.append(table_name + "." + key)
        print("\t".join(header))
        print("\t\t".join(final_table))
    else:
        print("[Attribute Error]: Invalid Arguments.\n")
        sys.exit(1)


def project_some_cols(index_list, table, table_list):
    header = []
    for col in list(index_list.keys()):
        for table_name in table_list:
            if col in database_dict[table_name.strip()]:
                header.append(table_name + '.' + col)
                break
    print("\t".join(header) + "\n")

    if is_distinct:
        x = str()
        distinct_set = set()
        for row in table:
            for index in index_list.values():
                x += (str(row[index]) + "\t\t")
            distinct_set.add(x)
            x = str()
        for item in distinct_set:
            print(item)

    else:    
        for row in table:
            for index in index_list.values():
                print(str(row[index]) + "\t\t", end = "")
            print()


def project_table(table, header):
    for col_name in header:
        print(col_name + "\t", end = "")
    print()
    for row in table:
        for item in row:
            print(str(item) + ",\t", end = "")
        print()
    print()


def make_table(table_list):
    table_name = table_list + '.csv'
    if not os.path.exists('./files/'+table_name):
        return -1
    result = []
    with open('./files/'+table_name, "r") as fopen:
        for line in fopen:
            result.append(list(map(int, line.strip().split(','))))
    return result


def cross_product(table_list):
    table = make_table(table_list[0].strip())
    if table == -1:
        return -1
    for item in range(1, len(table_list)):
        temp_table = []
        next_table = make_table(table_list[item].strip())
        if next_table == -1:
            return -1
        for x in range(len(table)):
            for y in range(len(next_table)):
                temp_table.append(table[x] + next_table[y])
        table = temp_table

    return table


def process_query(query):
    # Todo - Check for aggregation functions
    if identifiers[3] in ['where', 'WHERE']:
        print("[Table Error]: No Table Specified.\n")
        sys.exit(1)
    table_list = identifiers[3].strip().split(',')

    if len(table_list) == 1:
        table = make_table(table_list[0])
    else:
        table = cross_product(table_list)
    
    if table == -1:
        print("[Table Error]: No Table Found.\n")
        sys.exit(1)

    if '*' in identifiers[1] and len(identifiers[1].split(', ')) == 1:
        if 'where' in identifiers[4]:
            valid_query = True
            join_cols = identifiers[4].strip().split()[1].split('=')
            for item in join_cols:
                table_name, col_name = item.split('.')
                if col_name not in database_dict[table_name]:
                    valid_query = False
                    break
            project_join_query(table, join_cols)
        else:
            header = []
            for table_name in table_list:
                for column in database_dict[table_name.strip()]:
                    header.append(table_name + "." + column + ",")
            project_table(table[:], header)

    elif '*' not in identifiers[1] and len(identifiers[1].split(", ")) >= 1:
        if '(' not in identifiers[1]:
            column_list = identifiers[1].split(", ")
            index_list = validate_columns(column_list, table_list)
            if index_list == -1:
                print("[Attribute Error]: Invalid Arguments.\n")
                sys.exit(1)
            else:
                project_some_cols(index_list, table, table_list)
        else:
            agg = check_valid_agg_syntax(identifiers[1])
            if agg is not False:
                project_aggregation(agg, table_list[0].strip())
            else:
                return -1
    else:
        return -1


def parse_query(query):
    global is_distinct
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
        if 'distinct' in identifiers:
            is_distinct = True
            identifiers.pop(1)
        if len(identifiers) < 4:
            print("[Query Error]: Invalid Query.\n")
            sys.exit(1)
        read_metadata()
        status = process_query(query)
        if status == -1:
            print("[Query Error]: Invalid Query.")
            sys.exit(1)


if len(sys.argv) != 2:
    print("[System Error]: Invalid Number of Arguments.")
    print("Usage - python3 2018201100.py <query;>\n")
    sys.exit(1)

cmd_line_input = sys.argv[1]
parse_query(cmd_line_input)
