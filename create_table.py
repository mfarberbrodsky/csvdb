import json
import sqltokenizer
import os
import csv


# text =  r"""create table if not exists movies (title varchar, year int, duration int, score float);"""

def create_table(command):
    path = os.path.dirname(os.path.realpath(__file__))

    schema = {'schema': []}

    tokenizer = sqltokenizer.SqlTokenizer(command)

    while True:
        tok, val = tokenizer.next_token()

        if val == 'exists':
            tok, directory = tokenizer.next_token()

            newpath = os.path.join(path, directory)

            if not os.path.exists(newpath):
                os.mkdir(newpath)

            newpath = os.path.join(newpath, 'table.json')

        if val == "(":
            while True:
                tok2, val2 = tokenizer.next_token()
                if tok == sqltokenizer.SqlTokenKind.IDENTIFIER and tok2 == sqltokenizer.SqlTokenKind.KEYWORD:
                    schema['schema'].append({"field": val, "type": val2})
                tok, val = tok2, val2

                if val2 == ")":
                    break

        if tok == sqltokenizer.SqlTokenKind.EOF:
            break

    with open(newpath, 'wt') as f:
        json.dump(schema, f)


def load(command):
    tokenizer = sqltokenizer.SqlTokenizer(command)

    filename = ""
    tablename = ""
    ignore_lines = 0

    while True:
        tok, val = tokenizer.next_token()

        if tok == sqltokenizer.SqlTokenKind.KEYWORD and val == "infile":
            tok, filename = tokenizer.next_token()

        if tok == sqltokenizer.SqlTokenKind.KEYWORD and val == "table":
            tok, tablename = tokenizer.next_token()

        if tok == sqltokenizer.SqlTokenKind.KEYWORD and val == "ignore":
            tok, ignore_lines = tokenizer.next_token()

        if tok == sqltokenizer.SqlTokenKind.EOF:
            break

    path = os.path.dirname(os.path.realpath(__file__))

    with open(os.path.join(path, filename), "r") as file:
        with open(os.path.join(path, tablename, tablename + ".csv"), "w") as table:
            for i, line in enumerate(file):
                if i < ignore_lines:
                    continue
                table.write(line)


def select(command):
    tokenizer = sqltokenizer.SqlTokenizer(command)

    arg_list = []
    table_name = ""
    outfile = ""

    while True:
        tok, val = tokenizer.next_token()

        if tok == sqltokenizer.SqlTokenKind.KEYWORD and val == 'select':
            tok, val = tokenizer.next_token()
            while tok != sqltokenizer.SqlTokenKind.KEYWORD:
                if tok == sqltokenizer.SqlTokenKind.OPERATOR and val == "*":
                    arg_list = ["*"]
                    break
                if tok == sqltokenizer.SqlTokenKind.IDENTIFIER:
                    arg_list.append(val)
                tok, val = tokenizer.next_token()

        if tok == sqltokenizer.SqlTokenKind.KEYWORD and val == "outfile":
            tok, outfile = tokenizer.next_token()

        if tok == sqltokenizer.SqlTokenKind.KEYWORD and val == 'from':
            tok, table_name = tokenizer.next_token()

        if tok == sqltokenizer.SqlTokenKind.EOF:
            break

    schema = []
    with open(os.path.join(table_name, "table.json"), "r") as f:
        schema = json.load(f)["schema"]

    if arg_list == ["*"]:
        arg_list = [arg["field"] for arg in schema]


select(input())
