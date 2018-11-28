import json
import sqltokenizer
import os


# text =  r"""create table if not exists movies (title varchar, year int, duration int, score float);"""

def create_table(command):
    path = os.path.dirname(os.path.realpath(__file__))

    schema = {'schema': []}
    data_dict = {}

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
                    data_dict[val] = val2
                tok, val = tok2, val2

                if val2 == ")":
                    break

        if tok == sqltokenizer.SqlTokenKind.EOF:
            break

    schema['schema'] = data_dict

    with open(newpath, 'wt') as f:
        json.dump(schema, f)
