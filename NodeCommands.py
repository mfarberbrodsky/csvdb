import os
import shutil
import json
import csv
from Schema import Schema
from order_by import OrderBy


class NodeCreate:
    def __init__(self, if_not_exists, table_name, fields, as_select):
        self.if_not_exists = if_not_exists
        self.table_name = table_name
        self.fields = fields
        self.as_select = as_select

    def execute(self, rootdir):
        if os.path.isdir(os.path.join(rootdir, self.table_name)):
            if not self.if_not_exists:
                raise ValueError("Table {} already exists.".format(self.table_name))
            with open(os.path.join(rootdir, self.table_name, 'table.json'), 'r') as f_schema:
                schema = json.load(f_schema)
                if schema.get('schema') != self.fields:
                    raise ValueError("Existing schema in table {} doesn't match.".format(self.table_name))
        elif self.fields:
            os.mkdir(os.path.join(rootdir, self.table_name))
            with open(os.path.join(rootdir, self.table_name, 'table.json'), 'w') as f_schema:
                json.dump({'schema': self.fields}, f_schema)
        elif self.as_select is not None:
            if self.as_select.outfile_name is not None:
                raise ValueError("Create as select does not support outfile.")
            original_fields = json.load(open(os.path.join(rootdir, self.as_select.table_name, 'table.json')))['schema']
            if self.as_select.field_list == ['*']:
                new_fields = original_fields
            else:
                # new_fields = [field[1] for field in self.as_select.field_list]
                # new_fields = [field for field in original_fields if field['field'] in self.as_select.field_list]
                original_field_to_type = {field['field']: field['type'] for field in original_fields}
                new_fields = [{'field': field[1], 'type': original_field_to_type[field[0]]} for field in
                              self.as_select.field_list]

            temp_create = NodeCreate(False, self.table_name, new_fields, None)
            temp_create.execute(rootdir)
            self.as_select.outfile_name = os.path.join(self.table_name, 'temp.csv')
            self.as_select.execute(rootdir)
            temp_load = NodeLoad(os.path.join(self.table_name, 'temp.csv'), self.table_name, 0)
            temp_load.execute(rootdir)
            os.remove(os.path.join(rootdir, self.table_name, 'temp.csv'))


class NodeDrop:
    def __init__(self, if_exists, table_name):
        self.if_exists = if_exists
        self.table_name = table_name

    def execute(self, rootdir):
        if not os.path.isdir(os.path.join(rootdir, self.table_name)):
            if not self.if_exists:
                raise ValueError("Table {} does not exist.".format(self.table_name))
        else:
            shutil.rmtree(os.path.join(rootdir, self.table_name))


class NodeLoad:
    def __init__(self, infile_name, table_name, ignore_lines):
        self.infile_name = infile_name
        self.table_name = table_name
        self.ignore_lines = ignore_lines

    def execute(self, rootdir):
        if not os.path.isdir(os.path.join(rootdir, self.table_name)):
            raise ValueError("Table {} should be created before loading data.".format(self.table_name))
        if not os.path.isfile(os.path.join(rootdir, self.infile_name)):
            raise ValueError("Infile {} does not exist.".format(self.infile_name))
        with open(os.path.join(rootdir, self.infile_name), 'r') as infile:
            with open(os.path.join(rootdir, self.table_name, '{}.csv'.format(self.table_name)), 'w') as table:
                for i, line in enumerate(infile):
                    if i < self.ignore_lines:
                        continue
                    table.write(line)


class NodeSelect:
    def __init__(self, field_list, outfile_name, table_name, where, group_by_list, having, order_by_list):
        self.field_list = field_list
        self.outfile_name = outfile_name
        self.table_name = table_name
        self.where = where
        self.group_by_list = group_by_list
        self.having = having
        self.order_by_list = order_by_list

    def order_by(self, rootdir):
        order_obj = OrderBy(rootdir, self.field_list, self.table_name, self.order_by_list)
        order_obj.generate_temp_file()

    def where_to_func(self, schema):
        if self.where is None:
            return lambda row: True

        field_name, op, value = self.where
        i = schema.get_field_index(field_name)
        dic_func = {"=": (lambda x: x == str(value)), ">": (lambda x: x != '' and float(x) > value),
                    'is': (lambda x: x == ''), 'is not': (lambda x: x != ''),
                    "<": (lambda x: x != '' and float(x) < value), ">=": (lambda x: x != '' and float(x) >= value),
                    "<=": (lambda x: x != '' and float(x) <= value), "<>": (lambda x: x != '' and float(x) != value)}
        return lambda row: dic_func[op](row[i])

    def execute(self, rootdir):
        if not os.path.isdir(os.path.join(rootdir, self.table_name)):
            raise ValueError("Table {} does not exist.".format(self.table_name))

        schema = Schema(os.path.join(rootdir, self.table_name, 'table.json'))
        if self.field_list == ['*']:
            self.field_list = schema.get_all_field_names()
        else:
            self.field_list = [field[0] for field in self.field_list]

        where_func = self.where_to_func(schema)

        print(self.field_list)
        field_list_index = []
        for field in self.field_list:
            if isinstance(field, str):
                field_list_index.append(schema.get_field_index(field))
            elif isinstance(field, tuple):
                field_list_index.append(schema.get_field_index(field[1]))

        order = None
        if self.order_by_list:
            order = OrderBy(rootdir, self.field_list, self.table_name, self.order_by_list)
            order.generate_temp_file()

        if self.order_by_list:
            temp_file_name = os.listdir(os.path.join(rootdir, self.table_name, 'temp'))[0]
            table = open(os.path.join(rootdir, self.table_name, 'temp', temp_file_name), 'r')
        else:
            table = open(os.path.join(rootdir, self.table_name, '{}.csv'.format(self.table_name)), 'r')

        reader = csv.reader(table)
        if self.outfile_name is None:
            for row in reader:
                if row and where_func(row):
                    row = [row[i] for i in field_list_index]
                    print(','.join(row))
        else:
            with open(os.path.join(rootdir, self.outfile_name), 'w', newline='') as outfile:
                writer = csv.writer(outfile)
                for row in reader:
                    if row and where_func(row):
                        row = [row[i] for i in field_list_index]
                        writer.writerow(row)

        table.close()
