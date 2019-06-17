import os
import shutil
import json
import csv
from Schema import Schema
from order_by import OrderBy
from group_by import GroupBy


class NodeCreate:
    def __init__(self, if_not_exists, table_name, fields, as_select):
        self.if_not_exists = if_not_exists  # Boolean
        self.table_name = table_name  # String
        self.fields = fields  # List of fields of {"field": field_name, "type": field_type}
        self.as_select = as_select  # NodeSelect object

    def execute(self, rootdir):
        if os.path.isdir(os.path.join(rootdir, self.table_name)):  # Table already exists
            if not self.if_not_exists:
                raise ValueError("Table {} already exists.".format(self.table_name))

            # Check that fields match existing schema
            with open(os.path.join(rootdir, self.table_name, 'table.json'), 'r') as f_schema:
                schema = json.load(f_schema)
                if schema.get('schema') != self.fields:
                    raise ValueError("Existing schema in table {} doesn't match.".format(self.table_name))
        elif self.fields:  # Create with fields
            os.mkdir(os.path.join(rootdir, self.table_name))
            with open(os.path.join(rootdir, self.table_name, 'table.json'), 'w') as f_schema:
                json.dump({'schema': self.fields}, f_schema)
        elif self.as_select is not None:  # Create as select
            if self.as_select.outfile_name is not None:
                raise ValueError("Create as select does not support outfile.")

            # Generate schema for new table
            original_fields = json.load(open(os.path.join(rootdir, self.as_select.table_name, 'table.json')))['schema']
            if self.as_select.field_list == ['*']:
                new_fields = original_fields
            else:
                original_field_to_type = {field['field']: field['type'] for field in original_fields}
                new_fields = []
                for field in self.as_select.field_list:
                    if field.result_type is None:
                        new_fields.append({'field': field.as_name, 'type': original_field_to_type[field.name]})
                    else:
                        new_fields.append({'field': field.as_name, 'type': field.result_type})
            temp_create = NodeCreate(False, self.table_name, new_fields, None)
            temp_create.execute(rootdir)

            # Execute select and load result
            self.as_select.outfile_name = os.path.join(self.table_name, 'temp.csv')
            self.as_select.execute(rootdir)
            temp_load = NodeLoad(os.path.join(self.table_name, 'temp.csv'), self.table_name, 0)
            temp_load.execute(rootdir)
            os.remove(os.path.join(rootdir, self.table_name, 'temp.csv'))


class NodeDrop:
    def __init__(self, if_exists, table_name):
        self.if_exists = if_exists  # Boolean
        self.table_name = table_name  # String

    def execute(self, rootdir):
        if not os.path.isdir(os.path.join(rootdir, self.table_name)):
            if not self.if_exists:
                raise ValueError("Table {} does not exist.".format(self.table_name))
        else:
            shutil.rmtree(os.path.join(rootdir, self.table_name))  # Recursively delete directory


class NodeLoad:
    def __init__(self, infile_name, table_name, ignore_lines):
        self.infile_name = infile_name  # String
        self.table_name = table_name  # String
        self.ignore_lines = ignore_lines  # Number

    def execute(self, rootdir):
        if not os.path.isdir(os.path.join(rootdir, self.table_name)):
            raise ValueError("Table {} should be created before loading data.".format(self.table_name))
        if not os.path.isfile(os.path.join(rootdir, self.infile_name)):
            raise ValueError("Infile {} does not exist.".format(self.infile_name))
        with open(os.path.join(rootdir, self.infile_name), 'r') as infile:
            with open(os.path.join(rootdir, self.table_name, '{}.csv'.format(self.table_name)), 'a') as table:
                for i, line in enumerate(infile):
                    if i < self.ignore_lines:
                        continue
                    table.write(line)
                table.write('\n')


class SelectField:
    def __init__(self, name, result_type=None, index=None, as_name=None, agg=None):
        self.name = name
        self.as_name = name
        if as_name is not None:
            self.as_name = as_name
        self.agg = agg
        self.result_type = None
        self.set_result_type(result_type)
        self.index = index

    def set_result_type(self, result_type):
        if self.agg == "avg":
            self.result_type = "float"
        elif self.agg == "count":
            self.result_type = "int"
        else:
            self.result_type = result_type

    def __repr__(self):  # For debugging
        result = self.name
        if self.agg is not None:
            result = self.agg + "(" + self.name + ")"
        if self.as_name != self.name:
            result = result + " as " + self.as_name
        result += " (type: " + str(self.result_type) + ")"
        result += " (index: " + str(self.index) + ")"
        return result


class NodeSelect:
    def __init__(self, field_list, outfile_name, table_name, where, group_by_list, having, order_by_list):
        self.all_fields = []
        self.field_list = field_list  # List of SelectField objects
        self.outfile_name = outfile_name  # String
        self.table_name = table_name  # String
        self.where = where  # (field_name, operator, value)
        self.group_by_list = group_by_list  # List of field names
        self.having = having  # (field_name, operator, value) or ((agg, field_name), operator, value)
        self.order_by_list = order_by_list  # List of tuples of (field_name, order_type), order_type is 'asc' or 'desc'

    def order_by(self, rootdir):
        order_obj = OrderBy(rootdir, self.field_list, self.table_name, self.order_by_list)
        order_obj.generate_temp_file()

    def where_to_func(self, schema):
        if self.where is None:
            return lambda row: True

        field_name, op, value = self.where
        for f in self.all_fields:
            if f.as_name == field_name:
                index = f.index
                type = f.result_type

        type_to_func = {'int': int, 'timestamp': int, 'float': float, 'varchar': str}
        type_func = lambda x: type_to_func[type](x) if x != '' else ''

        dic_func = {"=": (lambda x: x == value), ">": (lambda x: x != '' and x > value),
                    'is': (lambda x: x == ''), 'is not': (lambda x: x != ''),
                    "<": (lambda x: x != '' and x < value), ">=": (lambda x: x != '' and x >= value),
                    "<=": (lambda x: x != '' and x <= value), "<>": (lambda x: x != '' and x != value)}

        return lambda row: dic_func[op](type_func(row[index]))

    def execute(self, rootdir):
        if not os.path.isdir(os.path.join(rootdir, self.table_name)):
            raise ValueError("Table {} does not exist.".format(self.table_name))

        schema = Schema(os.path.join(rootdir, self.table_name, 'table.json'))
        self.all_fields = [
            SelectField(field, result_type=schema.get_field_type(field), index=schema.get_field_index(field)) for field
            in schema.get_all_field_names()]

        if self.field_list[0] == "*":
            self.field_list = self.all_fields
        for field in self.field_list:
            field.set_result_type(schema.get_field_type(field.name))
            field.index = schema.get_field_index(field.name)

        for i, field in enumerate(self.all_fields):
            for field2 in self.field_list:
                if field.name == field2.name:
                    self.all_fields[i] = field2

        where_func = self.where_to_func(schema)

        file_name = '{}.csv'.format(self.table_name)
        group = None
        if self.group_by_list:
            group = GroupBy(rootdir, self.all_fields, self.field_list, self.table_name, self.group_by_list, self.having)
            group.generate_temp_file()
            file_name = os.path.join('temp', 'group_by_file')

        order = None
        if self.order_by_list:
            order = OrderBy(rootdir, self.table_name, self.all_fields, self.order_by_list, file_name)
            order.generate_temp_file()
            temp_file_name = os.listdir(os.path.join(rootdir, self.table_name, 'temp'))[0]
            table = open(os.path.join(rootdir, self.table_name, 'temp', temp_file_name), 'r')
        else:
            table = open(os.path.join(rootdir, self.table_name, file_name), 'r')

        reader = csv.reader(table)
        if self.outfile_name is None:
            i = 0
            for row in reader:
                if i >= 200:
                    break
                if row and where_func(row):
                    row = [row[field.index] for field in self.field_list]
                    print(','.join(row))
                i += 1
        else:
            with open(os.path.join(rootdir, self.outfile_name), 'w', newline='') as outfile:
                writer = csv.writer(outfile)
                for row in reader:
                    if row and where_func(row):
                        row = [row[field.index] for field in self.field_list]
                        writer.writerow(row)

        table.close()
        if order is not None:
            shutil.rmtree(order.temp_file_dir)
        elif group is not None:
            shutil.rmtree(group.temp_file_dir)
