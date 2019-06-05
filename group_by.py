import os
import csv

from order_by import OrderBy
from Schema import Schema


class GroupBy:
    def __init__(self, rootdir, field_list, table_name, group_by_list, having):
        self.rootdir = rootdir
        self.schema = Schema(os.path.join(rootdir, table_name, 'table.json'))
        type_to_func = {'int': int, 'varchar': str, 'float': float}
        agg_to_func = {'max': lambda x, y: max(float(x), float(y)), 'min': lambda x, y: min(float(x), float(y)), 'sum': lambda x, y: float(x) + float(y), 'count': lambda x, y: x + 1}
        self.field_list = field_list
        self.agg_field_dict = {}
        for field in field_list:
            print("AAAAAAAAAAAAAAAAAAAAAAAAAA ", field)
            if isinstance(field[0], tuple):
                field_agg = field[0][0]
                field_name = field[0][1]
                field_type = self.schema.get_field_type(field_name)
                self.agg_field_dict[self.schema.get_field_index(field_name)] = lambda x, y: type_to_func[field_type](agg_to_func[field_agg](x, y))
            elif isinstance(field, tuple):
                field_agg = field[0]
                field_name = field[1]
                field_type = self.schema.get_field_type(field_name)
                self.agg_field_dict[self.schema.get_field_index(field_name)] = lambda x, y: type_to_func[field_type](agg_to_func[field_agg](x, y))
        for i, func in self.agg_field_dict.items():
            print (i,func)

        # dictionary from index to aggregator name if exists
        self.table_name = table_name
        self.group_by_list = group_by_list
        self.having = having


    def having_to_func(self):
        if self.having is None:
            return lambda row: True

        field_name, op, value = self.having
        i = self.schema.get_field_index(field_name)
        dic_func = {"=": (lambda x: x == str(value)), ">": (lambda x: x != '' and float(x) > value),
                    'is': (lambda x: x == ''), 'is not': (lambda x: x != ''),
                    "<": (lambda x: x != '' and float(x) < value), ">=": (lambda x: x != '' and float(x) >= value),
                    "<=": (lambda x: x != '' and float(x) <= value), "<>": (lambda x: x != '' and float(x) != value)}
        return lambda row: dic_func[op](row[i])

    def execute(self):
        order_by = [(a,'asc') for a in self.group_by_list]
        orderObj = OrderBy(self.rootdir, self.field_list, self.table_name, order_by)
        orderObj.generate_temp_file()

        fileName = os.listdir(os.path.join(self.rootdir, self.table_name, 'temp'))[0]

        field_index_list = []
        for field in self.field_list:
            if isinstance(field, str):
                field_index_list.append(self.schema.get_field_index(field))
            elif isinstance(field, tuple):
                field_index_list.append(self.schema.get_field_index(field[1]))

        group_by_index_list = [self.schema.get_field_index(field) for field in self.group_by_list]

        new_table_file = open(os.path.join(self.rootdir, self.table_name, 'group_by_file'), 'w')

        with open(os.path.join(self.rootdir, self.table_name, 'temp', fileName), 'r') as table:
            reader = csv.reader(table)
            new_res_fields = next(reader)

            for row in reader:
                if row == []:
                    continue

                if not self.having_to_func()(row):
                    continue

                if [row[i] for i in group_by_index_list] == [new_res_fields[i] for i in group_by_index_list]:
                    for i, func in self.agg_field_dict.items():
                        new_res_fields[i] = str(func(new_res_fields[i], row[i]))
                else:
                    new_line = [str(new_res_fields[i]) for i in field_index_list]
                    new_line = ",".join(new_line)
                    new_table_file.write(new_line + "\n")
                    new_res_fields = row
            new_line = [str(new_res_fields[i]) for i in field_index_list] #last row
            new_line = ",".join(new_line)
            new_table_file.write(new_line + "\n")
