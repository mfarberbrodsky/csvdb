import os
import csv

from order_by import OrderBy

type_to_func = {'int': int, 'timestamp': int, 'float': float, 'varchar': str}
agg_to_func = {'min': min, 'max': max, 'sum': lambda result, x: result + x,
               'count': lambda result, x: result + 1}

class GroupBy:
    def __init__(self, rootdir, all_fields, selected_fields, table_name, group_by_list, having):
        self.rootdir = rootdir
        self.table_name = table_name
        self.selected_fields = selected_fields
        self.all_fields = all_fields
        self.group_by_list = []
        for field in group_by_list:
            for f in all_fields:
                if f.as_name == field:
                    self.group_by_list.append(f.index)
        self.order_by_list = [(x, 'asc') for x in group_by_list]

        self.aggregated_list = []
        for field in selected_fields:
            if field.agg is not None:
                if field.agg == 'count':
                    self.aggregated_list.append((agg_to_func[field.agg], field.index, (lambda x: 1)))
                else:
                    self.aggregated_list.append((agg_to_func[field.agg], field.index, type_to_func[field.result_type]))

        self.temp_file_dir = os.path.join(self.rootdir, self.table_name, 'temp')
        self.having = having

    def having_to_func(self):
        if self.having is None:
            return lambda row: True

        field, operator, value = self.having
        index = None
        if isinstance(field, tuple):  # Having on aggregator (without as)
            agg, field_name = field
            for f in self.selected_fields:
                if f.agg == agg and f.name == field_name:
                    index = f.index
        else:
            for f in self.selected_fields:
                if f.as_name == field:
                    index = f.index

        dic_func = {"=": (lambda x: x == value), ">": (lambda x: x != '' and x > value),
                    'is': (lambda x: x == ''), 'is not': (lambda x: x != ''),
                    "<": (lambda x: x != '' and x < value), ">=": (lambda x: x != '' and x >= value),
                    "<=": (lambda x: x != '' and x <= value), "<>": (lambda x: x != '' and x != value)}

        return lambda row: dic_func[operator](row[index])

    def initiate_res_fields(self, row):
        result = row[:]
        for agg_func, index, type_func in self.aggregated_list:
            result[index] = type_func(row[index])
        return result

    def generate_temp_file(self):  # Writes result to temp file
        order_by = OrderBy(self.rootdir, self.table_name, self.all_fields, self.order_by_list)
        order_by.generate_temp_file()
        order_by_result_file_name = os.path.join(order_by.temp_file_dir, os.listdir(order_by.temp_file_dir)[0])

        new_table_file = open(os.path.join(self.temp_file_dir, 'group_by_file'), 'w')

        having_func = self.having_to_func()

        with open(order_by_result_file_name, 'r') as order_by_result_file:
            reader = csv.reader(order_by_result_file)
            row = next(reader)
            new_res_fields = self.initiate_res_fields(row)

            for row in reader:
                if [row[i] for i in self.group_by_list] == [str(new_res_fields[i]) for i in self.group_by_list]:
                    for agg_func, index, type_func in self.aggregated_list:
                        new_res_fields[index] = agg_func(new_res_fields[index], type_func(row[index]))
                else:
                    if having_func(new_res_fields):
                        new_line = ','.join(str(x) for x in new_res_fields) + '\n'
                        new_table_file.write(new_line)
                    new_res_fields = self.initiate_res_fields(row)

            if having_func(new_res_fields):  # Last row
                new_line = ','.join(str(x) for x in new_res_fields) + '\n'
                new_table_file.write(new_line)

        os.remove(order_by_result_file_name)
        new_table_file.close()
