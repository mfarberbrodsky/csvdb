import os
import csv

from order_by import OrderBy
from Schema import Schema

agg_to_func = {'max': max, 'min': min, 'sum': lambda x,y: x+y, 'count': lambda x,y: x+1}

class GroupBy:
    def __init__(self, rootdir, field_list, table_name, group_by_list, having):
        self.rootdir = rootdir
        self.field_list = field_list
        # dictionary from index to aggregator name if exists
        self.table_name = table_name
        self.group_by_list = group_by_list
        self.having = having
        self.schema = Schema(os.path.join(rootdir, table_name, 'table.json'))

    def execute(self):
        orderObj = OrderBy(self.rootdir, self.field_list, self.table_name, self.group_by_list)
        orderObj.generate_temp_file()

        fileName = os.listdir(os.path.join(self.rootdir, self.table_name, 'temp'))[0]
        group_by_index_list = [Schema.get_field_index(field) for field in self.group_by_list]

        with open(os.path.join(self.rootdir, self.table_name, 'temp', fileName), 'r') as table:
            reader = csv.reader(table)
            prev_row = next(reader)
            prev_row = [prev_row[i] for i in group_by_index_list]
            agg_val_list = [0 for i in range(self.group_by_list)]

            for row in reader:
                if row == prev_row:
                    agg_val_list = [(agg_to_func[tmp_dict[i]])(agg_val_list[i], row[i]) for i in group_by_index_list if i in tmp_dict else agg_val_list[i]]
