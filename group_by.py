import os
import csv

from order_by import OrderBy
from Schema import Schema



class GroupBy:
    def __init__(self, rootdir, field_list, table_name, group_by_list, having):
        self.rootdir = rootdir
        self.schema = Schema(os.path.join(rootdir, table_name, 'table.json'))
        agg_to_func = {'max': max, 'min': min, 'sum': lambda x, y: x + y, 'count': lambda x, y: x + 1}
        self.field_list = field_list
        self.agg_field_dict = {}
        for field in field_list:
            if isinstance(field[0], tuple):
                field_agg = field[0][0]
                field_name = field[0][1]
                self.agg_field_dict[self.schema.get_field_index(field_name)] = agg_to_func[field_agg]

        # dictionary from index to aggregator name if exists
        self.table_name = table_name
        self.group_by_list = group_by_list
        self.having = having


    def execute(self):
        orderObj = OrderBy(self.rootdir, self.field_list, self.table_name, self.group_by_list)
        orderObj.generate_temp_file()

        fileName = os.listdir(os.path.join(self.rootdir, self.table_name, 'temp'))[0]
        field_index_list = [self.schema.get_field_index(field) for field in self.field_list]
        group_by_index_list = [self.schema.get_field_index(field) for field in self.group_by_list]

        with open(os.path.join(self.rootdir, self.table_name, 'temp', fileName), 'r') as table:
            reader = csv.reader(table)
            new_res_fields = next(reader)
            #prev_row = next(reader)
            #prev_row = [prev_row[i] for i in group_by_index_list]
            #agg_val_list = [prev_row[i] for i in self.agg_field_dict]

            for row in reader:
                #if [row[i] for i in group_by_index_list] == prev_row:
                if [row[i] for i in group_by_index_list] == [new_res_fields[i] for i in group_by_index_list]:
                    for i, func in self.agg_field_dict.items():
                        #agg_val_list[i] = func(agg_val_list[i], row[i])
                        new_res_fields[i] = func(new_res_fields[i], row[i])
                else:
                    new_line = [str(new_res_fields[i]) for i in field_index_list]
                    new_line = " ".join(new_line)
                    if self.having is None:
                        print (new_line)
                    #elif new_res_fields[self.schema.get_field_index(self.having[0])] (op) (value):
                     #   print (new_line)
                    new_res_fields = row