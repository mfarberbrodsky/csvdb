from Parser import Parser
import csv

command_parser = Parser("select title from movies order by title desc;")
command_node = command_parser.parse_command()
# print("Field list:", command_node.field_list)
# print("Outfile name:", command_node.outfile_name)
# print("Table name:", command_node.table_name)
# print("Where:", command_node.where)
# print("Group by list:", command_node.group_by_list)
# print("Having:", command_node.having)
# print("Order by list:", command_node.order_by_list)


command_node.order_by(".")