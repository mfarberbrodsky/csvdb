import csv
import os
from multiprocessing import Pool
from Schema import Schema

chunk_size = 32768
pool_size = os.cpu_count()

class OrderBy:
    def __init__(self, rootdir, field_list, table_name, order_by_list):
        self.rootdir = rootdir
        self.field_list = field_list
        self.file_name = os.path.join(rootdir, table_name, '{}.csv'.format(table_name))
        self.file_size = os.path.getsize(self.file_name)
        self.num_threads = os.cpu_count()
        if not os.path.isdir(os.path.join(rootdir, table_name, 'temp')):
            os.mkdir(os.path.join(rootdir, table_name, 'temp'))
        self.temp_file_name = os.path.join(rootdir, table_name, 'temp', '{}.csv'.format(table_name))
        self.temp_file_dir = os.path.join(rootdir, table_name, 'temp')
        self.order_by_list = order_by_list
        self.schema = Schema(os.path.join(rootdir, table_name, 'table.json'))

    def lexicographic_row(self, row):
        type_to_func = {'int': int, 'float': float}
        result = []
        print (self.order_by_list)
        for field, order in self.order_by_list:
            field_index = self.schema.get_field_index(field)
            field_type = self.schema.get_field_type(field)
            if field_type == 'varchar':
                if order == 'asc':
                    result.append([ord(ch) for ch in row[field_index]])
                else:
                    result.append([-ord(ch) for ch in row[field_index]] + [float('inf')])
            else:
                if row[field_index] == '':
                    result.append(float('-inf'))
                elif order == 'asc':
                    result.append(type_to_func[field_type](row[field_index]))
                else:
                    result.append(-type_to_func[field_type](row[field_index]))

        return result

    def sort_chunk(self, i):
        with open(os.path.join(self.temp_file_dir, str(i).zfill(3) + '.chunk'), 'r') as f:
            chunk_lines = list(csv.reader(f))
        with open(os.path.join(self.temp_file_dir, str(i).zfill(3) + '.chunk'), 'w') as f:
            chunk_lines.sort(key=self.lexicographic_row)
            csv.writer(f).writerows(chunk_lines)

    def merge_chunks(self, name1, name2, result_name):
        chunk1 = open(os.path.join(self.temp_file_dir, name1), 'r')
        chunk2 = open(os.path.join(self.temp_file_dir, name2), 'r')
        chunk1_reader = csv.reader(chunk1)
        chunk2_reader = csv.reader(chunk2)

        new_chunk = open(os.path.join(self.temp_file_dir, result_name), 'w')
        new_chunk_writer = csv.writer(new_chunk)

        line1 = next(chunk1_reader)
        line2 = next(chunk2_reader)

        while True:
            val1 = self.lexicographic_row(line1)
            val2 = self.lexicographic_row(line2)
            if val1 < val2:
                new_chunk_writer.writerow(line1)
                try:
                    line1 = next(chunk1_reader)
                except StopIteration:
                    break
            else:
                new_chunk_writer.writerow(line2)
                try:
                    line2 = next(chunk2_reader)
                except StopIteration:
                    break

        for line in chunk1_reader:
            new_chunk_writer.writerow(line)

        for line in chunk2_reader:
            new_chunk_writer.writerow(line)

        chunk1.close()
        os.remove(os.path.join(self.temp_file_dir, name1))
        chunk2.close()
        os.remove(os.path.join(self.temp_file_dir, name2))
        new_chunk.close()

    def generate_temp_file(self):
        # Separate file to chunks
        file = open(self.file_name, 'rb')
        num_chunks = 0
        last_chunk_end = 0
        current_chunk_file = open(os.path.join(self.temp_file_dir, str(num_chunks).zfill(3) + '.chunk'), 'wb')
        line = file.readline()
        while line != b'':
            current_chunk_file.write(line)
            if file.tell() > last_chunk_end + chunk_size:
                num_chunks += 1
                last_chunk_end = file.tell()
                current_chunk_file.close()
                current_chunk_file = open(os.path.join(self.temp_file_dir, str(num_chunks).zfill(3) + '.chunk'), 'wb')
            line = file.readline()
        current_chunk_file.close()
        file.close()
        num_chunks += 1

        # Sort each chunk
        for i in range(num_chunks):
            self.sort_chunk(i)

        # Merge chunks
        to_merge = []
        files = os.listdir(self.temp_file_dir)
        num_files = len(files)
        j = 1
        while num_files >= 2:
            for i in range(num_files // 2):
                self.merge_chunks(files[2 * i], files[2 * i + 1],
                                  'temp' + str(j) + str(i))
            j += 1
            files = os.listdir(self.temp_file_dir)
            num_files = len(files)
