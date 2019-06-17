import csv
import os
from Schema import Schema
import functools

chunk_size = 33554432


class OrderBy:
    def __init__(self, rootdir, table_name, all_fields, order_by_list, file_name=None):
        if file_name is None:
            file_name = table_name + ".csv"

        self.file_name = os.path.join(rootdir, table_name, file_name)
        if not os.path.isdir(os.path.join(rootdir, table_name, 'temp')):
            os.mkdir(os.path.join(rootdir, table_name, 'temp'))
        self.temp_file_dir = os.path.join(rootdir, table_name, 'temp')

        self.order_by_list = []
        for field, order in order_by_list:
            for f in all_fields:
                if f.as_name == field:
                    self.order_by_list.append((f.index, f.result_type, order))

        self.type_to_func = {'int': int, 'float': float, 'varchar': (lambda x: x)}  # Used for compare_rows

    # -1 if row1 < row2, 0 if row1 == row2, 1 if row1 > row2
    def compare_rows(self, row1, row2):
        if not row1 and not row2:
            return 0
        if not row1:
            return 1 if 'asc' else -1
        if not row2:
            return -1 if 'asc' else 1

        for index, field_type, order in self.order_by_list:
            if row1[index] == row2[index]:
                continue

            if row1[index] == '':
                result = True
            elif row2[index] == '':
                result = False
            else:
                result = self.type_to_func[field_type](row1[index]) < self.type_to_func[field_type](row2[index])

            if order == 'asc':
                return -1 if result else 1
            else:
                return 1 if result else -1
        return 0

    def sort_chunk(self, i):
        with open(os.path.join(self.temp_file_dir, str(i).zfill(3) + '.chunk'), 'r') as f:
            chunk_lines = list(csv.reader(f))
        with open(os.path.join(self.temp_file_dir, str(i).zfill(3) + '.chunk'), 'w') as f:
            chunk_lines.sort(key=functools.cmp_to_key(self.compare_rows))
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
            if self.compare_rows(line1, line2) <= 0:
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

    def generate_temp_file(self):  # Writes result to temp file in directory self.temp_file_dir
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
        files = os.listdir(self.temp_file_dir)

        if 'group_by_file' in files:
            files.remove('group_by_file')

        num_files = len(files)
        j = 1
        while num_files >= 2:
            for i in range(num_files // 2):
                self.merge_chunks(files[2 * i], files[2 * i + 1],
                                  'temp' + str(j) + str(i))
            j += 1
            files = os.listdir(self.temp_file_dir)
            num_files = len(files)
