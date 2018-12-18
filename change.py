import re
import csv
import time


def command_from_keyboard():
    cmd = ""
    while not cmd.endswith(";"):
        line = input()
        line = re.sub(r"--.*$", "", line)
        cmd += " " + line
    cmd = re.sub(r"\s+", " ", cmd)
    return cmd.lower()[1:]


def get_row_titles(a_reader):
    i = 0
    for row in a_reader:
        i += 1
        if i == 1:
            return row


def get_data(a_reader, ind):
    data = []
    for row in a_reader:
        data.append(row[ind])
    return data


try:
    with open('movies2.csv', 'r') as f:
        my_reader = csv.reader(f)
        t = get_row_titles(my_reader)
        while True:
            cmd = command_from_keyboard().split(" ")
            print(cmd)
            if cmd[1] in t:
                i = t.index(cmd[1])
                data = get_data(my_reader, i)
                print(data)

except Exception as Err:
    print(Err)
    print("d")
    time.sleep(50)
