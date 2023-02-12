import csv
import sqlite3


def csv_two(columns, path, delimiter):
    with open(path) as csvfile:
        f = csv.DictReader(csvfile, fieldnames=columns, delimiter=delimiter)
        return list(f)


connection = sqlite3.connect(':memory:')
oltp = sqlite3.connect(':memory:')
