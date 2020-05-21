import math


def filter(record):
    for key in record.keys():
        value = record[key]
        if value is None or ((type(value) is int or type(value) is float) and math.isnan(value)):
            return False
        return True
