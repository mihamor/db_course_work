import argparse
from pymongo import MongoClient
import random
from filter import filter as validate_vacancy_record


def generate(db):
    try:
        record = list(db.aggregate([{'$sample': {'size': 1}}]))[-1]
        for key in record.keys():
            if (type(record[key]) is int or type(record[key]) is float) and key != 'company_size':
                record[key] = (1.3 - 0.6 * random.random()) * record[key]
        record.pop('_id', None)
        if validate_vacancy_record(record):
            db.insert_one(record)
    except IndexError:
        print('Please, add some records to Mongo, before generate a new')
        exit(-1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate massive amount of pseudo random data')
    parser.add_argument('count', metavar='n', type=int,
                        help='count of records to generate (0 is eq INF)')

    args = parser.parse_args()

    client = MongoClient(host='localhost', port=27017)
    db = client.admin.cwdb
    if args.count > 0:
        print('Generate %i records' % args.count)
        for i in range(args.count):
            generate(db)
            if i > 0 and i % 10 == 0:
                print('Already generated %i records' % i)
    else:
        print('Generate infinite amount of records')
        print('Press ^С to stop')
        i = 0
        while True:
            i += 1
            generate(db)
            if i % 10 == 0:
                print('Already generated %i records' % i)
