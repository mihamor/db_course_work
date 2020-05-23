import pandas as pd
import re
from db import db, client
import random
from filter import filter as row_filter
from datetime import datetime, date


def load_oil_data(file, rename, marker):
    print('load %s' % file)
    df = pd.read_csv(file)
    df = df.rename(index=str, columns=rename)
    df['marker'] = marker
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna()

    REQUIRED_FIELDS = ['date', 'price', 'marker']
    df = df[REQUIRED_FIELDS]
    df = df[df.apply(row_filter, axis=1)]
    db.insert_many(df.to_dict('records'))


if __name__ == '__main__':
    # db.drop()
    load_oil_data(file='./initial-data/brent-daily.csv',
              rename={
                  'Date': 'date',
                  'Price': 'price',
              },
              marker='Brent'
              )
    load_oil_data(file='./initial-data/wti-daily.csv',
              rename={
                  'Date': 'date',
                  'Price': 'price',
              },
              marker='WTI'
              )
    load_oil_data(file='./initial-data/mars-monthly.csv',
              rename={
                  'Date': 'date',
                  'Price': 'price',
              },
              marker='Mars US'
              )