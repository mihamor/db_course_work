import pandas as pd
import re
from db import db
import random
from filter import filter as row_filter
from datetime import datetime, date


REQUIRED_FIELDS = ['date', 'price', 'marker', 'change']

def load_oil_data(file, rename_maps, marker, map_lambdas={}):
    print('load %s' % file)
    df = pd.read_csv(file)
    df = df.rename(index=str, columns=rename_maps)
    df['marker'] = marker
    for key, cb in map_lambdas.items():
        df[key] = df.apply(cb, axis=1)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['change'] = df['price'].pct_change()
    df = df.dropna()

    df = df[REQUIRED_FIELDS]
    df = df[df.apply(row_filter, axis=1)]
    db.insert_many(df.to_dict('records'))


if __name__ == '__main__':
    db.drop()
    load_oil_data(file='./csv-initial-data/brent-daily.csv',
              rename_maps={
                  'Date': 'date',
                  'Price': 'price',
              },
              marker='Brent'
              )
    load_oil_data(file='./csv-initial-data/wti-daily.csv',
              rename_maps={
                  'Date': 'date',
                  'Price': 'price',
              },
              marker='WTI'
              )
    load_oil_data(file='./csv-initial-data/mars-monthly.csv',
              rename_maps={
                  'Date': 'date',
                  'Price': 'price',
              },
              marker='Mars US'
              )