import quandl
import pandas as pd
from datetime import datetime
from filter import filter as row_filter
import time
import schedule
from db import db

quandl.ApiConfig.api_key = "TafKwssb151Bphsv7u-g"


def performEachDay():
  today = datetime.today()
  print("Checking brent oil price", today)
  todayPrice = quandl.get("FRED/DCOILBRENTEU", start_date=today)
  todayPrice['date'] = todayPrice.index

  df = todayPrice.rename(columns={'Value': 'price'})
  df['marker'] = "Brent"
  df['date'] = pd.to_datetime(df['date'], errors='coerce')
  df = df.dropna()

  REQUIRED_FIELDS = ['date', 'price', 'marker']
  df = df[REQUIRED_FIELDS]
  df = df[df.apply(row_filter, axis=1)]
  records = df.to_dict('records')
  print(df)

  if len(records) > 0:
    print('new records available, inserting...')
    db.insert_many(records)

if __name__ == '__main__':
  #initial check
  performEachDay()

  schedule.every().day.at("01:00").do(performEachDay,'It is 01:00, checking prices')

  while True:
    schedule.run_pending()
    time.sleep(60) # wait one minute

