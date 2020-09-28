import requests
import _json
import json
from bs4 import BeautifulSoup
import pandas as pd
import openpyxl
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
import datetime
import sqlite3

headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/json;q=0.9,image/webp,*/*;q=0.8'
                }

conn = sqlite3.connect('./cfg/twseDB.db', check_same_thread=False)
conn.isolation_level = None
startDate2 = "20200720"
startDateObject2 = datetime.datetime.strptime(startDate2, "%Y%m%d")
numOfMonths = 3

def storeStockIndex(startDateObject2, numOfMonths):
    global headers, conn
    for m in range(0, numOfMonths):
        date = (startDateObject2+relativedelta(months=-m)).strftime("%Y%m%d")
        urlfinal = 'https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=json&date={t}'.format(t=date)
        resp = requests.get(urlfinal).json()
        time.sleep(3)
        df1 = pd.DataFrame()
        if (len(resp['data'][m])) == 0:
            continue
        for i in range(len(resp['data'])):
            df1[i] = resp['data'][i]
        df1 = df1.T
        df1.columns = ["日期", "開盤指數", "最高指數", "最低指數", "收盤指數"]
        d = df1["日期"]
        for i in range(len(df1)):
            d.iloc[i] = d.iloc[i].replace(d.iloc[i][0:3], str(int(d.iloc[i][0:3]) + 1911))
        sql = "REPLACE INTO stockmarketindex (\"日期\", \"開盤指數\", \"最高指數\", \"最低指數\", \"收盤指數\") VALUES(?,?,?,?,?)"
        for index, row in df1.iterrows():
            args = (row[0], row[1].replace(',',''), row[2].replace(',',''), row[3].replace(',',''), row[4].replace(',',''))
            if conn is not None:
                try:
                    cur = conn.cursor()
                    cur.execute(sql, args)
                    conn.commit()
                except Exception as e:
                    cur.execute("rollback")
                    print(e)

if __name__ == "__main__":
    storeStockIndex(startDateObject2, numOfMonths)
    conn.close()
