import requests
import _json
import json
from bs4 import BeautifulSoup
import pandas as pd
import openpyxl
import time
# from datetime import datetime
import datetime

from sqlalchemy import create_engine
import sqlite3

headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/json;q=0.9,image/webp,*/*;q=0.8'
                }

conn = sqlite3.connect('./cfg/twseDB.db', check_same_thread=False)
conn.isolation_level = None
startDate = '20200721'
startDate2 = "20200701"
startDateObject = datetime.datetime.strptime(startDate, "%Y%m%d")
startDateObject2 = datetime.datetime.strptime(startDate2, "%Y%m%d")
numOfDays = 10
numOfMonths = 10

def storeStockData(startDateObject, numOfDays):
    global headers,conn
    date = (startDateObject + datetime.timedelta(days=-1))
    for d in range(1, numOfDays):
        date = (startDateObject+datetime.timedelta(days=-d)).strftime("%Y%m%d")
        urlfinal = 'https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date={t}'.format(t=date)
        dateString = (startDateObject+datetime.timedelta(days=-d)).strftime("%Y/%m/%d")
        resp = requests.get(urlfinal).json()
        time.sleep(3)
        df1 = pd.DataFrame()
        if(len(resp['creditList']) ==0):
            continue
        for i in range(len(resp['creditList'])):
            df1[i] = resp['creditList'][i]
        df1 = df1.T
        list = [dateString, dateString, dateString]
        df1['日期'] = list
        df1.columns = ["項目", "買進", "賣出", "現金(券)償還", "前日餘額", "今日餘額", '日期']
        sql = "REPLACE INTO twse (\"項目\", \"買進\", \"賣出\", \"現金(券)償還\", \"前日餘額\", \"今日餘額\", \"日期\") VALUES(?,?,?,?,?,?,?)"
        for index, row in df1.iterrows():
            args = (row[0], row[1].replace(',',''), row[2].replace(',',''), row[3].replace(',',''), row[4].replace(',',''), row[5].replace(',',''), row[6])
            if conn is not None:
                try:
                    cur = conn.cursor()
                    cur.execute(sql, args)
                    conn.commit()
                except Exception as e:
                    cur.execute("rollback")
                    print(e)


if __name__ == "__main__":
    storeStockData(startDateObject, numOfDays)
    conn.close()


