import requests
import _json
import json
from bs4 import BeautifulSoup
import openpyxl
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
import datetime
import sqlite3
import os
import psycopg2 as p
from psycopg2 import Error
import requests
from bs4 import BeautifulSoup
import pandas as pd
import openpyxl
import datetime
import locale
import string
import random


conn = None
back_day = 100
avg_day = 5
#list = ['指數%','融資%','融券%']
list = ['融資%','融券%']
filename = 'Feature.txt'


def __init__():
    global conn
    conn = sqlite3.connect('twseDB.db')
    print("Open database")



def start():
    global conn, back_day, avg_day, list, filename
    sqlcom = "select a.日期,a.收盤指數,b.前日餘額,b.今日餘額 from stockmarketindex a,twse b where a.日期 = b.日期 and b.項目='融資(交易單位)' order by a.日期 desc limit 600"
    sqlcom2 = "select 日期, 前日餘額, 今日餘額 from twse where 項目 = '融券(交易單位)'order by 日期 desc"
    df1 = pd.read_sql(sqlcom, con=conn)
    df2 = pd.read_sql(sqlcom2, con=conn)
    result = []
    # df1.columns = ["日期", "收盤指數", "前日餘額", "今日餘額"]
    df_analysis = pd.DataFrame(columns=["日期", "指數%", "融資%", "融券%"])
    for index, row in df1.iterrows():
        if (index + 1)<(len(df1)):
            row2 = df1.loc[index + 1]
            row3 = df2.loc[index]
            closing_index_prev = row2["收盤指數"]
            closing_index_now = row["收盤指數"]
            indexPercentage = round(((closing_index_now - closing_index_prev) / closing_index_now) * 100, 2)
            financePercentage = round(((row["今日餘額"] - row["前日餘額"]) / row["今日餘額"]) * 100, 2)
            margin_trading_percentage = round(((row3["今日餘額"] - row3["前日餘額"]) / row3["今日餘額"]) * 100, 2)
            temp = {}
            temp["日期"] = row["日期"]
            temp['指數%'] = indexPercentage
            temp['融資%'] = financePercentage
            temp['融券%'] = margin_trading_percentage
            # print(index,temp)
            df_analysis = df_analysis.append(temp, ignore_index=True)


    # print(df_analysis)

    for index, row in df_analysis.iterrows():
        if ((index + back_day) < len(df_analysis)) & ((index - avg_day) > 0):
            result.append(getData(index,df_analysis))
    conn.close()
    df_result = pd.DataFrame(result)
    df_one = pd.DataFrame()
    df_zero = pd.DataFrame()
    df_values = pd.DataFrame()
    for index, row in df_result.iterrows():
        if row[len(row)-1] == 1:
            df_one = df_one.append(row)
        else:
            df_zero = df_zero.append(row)
    if len(df_zero) > len(df_one):
        size = len(df_one)
        df_zero = df_zero.sample(size)
    else:
        size = len(df_zero)
        df_one = df_one.sample(size)
    df_values = df_values.append(df_zero)
    df_values = df_values.append(df_one)
    result = df_values.values.tolist()
    fp = open(filename, "w", encoding="utf-8")
    fp.write(str(result))
    fp.close()
    print(result)




def getData(index,df_analysis):
    global back_day,avg_day,list
    sum = 0
    feature = []
    for i in range(back_day, -1, -1):
        if (index + i) < len(df_analysis):
            row = df_analysis.loc[index + i]
            for item in list:
                feature.append(row[item])
    for i in range(1, avg_day, 1):
        row = df_analysis.loc[index + -i]
        sum = sum + float(row['指數%'])
    if (sum > 0):
        feature.append(1)
    else:
        feature.append(0)
    return feature




if __name__ == "__main__":
    __init__()
    start()

# def dateExist(date, days):
#     date = datetime.datetime.strptime(date, "%Y/%m/%d")
#     RealDateString = (date+datetime.timedelta(days=-days)).strftime("%Y/%m/%d")
#     while True:
#         for d in range(len(dfTWSE["日期"])):
#             if dfTWSE["日期"][d] == RealDateString:
#                 isFound = True
#                 break
#             else:
#                 isFound = False
#         if isFound == False:
#             RealDate = datetime.datetime.strptime(RealDateString,"%Y/%m/%d")
#             RealDateString = (RealDate+datetime.timedelta(days=-1)).strftime("%Y/%m/%d")
#             continue
#         else:
#             return RealDateString
#             break
#
# # print(dateExist(startDate,4))
#
# def firstValue(date):
#     FourDaysBeforeDate = dateExist(date,4)
#     for i in range(len(dfTWSE["日期"])):
#         if dfTWSE["日期"][i] == FourDaysBeforeDate:
#             if dfTWSE["項目"][i] == "融資(交易單位)":
#                 valueFirst = dfTWSE['今日餘額'][i]
#                 valueSecond = dfTWSE['前日餘額'][i]
#                 return (valueFirst-valueSecond)/valueSecond
#                 break
#
# def secondValue(date):
#     FourDaysBeforeDate = (date+datetime.timedelta(days=-4)).strftime("%Y/%m/%d")
#     for i in range(len(dfStockIndex['日期'])):
#         if dfStockIndex['日期'][i] == FourDaysBeforeDate:
#             valueFirst = dfStockIndex['收盤指數'][i]
#             valueSecond = dfStockIndex['開盤指數'][i]
#             return (valueFirst - valueSecond) / valueSecond
#             break
#
# def thirdValue(date):
#     ThreeDaysBeforeDate = (date+datetime.timedelta(days=-3)).strftime("%Y/%m/%d")
#     for i in range(len(dfTWSE["日期"])):
#         if dfTWSE["日期"][i] == ThreeDaysBeforeDate:
#             if dfTWSE["項目"][i] == "融資(交易單位)":
#                 valueFirst = dfTWSE['今日餘額'][i]
#                 valueSecond = dfTWSE['前日餘額'][i]
#                 return (valueFirst - valueSecond) / valueSecond
#                 break
#
# def fourthValue(date):
#     ThreeDaysBeforeDate = (date+datetime.timedelta(days=-3)).strftime("%Y/%m/%d")
#     for i in range(len(dfStockIndex['日期'])):
#         if dfStockIndex['日期'][i] == ThreeDaysBeforeDate:
#             valueFirst = dfStockIndex['收盤指數'][i]
#             valueSecond = dfStockIndex['開盤指數'][i]
#             return (valueFirst - valueSecond) / valueSecond
#             break
#
# def fifthValue(date):
#     TwoDaysBeforeDate = (date+datetime.timedelta(days=-2)).strftime("%Y/%m/%d")
#     for i in range(len(dfTWSE["日期"])):
#         if dfTWSE["日期"][i] == TwoDaysBeforeDate:
#             if dfTWSE["項目"][i] == "融資(交易單位)":
#                 valueFirst = dfTWSE['今日餘額'][i]
#                 valueSecond = dfTWSE['前日餘額'][i]
#                 return (valueFirst - valueSecond) / valueSecond
#                 break
#
# def sixthValue(date):
#     TwoDaysBeforeDate = (date+datetime.timedelta(days=-2)).strftime("%Y/%m/%d")
#     for i in range(len(dfStockIndex['日期'])):
#         if dfStockIndex['日期'][i] == TwoDaysBeforeDate:
#             valueFirst = dfStockIndex['收盤指數'][i]
#             valueSecond = dfStockIndex['開盤指數'][i]
#             return (valueFirst - valueSecond) / valueSecond
#             break
#
# def seventhValue(date):
#     OneDayBeforeDate = (date+datetime.timedelta(days=-1)).strftime("%Y/%m/%d")
#     for i in range(len(dfTWSE["日期"])):
#         if dfTWSE["日期"][i] == OneDayBeforeDate:
#             if dfTWSE["項目"][i] == "融資(交易單位)":
#                 valueFirst = dfTWSE['今日餘額'][i]
#                 valueSecond = dfTWSE['前日餘額'][i]
#                 return (valueFirst - valueSecond) / valueSecond
#                 break
#
# def eigthValue(date):
#     OneDayBeforeDate = (date+datetime.timedelta(days=-1)).strftime("%Y/%m/%d")
#     for i in range(len(dfStockIndex['日期'])):
#         if dfStockIndex['日期'][i] == OneDayBeforeDate:
#             valueFirst = dfStockIndex['收盤指數'][i]
#             valueSecond = dfStockIndex['開盤指數'][i]
#             return (valueFirst - valueSecond) / valueSecond
#             break
#
# def ninethValue(date):
#     date = date.strftime("%Y/%m/%d")
#     for i in range(len(dfTWSE["日期"])):
#         if dfTWSE["日期"][i] == date:
#             if dfTWSE["項目"][i] == "融資(交易單位)":
#                 valueFirst = dfTWSE['今日餘額'][i]
#                 valueSecond = dfTWSE['前日餘額'][i]
#                 return (valueFirst - valueSecond) / valueSecond
#                 break
#
# def tenthValue(date):
#     date = date.strftime("%Y/%m/%d")
#     for i in range(len(dfStockIndex['日期'])):
#         if dfStockIndex['日期'][i] == date:
#             valueFirst = dfStockIndex['收盤指數'][i]
#             valueSecond = dfStockIndex['開盤指數'][i]
#             return (valueFirst - valueSecond) / valueSecond
#             break
#
#
# def createListData():
#     df6 = pd.DataFrame(
#         {"日期": [], "D-4融資%數": [], "D-4股價%": [], "D-3融資%數": [], "D-3股價%": [], "D-2融資%數": [], "D-2股價%": [], "D-1融資%數": [],
#          "D-1股價%": [], "D融資%數": [], "D股價%": [], "((D+1)+(D+2)+(D+3)+(D+4)+(D+5))/5": []
#          })
#     df6["日期"] = dfStockIndex['日期']
#     # print(df6["日期"])
#     listOne = []
#     for i in range(0,123):
#         print(i)
#         listOne.append(firstValue(df6["日期"][i]))
#     print(listOne)
#     print(df6)





