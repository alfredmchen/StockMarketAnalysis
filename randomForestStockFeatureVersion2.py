import csv
import jieba
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
import joblib
import sqlite3

# filename = 'Feature.txt'
# filename2 = 'Feature2.txt'
date='2020/07/17'
modelname = 'RandomForest_twse_model.pkl'
answer = [1, 1, 1, 1, 0, 0, 0, 1, 0, 0, 1]
list = ['融資%','融券%']
back_day = 100


def start(date):
    global filename,modelname,list,answer
    conn = sqlite3.connect('./cfg/twseDB.db')
    sqlcom = "select a.日期,a.收盤指數,b.前日餘額,b.今日餘額 from stockmarketindex a,twse b where a.日期 = b.日期 and b.項目='融資(交易單位)' order by a.日期 desc limit 600"
    sqlcom2 = "select 日期, 前日餘額, 今日餘額 from twse where 項目 = '融券(交易單位)'order by 日期 desc"
    df2 = pd.read_sql(sqlcom2, con=conn)
    df1 = pd.read_sql(sqlcom, con=conn)
    print(df1)
    print(df2)
    result = []
    df_analysis = pd.DataFrame(columns=["日期", "指數%", "融資%", "融券%"])
    for index, row in df1.iterrows():
        if (index + 1) < (len(df1)):
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
            df_analysis = df_analysis.append(temp, ignore_index=True)
    for i in range(len(date)):
        index = df_analysis.loc[(df_analysis['日期'] == date[i])].index
        result.append(getData(index,df_analysis))
    conn.close()
    model = joblib.load(modelname)
    df = pd.DataFrame(result)
    result = model.predict(df)
    result = result.tolist()
    dfPredict = pd.DataFrame(result,date).T
    dfResultTable = pd.DataFrame(answer,date).T
    dfResultTable = dfResultTable.append(dfPredict)
    print(dfResultTable.to_string())

def getData(index,df_analysis):
    global back_day,list
    feature = []
    for i in range(back_day, -1, -1):
        row = df_analysis.loc[index + i]
        for item in list:
            feature.append(float(row[item]))
    return feature

if __name__ == '__main__':
    # test_scan_date = ['2020/04/08']
    test_scan_date = ['2020/07/17']
    answer = ["?"]

    start(test_scan_date)

