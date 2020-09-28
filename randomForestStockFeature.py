import csv
import jieba
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
import joblib
import sqlite3

filename = 'Feature.txt'
modelname = 'RandomForest_twse_model.pkl'


def start():
    global filename,modelname
    fp = open(filename, "r", encoding="utf-8")
    line = fp.readline()
    fp.close()
    maped_Train_data = eval(line)
    df = pd.DataFrame(maped_Train_data)
    X = df.drop(df.columns[-1], axis=1)
    y = df[df.columns[-1]]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=101)
    rfc = RandomForestClassifier(n_estimators=200)
    rfc.fit(X_train, y_train)
    rfc_pred = rfc.predict(X_test)
    print(rfc_pred)
    print(confusion_matrix(y_test, rfc_pred))
    print(classification_report(y_test, rfc_pred))
    joblib.dump(rfc, modelname)


if __name__ == '__main__':
    start()

