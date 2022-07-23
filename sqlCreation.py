from codecs import ignore_errors
from turtle import clear
import mysql.connector
from datetime import date
import pandas as pd

creation = 0

mydb = mysql.connector.connect(
    host ="192.168.42.199",
    user = "pegadaian",
    password = "pegadaian123",
    database = "pegadaian_test"
)

df = pd.read_csv("surabaya2.csv")
df.drop(df.columns[0], axis=1, inplace=True)
df_sql = pd.DataFrame().assign(CBM=df['CBM'], UBM=df['UBM'], OSL=df['OSL'])
list_monitoring = []

def clearTable():
    mycursor = mydb.cursor()
    sql = "DELETE FROM surabaya2"
    mycursor.execute(sql)
    mydb.commit()

def createTable():
    mycursor = mydb.cursor()
    sql = "CREATE TABLE surabaya2 (CBM varchar(255), UBM varchar(255), OSL int)"
    mycursor.execute(sql)
    mydb.commit()

def insertValue(ubm, cbm, osl):
    mycursor = mydb.cursor()
    sql = "INSERT INTO surabaya2 (UBM, CBM, OSL) VALUES ('%s', '%s', '%d')" %(ubm, cbm, osl)
    mycursor.execute(sql)
    mydb.commit()

def sum(): 
    mycursor = mydb.cursor(buffered=True)
    sql = "SELECT UBM, CBM, SUM(OSL) AS OSL_TOTAL, COUNT(UBM) FROM surabaya2 GROUP BY UBM"
    mycursor.execute(sql)
    mydb.commit()
    rows = mycursor.fetchall()
    for row in rows:
        list_monitoring.append([row[0], row[1], row[2], row[3]])
    return list_monitoring

list_monitoring = sum()
df_final = pd.DataFrame(list_monitoring, columns=['UBM', 'CBM', 'OSL_TOTAL', 'COUNT'])
print(df_final)

if creation == 1:
    clearTable()
    for index, row in df_sql.reset_index().iterrows():
        ubm = row['UBM'][6:-1]
        cbm = row['CBM'][6:]
        osl = row['OSL']
        insertValue(ubm, cbm, osl)
        print(index)