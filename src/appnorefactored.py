import pandas as pd
import sqlite3

#1.- Unique TransactionID
data = pd.read_csv('../data/Transactions.csv')
data.sort_values('TransactionID', inplace=True)
data.drop_duplicates(subset=['TransactionID'],keep=False,inplace=True)
data.to_csv('CleanedTransactions.csv',index=False)
data['TransactionAmount'] = data['TransactionAmount'].str.replace('$','', regex=True).astype(float)

#2a.- Report1 TransactionsByDay

transactions = data.groupby('Date')['TransactionID'].count().rename('CountOfTransactions')
transactions.to_csv('../reports/TransactionsByDay.csv')

#2b.- Report2 CustomersTotalsByDay
customers = data.groupby(by=['CustomerID','Date']).agg(Total = ("TransactionAmount", 'sum'))
customers.to_csv('../reports/CustomerTotalsByDay.csv')

#3.- SQL Lite database
con = None
con = sqlite3.connect("../db/database_for_future.sqlite")
data.to_sql("CleanedTransactions", con, if_exists="replace")
transactions.to_sql("Transactions", con, if_exists="replace")
customers.to_sql("Customers", con, if_exists="replace")
con.close()

#4 .- 
#df = pd.read_sql_query("SELECT Date,count(TransactionID) as CountOfTransactions from CleanedTransactions group by Date", con)
#print(df.head())
f = open ('../query/TransactionsByDay.sql','w')
f.write('SELECT Date,count(TransactionID) as CountOfTransactions from CleanedTransactions group by Date')
f.close()
#df = pd.read_sql_query("SELECT CustomerID,Date,sum(TransactionAmount) as Total from CleanedTransactions group by CustomerID,Date", con)
#print(df.head())
f = open ('../query/CustomerTotalsByDay.sql','w')
f.write('SELECT CustomerID,Date,sum(TransactionAmount) as Total from CleanedTransactions group by CustomerID,Date')
f.close()
