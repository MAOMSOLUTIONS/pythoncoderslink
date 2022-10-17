import pandas as pd
import sqlite3
from dotenv import load_dotenv
import os

load_dotenv()

def sqlqueries():
    filetransactions = None
    fcustomer = None
    try:    
        filetransactions = open (f'{os.environ["DATA_OUTPUT_DIRECTORY"]}/{os.environ["QUERY_DIRECTORY"]}/{os.environ["TRANSACTIONS_BYDAY"]}.sql','w')
        filetransactions.write('SELECT Date,count(TransactionID) as CountOfTransactions from CleanedTransactions group by Date')
        filetransactions.close()
        #df = pd.read_sql_query("SELECT Date,count(TransactionID) as CountOfTransactions from CleanedTransactions group by Date", con)
        #print(df.head())
        fcustomer = open (f'{os.environ["DATA_OUTPUT_DIRECTORY"]}/{os.environ["QUERY_DIRECTORY"]}/{os.environ["CUSTOMER_BYDAY"]}.sql','w')
        fcustomer.write('SELECT CustomerID,Date,sum(TransactionAmount) as Total from CleanedTransactions group by CustomerID,Date')
        fcustomer.close()
        #df = pd.read_sql_query("SELECT CustomerID,Date,sum(TransactionAmount) as Total from CleanedTransactions group by CustomerID,Date", con)
        #print(df.head())
        print(f'{os.environ["LABEL_SQL_FILES"]}{os.environ["LABEL_OK"]}')
    except Error as e:
        print(f'{os.environ["LABEL_SQL_FILES"]}{os.environ["LABEL_EROR"]}{e}')       
        return False
    
def sqllite_database(data,transactions,customers):
    con = None
    try:    
        con = sqlite3.connect(f'{os.environ["DATA_OUTPUT_DIRECTORY"]}/{os.environ["DATA_OUTPUT_DB_DIRECTORY"]}/{os.environ["DATABASE_FOR_FUTURE"]}')
        data.to_sql(os.environ["CLEANEDTRANSACTIONS"], con, if_exists="replace",index=False)
        transactions.to_sql(os.environ["TRANSACTIONS"], con, if_exists="replace")
        customers.to_sql(os.environ["CUSTOMER"], con, if_exists="replace")
        con.close()
        print(f'{os.environ["LABEL_DB"]}{os.environ["LABEL_OK"]}')
        return True
    except Error as e:
        print(f'{os.environ["LABEL_DB"]}{os.environ["LABEL_EROR"]}{e}')       
        return False

def customersbyday(data):    
    customerfile = None
    joindata = None
    customers = None
    try:
        customerfile = pd.read_csv(f'{os.environ["DATA_INPUT_DIRECTORY"]}/{os.environ["CUSTOMER"]}.csv')
        joindata = data.merge(customerfile, on="CustomerID", how="left")        
        customers = joindata.groupby(by=[f'{os.environ["FIELD_CUSTOMER_NAME"]}',f'{os.environ["FIELD_DATE"]}']).agg(Total = (f'{os.environ["FIELD_TRANSACTION_AMOUNT"]}', 'sum'))
        customers.to_csv(f'{os.environ["DATA_OUTPUT_DIRECTORY"]}/{os.environ["DATA_OUTPUT_REPORT_DIRECTORY"]}/{os.environ["CUSTOMER_BYDAY"]}.csv')        
        print(f'{os.environ["LABEL_REPORT_CUSTOMERBYDAY"]}{os.environ["LABEL_OK"]}')
        return True,customers
    except Error as e:
        print(f'{os.environ["LABEL_REPORT_CUSTOMERBYDAY"]}{os.environ["LABEL_EROR"]}{e}')       
        return False

def transactionsbyday(data):
    transactions = None    
    try:
        transactions = data.groupby(f'{os.environ["FIELD_DATE"]}')[f'{os.environ["FIELD_TRANSACTIONID"]}'].count().rename(f'{os.environ["FIELD_COUNT_OF_TRANSACTIONS"]}')
        transactions.to_csv(f'{os.environ["DATA_OUTPUT_DIRECTORY"]}/{os.environ["DATA_OUTPUT_REPORT_DIRECTORY"]}/{os.environ["TRANSACTIONS_BYDAY"]}.csv')
        print(f'{os.environ["LABEL_REPORT_TRANSACTIONBYDAY"]}{os.environ["LABEL_OK"]}')
        return True,transactions
    except Error as e:
        print(f'{os.environ["LABEL_REPORT_TRANSACTIONBYDAY"]}{os.environ["LABEL_EROR"]}{e}')       
        return False

def datanormalization(data):
    data['TransactionAmount'] = data['TransactionAmount'].str.replace('$','', regex=True).astype(float)
    
def cleanedtransactions(index):
    data = None    
    try:
        data = pd.read_csv(f'{os.environ["DATA_INPUT_DIRECTORY"]}/{os.environ["TRANSACTIONS"]}.csv')
        data.sort_values(index, inplace=True)
        data.drop_duplicates(subset=[index],keep=False,inplace=True)
        data.to_csv(f'{os.environ["DATA_OUTPUT_DIRECTORY"]}/{os.environ["CLEANEDTRANSACTIONS"]}.csv',index=False)
        print(f'{os.environ["LABEL_CLEANED_TRANSACTION"]}{os.environ["LABEL_OK"]}')
        datanormalization(data)
        return True,data
    except Error as e:
        print(f'{os.environ["LABEL_CLEANED_TRANSACTION"]}{os.environ["LABEL_EROR"]}-{e}')
        return False,""
    
def main():
    flag = None    
    datafile = None    
    flagtransaction = None    
    transactiondata = None    
    flagcustomer = None    
    customerdata = None    
    print(os.environ["LABEL_TITLE"])
    flag,datafile = cleanedtransactions('TransactionID')
    if flag:
        flagtransaction,transactiondata = transactionsbyday(datafile)
        flagcustomer,customerdata = customersbyday(datafile)
        if  flagtransaction and flagcustomer:
            if sqllite_database(datafile,transactiondata,customerdata):
                sqlqueries()
    
if __name__ == '__main__':
    main()