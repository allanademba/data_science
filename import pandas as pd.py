###libraries imported
import pandas as pd
import sqlalchemy as db
import cx_Oracle as orcl

print("cx_Oracle.version:", orcl.version)

###connecting to the database
host = "10.21.18.13"
port = "1521"
sid = "ECHODB1"
dsn = orcl.makedsn(host, port, sid)
connection = orcl.connect("UAT","uat", dsn)
cur = orcl.Cursor(connection) 




statements = """SELECT * FROM CUSTOMER"""
names = pd.read_sql_query(statements, connection)
pd.set_option('display.max_columns', None)
names.head(4)

# show all columns without ...
pd.set_option('display.max_columns', None)
names.tail(4)

statement_trx = """select P.ACCOUNT_NUMBER,p.cust_id,
trim(C.SURNAME) || ' '|| trim(C.FIRST_NAME) ||' ' || trim(C.MIDDLE_NAME) as name,
D.ID_TRANSACT,
d.TIMESTAMP,
D.ID_JUSTIFIC,
J.DESCRIPTION,
DECODE (D.DEBIT_CREDIT_FLAG,'2','CR','1','DR') "DR/CR",
D.ENTRY_AMOUNT,
D.PREV_ACC_BALANCE, 
D.VALUE_DATE,
D.TRX_USR,
D.ENTRY_COMMENTS
from fst_demand_extrait d, justific j, profits_account p, customer c
where
D.ID_JUSTIFIC = J.ID_JUSTIFIC
and
P.CUST_ID = C.CUST_ID
and
D.FK_DEPOSIT_ACCOACC= P.DEP_ACC_NUMBER
and p.PRFT_SYSTEM=3
and trunc(d.TIMESTAMP) >= to_date('01-06-2019','dd-mm-yyyy')
and trunc(d.TIMESTAMP) <= to_date('30-09-2019','dd-mm-yyyy')
order by D.VALUE_DATE asc"""
trxns_data = pd.read_sql_query(statement_trx, connection)
pd.set_option('display.max_columns', None)
trxns_data.head(10)