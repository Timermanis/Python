import pandas as pd
import json
import requests
import getpass
import oracledb
import contextlib
import pyodbc


try:
    connection = oracledb.connect(
    user="datafix",
    password="datafix_password01", #pw,
    dsn="DFIXT") #ORA
    cursor = connection.cursor()
    cursor1 = connection.cursor()
    cursor_max = connection.cursor()
    print("Successfully connected to Oracle Database")

    for row in cursor_max.execute("select nvl(max(to_date(OPENED_AT,'yyyy-mm-dd hh24:mi:ss')),sysdate - 365 ) OPENED_AT from datafix.jt_SPARK_INCIDENTS"):
        print(row[0])
        max_opened_at=str(row[0])
    
except:
    print('Exception Oracle')


try:
    print(max_opened_at)
    #connect = pyodbc.connect('DRIVER= {ODBC Driver 18 for SQL Server}; SERVER=vdc-vm-0002695\SIMDH1D; DATABASE=AllUsers_SIMDataHub_001; Trusted_Connection=yes')
    connect = pyodbc.connect('DRIVER= {ODBC Driver 18 for SQL Server}; SERVER=WPSQLSA0\SIMDH1P; DATABASE=AllUsers_SIMDataHub_001; Trusted_Connection=yes')
    cursorSQL = connect.cursor()
    print("Successfully connected to SQL Server Database")

    #print("SELECT a.number SCTASK,b.* FROM [~sc_req_item] b join [~sc_task] a on a.dv_parent = b.number where upper(b.description) like '%DQ4522%' and b.opened_at > " + "'"+ max_opened_at+"'")
    
    cursorSQL.execute ("SELECT a.number SCTASK,b.* FROM [~sc_req_item] b join [~sc_task] a on a.dv_parent = b.number where upper(b.description) like '%DQ4522%' and b.dv_state='Open' and b.opened_at > " + "'"+ max_opened_at+"'")
    #cursorSQL.execute ("SELECT a.number SCTASK,b.* FROM [~sc_req_item] b join [~sc_task] a on a.dv_parent = b.number where upper(b.description) like '%DQ4522%' and b.opened_at > getdate()-365 ")
    
    
except Exception as e:
    print(e)
    print('Exception SQL Server')
    

#################################################################### PROCESS the RECORDS ######################################

try:

    for row in cursorSQL:
        print(row.number)
        cursor.execute(f"INSERT INTO datafix.jt_SPARK_INCIDENTS (ID_number,state,short_description,description,opened_at,request,SCTASK) VALUES ('{row.number}','{row.state}','{row.short_description}','{row.description}','{row.opened_at}','{row.dv_request}','{row.SCTASK}')")
        cursor.execute("COMMIT")

except Exception as e:
    print(e)
    print('Exception in INSERT')


print('Successfully completed SPARK EXTRACT programm')


    
