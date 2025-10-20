import pandas as pd
import json
import requests
import getpass
import oracledb
import contextlib
import time

#pw = getpass.getpass("Enter password: ")
print("trying to connect ...")
connection = oracledb.connect(
    user="datafix",
    password="", #pw,
    dsn="DFIXT") #ORA

cursor = connection.cursor()
cursor1 = connection.cursor()
print("Successfully connected to Oracle Database")


def update_records (id):
    #print(id)
    #cursor.execute (f" update DATAFIX.DFIX_SPARK_INTERFACE set loaded = 'Y' where ID_NUMBER = {id}")
    cursor1.execute (f" update DATAFIX.jt_SPARK_INCIDENTS set loaded = 'Y' where SCTASK =  '{id}'")
    cursor1.execute("COMMIT")

def update_records_err (id):
    #print(id)
    #cursor.execute (f" update DATAFIX.DFIX_SPARK_INTERFACE set loaded = 'Y' where ID_NUMBER = {id}")
    cursor1.execute (f" update DATAFIX.jt_SPARK_INCIDENTS set loaded = 'E' where SCTASK =  '{id}'")
    cursor1.execute("COMMIT")


for row in cursor.execute("select SCTASK, NOTE from DATAFIX.jt_SPARK_INCIDENTS where loaded is null"):
    ticket   = row[0]
    note_upd = row[1]
    print(ticket)
    #r = requests.post('https://sparkproxyuat.azure-api.net/service3050/request?ticket_number='+ticket+'&work_notes='+note_upd+'&updated_by=jtr44', headers={'Ocp-Apim-Subscription-Key': '9c471674c8b34632931498b4c0676fe5'})
    r1 = requests.post('https://sparkproxy.azure-api.net/service3050/request?ticket_number='+ticket+'&work_notes='+note_upd+'&updated_by=jtr44', headers={'Ocp-Apim-Subscription-Key': ''})
    time.sleep(0.5)
    r2 = requests.post('https://sparkproxy.azure-api.net/service3051/request?ticket_number='+ticket+'&state=Complete&updated_by=jtr44', headers={'Ocp-Apim-Subscription-Key': ''})
    print ('https://sparkproxy.azure-api.net/service3050/request?ticket_number='+ticket+'state=Open&updated_by=jtr44')
    txt = (r1.text+r2.text)
    print (txt)
    x = txt.find("error")
    if x >= 0:
        #print (x)
        update_records_err (ticket)
    else:
        #print (x)
        update_records (ticket)

    

cursor.close()
connection.close()
print("Successfully completed the SPARK GET/POST")
