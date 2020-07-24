#!/usr/bin/env python
# coding: utf-8

from flask import Flask, request, jsonify 
from flask_restful import Resource, Api
import json
import requests

import pandas as pd

import vmbotlib
from vmbotlib import *
import vmsvclib
from vmsvclib import *

app = Flask(__name__)
api = Api(app)

#class HelloWorld(Resource):
#     def get(self):
#        txt = "Hello world from Python Api !"
#        result = {'reply' : txt}
#        json = jsonify(result)        
#        return json         

# http://127.0.0.1:6452/hello
#api.add_resource(HelloWorld, '/hello') 

global client_name, resp_dict

@app.route("/")
def main():
    msg = 'example:<br>http://127.0.0.1:6452/progress?studentid=6116&courseid=FOS-0620A'
    print(msg)
    #return jsonify({'reply':msg})
    return msg
    

@app.route('/view', methods=['GET', 'POST'])
def viewrecords():
    global client_name
    studentid = request.args.get('studentid')
    cohort = request.args.get('courseid')
    sid = int(studentid)    
    query = f"SELECT * FROM userdata where client_name='{client_name}' and studentid={sid} and courseid like '%{cohort}%';"
    df = vmsvclib.rds_df(query)
    if df is None:
        return "Unable to find the information !"
    df.columns = vmsvclib.get_columns("userdata")    
    courseid = df.courseid.values[0]
    tt = ""
    vars = {}    
    (txt, vars) = vmbotlib.verify_student(client_name, df, int(sid), courseid)    
    df1 = pd.DataFrame({
        'keys' : [ key for key, value in vars.items() ],
        'values' : [ value for key, value in vars.items() ]
    })
    df1 = df1.set_index("keys")
    msg = write2html(df1, f"Results for Student ID {studentid} on {cohort}", "")
    return msg

@app.route('/progress', methods=['GET', 'POST'])
def viewprogress():
    global client_name, resp_dict
    studentid = request.args.get('studentid')
    cohort = request.args.get('courseid')
    sid = int(studentid)    
    query = f"SELECT * FROM userdata where client_name='{client_name}' and studentid={sid} and courseid like '%{cohort}%';"
    df = vmsvclib.rds_df(query)
    if df is None:
        return "Unable to find the information !"
    df.columns = vmsvclib.get_columns("userdata")    
    courseid = df.courseid.values[0]
    tt = ""
    vars = {}    
    (tt, vars) = vmbotlib.verify_student(client_name, df, int(sid), courseid)        
    vars = vmbotlib.display_progress(df, vars, client_name, resp_dict, 0.7)
    msg  = vars['notification']    
    msg = msg.replace("\n","<br>")
    return msg
    
def test_surveydatapoint():
    # https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/step-3-proof-of-concept-connecting-to-sql-using-pyodbc?view=sql-server-ver15
    # https://dataedo.com/kb/query/sql-server/list-table-columns-in-database
    # pip install pyodbc
    import pyodbc
    driver = "SQL Server"
    host = "lithanbidb.database.windows.net"
    user = "LithanBI"
    passwd = "Lithan4u$"
    port = 1433 
    db = "LithanSurvey"
    pss_connstr = f"DRIVER={driver};SERVER=tcp:{host};DATABASE={db};UID={user};PWD={passwd}"
    psscon = pyodbc.connect(pss_connstr)
    if psscon is None:
        print("unable to connect tb_pss")
    psscur = psscon.cursor()
    query = "SELECT @@version;"
    qry_desctable = """SELECT column_name FROM (
SELECT col.name as column_name,
	col.column_id,
	 schema_name(tab.schema_id) as schema_name,
    tab.name as table_name        
from sys.tables as tab
	inner join sys.columns as col
	on tab.object_id = col.object_id
	left join sys.types as t
	on col.user_type_id = t.user_type_id
WHERE schema_name(tab.schema_id) = 'dbo' and 
	tab.name = 'tb_pss' 	
)  pss
    """
    fld_list = ['cohort', 'mode_of_session', 'learner_name', 'learner_email', 'content', 'content_comments', \
    'duration', 'duration_comments', 'faculty', 'faculty_comments', 'support', 'support_comments', 'understanding', 'understanding_comments', \
    'content_response', 'duration_response', 'faculty_response', 'support_response', 'understanding_response']
    field_list = ','.join(fld_list)
    query = f"select {field_list} from tb_pss where cohort = '0620A' and pillar='SM' and qualification = 'EIT'; "
    psscur.execute(query)
    row = psscur.fetchone() 
    cnt=len(list(row))
    print('='*50)
    print( '\n'.join( [ fld_list[n] + ' : ' + str(row[n]) for n in range(2) ] ) )
    print('-'*50)
    while row:         
        print( '\n'.join( [ fld_list[n] + ' : ' + str(row[n]) for n in range(cnt) if n>1] ) )
        print('-'*50)
        row = psscur.fetchone()        
    return
    
if __name__ == '__main__':
    global client_name, resp_dict
    with open("vmbot.json") as json_file:  
        bot_info = json.load(json_file)
    client_name = bot_info['client_name']
    vmsvclib.rds_connstr = ""
    vmsvclib.rdscon = None
    #resp_dict = vmbotlib.load_respdict()
    #app.run(port='6452')
    #app.run()
    test_surveydatapoint()

