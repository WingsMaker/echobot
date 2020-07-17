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

@app.route("/")
def main():
    msg = 'example:<br>http://127.0.0.1:6452/view?studentid=6116&courseid=FOS-0620A'
    print(msg)
    #return jsonify({'reply':msg})
    return msg
    

@app.route('/view', methods=['GET', 'POST'])
def view():
    studentid = request.args.get('studentid')
    cohort = request.args.get('courseid')
    sid = int(studentid)
    client_name = 'Sambaash'
    query = f"SELECT * FROM userdata where client_name='{client_name}' and studentid={sid} and courseid like '%{cohort}%';"
    df = vmsvclib.rds_df(query)
    if df is None:
        return "Unable to find the information !"
    df.columns = vmsvclib.get_columns("userdata")    
    courseid = df.courseid.values[0]
    tt = ""
    vars = {}    
    (txt, vars) = vmbotlib.verify_student(client_name, df, int(sid), courseid)    
    df = pd.DataFrame({
        'keys' : [ key for key, value in vars.items() ],
        'values' : [ value for key, value in vars.items() ]
    })
    df = df.set_index("keys")
    msg = write2html(df, f"Results for Student ID {studentid} on {cohort}", "")
    #stg = vars['stage']
    #msg = vmbotlib.display_progress(df, stg, vars, client_name)    # vmbot needed !!
    #msg = msg.replace("\n","<br>")
    #return  jsonify( {"reply":msg} )
    return msg
    
    
if __name__ == '__main__':
    vmsvclib.rds_connstr = ""
    vmsvclib.rdscon = None
    app.run(port='6452')  