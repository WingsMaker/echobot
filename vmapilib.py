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
    
    
if __name__ == '__main__':
    global client_name, resp_dict
    with open("vmbot.json") as json_file:  
        bot_info = json.load(json_file)
    client_name = bot_info['client_name']
    vmsvclib.rds_connstr = ""
    vmsvclib.rdscon = None
    resp_dict = vmbotlib.load_respdict()
    app.run(port='6452')
    #app.run()
