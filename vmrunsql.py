import pandas as pd
import vmsvclib
from vmsvclib import *
with open("vmbot.json") as json_file:  
    bot_info = json.load(json_file)
client_name = bot_info['client_name']
vmsvclib.rds_connstr = bot_info['omdb']
vmsvclib.rdscon = None
vmsvclib.rds_pool = 0
vmsvclib.rds_schema = bot_info['schema']
#qry = f"update params set `value`='en' where `key`='lang' and client_name = '{client_name}';"
qry ="drop table menu_text;"
zz="""
file1 = open('runsql.txt', 'r') 
Lines = file1.readlines() 
file1.close() 
qry = ''.join(Lines)
#"""
print(qry)
rds_update(qry)
