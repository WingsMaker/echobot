#
#  ______                        __ __       __                     __  
# /      \                      |  \  \     /  \                   |  \
#|  ▓▓▓▓▓▓\______ ____  _______  \▓▓ ▓▓\   /  ▓▓ ______  _______  _| ▓▓_    ______   ______
#| ▓▓  | ▓▓      \    \|       \|  \ ▓▓▓\ /  ▓▓▓/      \|       \|   ▓▓ \  /      \ /      \
#| ▓▓  | ▓▓ ▓▓▓▓▓▓\▓▓▓▓\ ▓▓▓▓▓▓▓\ ▓▓ ▓▓▓▓\  ▓▓▓▓  ▓▓▓▓▓▓\ ▓▓▓▓▓▓▓\\▓▓▓▓▓▓ |  ▓▓▓▓▓▓\  ▓▓▓▓▓▓\
#| ▓▓  | ▓▓ ▓▓ | ▓▓ | ▓▓ ▓▓  | ▓▓ ▓▓ ▓▓\▓▓ ▓▓ ▓▓ ▓▓    ▓▓ ▓▓  | ▓▓ | ▓▓ __| ▓▓  | ▓▓ ▓▓   \▓▓
#| ▓▓__/ ▓▓ ▓▓ | ▓▓ | ▓▓ ▓▓  | ▓▓ ▓▓ ▓▓ \▓▓▓| ▓▓ ▓▓▓▓▓▓▓▓ ▓▓  | ▓▓ | ▓▓|  \ ▓▓__/ ▓▓ ▓▓
# \▓▓    ▓▓ ▓▓ | ▓▓ | ▓▓ ▓▓  | ▓▓ ▓▓ ▓▓  \▓ | ▓▓\▓▓     \ ▓▓  | ▓▓  \▓▓  ▓▓\▓▓    ▓▓ ▓▓
#  \▓▓▓▓▓▓ \▓▓  \▓▓  \▓▓\▓▓   \▓▓\▓▓\▓▓      \▓▓ \▓▓▓▓▓▓▓\▓▓   \▓▓   \▓▓▓▓  \▓▓▓▓▓▓ \▓▓
#
# Library functions by KH                                                
#------------------------------------------------------------------------------------------------------
summary = """
╔«═══════════════════════════════════════════════════════════════════════•[^]»╗
║ ███████████████████████     Functions Name       ███████████████████████    ║▒▒
╟─────────────────────────────────────────────────────────────────────────────╢▒▒
║ banner_msg         create a string of text banner in a line                 ║▒▒
║ build_menu         build telegram reply-to menu buttons with a list         ║▒▒
║ copydbtbl          append the dataframe into another table in RDS           ║▒▒
║ email_lookup       email address search when the field is encrypted         ║▒▒
║ get_columns        product the dataframe header into python list            ║▒▒
║ html_report        process tabulated data into formatted telegram message   ║▒▒
║ html_tbl           process tabulated data into formatted html document      ║▒▒
║ printdict          print the item details of the given dictionary object    ║▒▒
║ rds_connector      database connection to RDS database by type of client    ║▒▒
║ rds_df             output sql query on RDS database into dataframe          ║▒▒
║ rds_engine         mysql engine for connecting existing RDS database        ║▒▒
║ rds_param          get a value from RDS parameters table with a key given   ║▒▒
║ rds_update         perform SQL update query for RDS database                ║▒▒
║ syslog             standard logger for working in async, this replaces it   ║▒▒
║ time_hhmm          local time in hhmm numeric format                        ║▒▒
║ time_hhmmss        local time in hhmmss numeric format                      ║▒▒
║ write2html         output dataframe content into HTML file                  ║▒▒
╚═════════════════════════════════════════════════════════════════════════════╝▒▒
 ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
"""
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore")
import pandas as pd
import pandas.io.formats.style
import os, re, sys, time, datetime, string
import numpy as np
import matplotlib.pyplot as plt
import six
import math
import wget
import json
import pymysql
import pymysql.cursors
import pymysqlpool
import mysql.connector
from sqlalchemy import create_engine
import inspect

global rdscon, rds_connstr, rds_pool, rdsdb, rds_schema

piece = lambda txtstr,seperator,pos : txtstr.split(seperator)[pos]

def banner_msg(banner_title, banner_msg):
    txt = "❚█══ " + banner_title + " ══█❚"
    txt += "\n" + banner_msg + "\n"
    return txt

def build_menu(btn_list, btns_rows = 3, extra_btn='',toadd_btn=[]):
    if extra_btn != '':
        btn_list.append(extra_btn)
    nn = len(btn_list)
    for btn in toadd_btn:
        if nn % btns_rows > 0:
            btn_list.append(btn)
            nn += 1
    kk = btns_rows - 1
    rr = int((nn+kk)/btns_rows)
    results = [  btn_list[n*btns_rows:][:btns_rows] for n in range(rr) ]
    return results

def copydbtbl(df, tblname):
    global rdscon
    try:
        df.reset_index()
        rdsEngine = rds_engine()
        conn = rdsEngine.connect()
        df.to_sql(tblname, con=conn, if_exists = 'append', index=False, chunksize = 1000)
        conn.close()
        ok=True
    except:
        ok=False
    return ok

def email_lookup(df, email):
    student_list = [x for x in  df.studentid]
    email_list = [x for x in  df.email]
    if email in email_list:
        n = email_list.index(email)
        sid = student_list[n]
        return str(sid)
    email_list = [x.lower() for x in  df.email]
    user_dict = dict(zip(email_list,student_list))
    if email in email_list:
        sid = user_dict[email]
        return str(sid)
    return ""

def get_columns(tablename):
    global rds_schema
    query = f"SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_SCHEMA = '{rds_schema}' AND TABLE_NAME = '{tablename}';"
    df = rds_df(query)
    cols =[x for x in df[0]]
    del df
    return cols

def html_report(df, fld_list, gaps,  maxrow=20):
    m = len(fld_list)
    spacing = " "*100
    cnt = 0
    msglist = []
    msg = ' '.join([(str(fld_list[x])+spacing)[:gaps[x]] for x in range(m)]) + '\n'
    for index, row in df.iterrows():
        msg += ' '.join([(str(row[x])+spacing)[:gaps[x]] for x in range(m)]) + '\n'
        cnt += 1
        if cnt == maxrow:
            msglist.append(msg)
            cnt = 0
            msg = ' '.join([(str(fld_list[x])+spacing)[:gaps[x]] for x in range(m)]) + '\n'
    if cnt > 0:
        msglist.append(msg)
    return msglist

def html_tbl(clt, tblname, titlename, fn):
    if tblname=="":
        return None
    query = f"select * from {tblname}"
    if clt != "":
        query += f" where client_name = '{clt}';"
    df = rds_df(query)
    if df is None:
        f = None
    else:
        df.columns = get_columns(tblname)
        write2html(df, title=titlename, filename=fn)
        f = open(fn, 'rb')
    return f

def printdict(obj):
    print(*obj.items(), sep = '\n')
    return

def rds_connector():
    global rds_connstr, rds_pool, rdsdb
    port = 3306
    if rds_connstr=="":
        with open("vmbot.json") as json_file:
            bot_info = json.load(json_file)
        rds_connstr = bot_info['omdb']
        
    if ':' in rds_connstr:
        ssl_pem=rds_connstr.split('=')[1] if '=' in rds_connstr else ''
        conn_info = rds_connstr.split(":")
        user = conn_info[1].replace('/','')
        pwhost = conn_info[2].split('/')[0].split('@')
        passwd = pwhost[0]
        host = pwhost[1]
        if len(conn_info)<4:
            dbase = conn_info[2].split('/')[1].split('?')[0]
        else:
            port = conn_info[3].split('/')[0]
            if port != "":
                port = int(port)
            dbase = conn_info[3].split('/')[1]
        if 'ssl_ca' in rds_connstr:
            if host == "localhost":
                dbase = dbase.split('?')[0]
                rdscon = pymysql.connect(host=host, port=port, user=user,passwd=passwd, db=dbase,ssl={'ssl': 'OmniMentorProdAzure.pem'})
                rdsdb = None
            else:
                if rdsdb is None:
                    rds_pool = 0
                else:
                    rds_pool = rdsdb.size()
                if rds_pool == 0:
                    rds_pool = 20
                    config={'host':host, 'user':user, 'password':passwd, 'database':dbase, 'autocommit':True, 'ssl':{'ca': ssl_pem}}
                    rdsdb = pymysqlpool.ConnectionPool(size=rds_pool,name='pool', **config)
                    rds_pool = rdsdb.size()
                rdscon = rdsdb.get_connection()
        else:
            rds_pool = 1
            rdsdb = None
            rdscon = mysql.connector.connect(host=host, port=port,user=user,password=passwd,db=dbase)
    else:
        rdscon = None
    return rdscon

def rds_df(query):
    global rdscon, rdsdb
    df = None
    if rdsdb is None:
        rds_pool = 0
    else:
        rds_pool = rdsdb.size()
    if rds_pool==0:
        rdscon = rds_connector()
    if 'open' in dir(rdscon):
        rdscon.open
    rdscur = rdscon.cursor()
    rdscur.execute(query)
    rows = rdscur.fetchall()
    #if rdsdb is not None:
    #    rdscon.close()
    if len(rows) == 0:
        return None
    else:
        df = pd.DataFrame.from_dict(rows)
    return df

def rds_engine():
    global rds_connstr
    if rds_connstr=="":
        with open("vmbot.json") as json_file:
            bot_info = json.load(json_file)
        rds_connstr = bot_info['omdb']
    rdsEngine = create_engine(rds_connstr, pool_recycle=3600)
    return rdsEngine

def rds_param(query, retval="", dfmode=False):
    sqlvar = retval
    try:
        df = rds_df(query)
        if df is None:
            return retval
        if dfmode :
            sqlvar = df.copy()
        else:
            sqlvar = list(df.iloc[0])[0]
            if type(retval) != type(sqlvar):
                sqlvar = eval( str(sqlvar) )
    except:
        pass
    return sqlvar

def rds_update(query):
    global rdscon, rds_pool
    if rdscon is None:
        rds_pool = 0
    else:
        try:
            rds_pool = rdsdb.size()
        except:
            pass
    if rds_pool==0:
        rdscon = rds_connector()
    df = None
    if 'open' in dir(rdscon):
        rdscon.open
    try:
        rdscur = rdscon.cursor()
        rdscur.execute(query)
        rdscon.commit()
    except:
        pass
    #if rdsdb is not None:
    #    rdscon.close()
    return

def syslog(msg):
    s = inspect.stack()[1]
    date_now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime() )
    result = f"{date_now} :: INFO :: {s[3]} :: {s[2]} :: {msg}\n"
    f = open('syslog.log','a')
    fname = 'syslog.log'
    with open(fname, "a", encoding="utf-8") as f:
        f.write(str(result))
    f.close()
    return

def time_hhmm(gmt):
    hh = int(datetime.datetime.now().strftime('%H'))
    mm = int(datetime.datetime.now().strftime('%M'))
    hrs = (hh+gmt+24) % 24
    return hrs*100+mm

def time_hhmmss(gmt):
    hh = int(datetime.datetime.now().strftime('%H'))
    mm = int(datetime.datetime.now().strftime('%M'))
    ss = int(datetime.datetime.now().strftime('%S'))
    hrs = (hh+gmt+24) % 24
    tt = (hrs*100+mm)*100+ss
    return tt

def write2html(df, title='', filename='report.html'):
    if df is None:
        return
    result = '''
    <html>
    <head>
    <style>
        h2 {
            text-align: center;
            font-family: Helvetica, Arial, sans-serif;
        }
        table { 
            margin-left: auto;
            margin-right: auto;
        }
        table, th, td {
            border: 1px solid black;
            border-collapse: collapse;
        }
        th, td {
            padding: 5px;
            text-align: center;
            font-family: Helvetica, Arial, sans-serif;
            font-size: 90%;
        }
        table tbody tr:hover {
            background-color: #dddddd;
        }
        .wide {
            width: 90%; 
        }

    </style>
    </head>
    <body>
        '''
    result = '\n'.join([x[4:] for x in result.split('\n') ]) 
    result += '<h2> %s </h2>\n' % title
    if type(df) == pd.io.formats.style.Styler:
        result += df.render()
    else:
        result += df.to_html(classes='wide', escape=False)
    result += '''</body></html>'''
    if filename != "":
        with open(filename, 'w') as f:
            f.write(result)
    return result
   
if __name__ == "__main__":
    global rdscon, rds_connstr, rdsdb, rds_pool, rds_schema
    get_cols = lambda qry : [('~'+tt).replace(' as ','~').split('~')[-1] for tt in qry.lower().replace('from',':').replace('select','').replace('distinct','').split(':')[0].strip().split(',')]
    client_name = 'dev'
    rds_schema = "omnimentor"
    rds_connstr = "mysql://omnimentor@db-sambaashplatform-cluster-2a:omnimentor@localhost:33327/omnimentor"
    try:
        with open("vmbot.json") as json_file:
            bot_info = json.load(json_file)
        if 'client_name' in list(bot_info):
            client_name = bot_info['client_name']
        if 'schema' in list(bot_info):
            rds_schema = bot_info['schema']
        if 'omdb' in list(bot_info):
            rds_connstr = bot_info['omdb']
    except:
        bot_info = dict()
    rds_pool = 0
    rdsdb = None
    rdscon = None
    if len(sys.argv)>=2:
        rdscon = rds_connector()
        qry = str(sys.argv[1])
        print(qry)
        if qry.lower().startswith('select'):
            try:
                df = rds_df(qry)
            except:
                df = None
            if df is None:
                print("No information found")
            else:
                if '*' in qry:
                    tbl= [w for w in qry.split('from')[1].split(' ') if w !=  ''][0]
                    cols = get_columns(tbl)
                    df.columns = cols
                else:
                    cols = get_cols(qry)
                    df.columns = cols
                print(df)
            print("select query processed")
        elif qry.lower().startswith('update'):
            rds_update(qry)
            print("update query processed")
        elif qry.lower().startswith('delete'):
            rds_update(qry)
            print("delete query processed")
    print("End of vmsvclib.py")
