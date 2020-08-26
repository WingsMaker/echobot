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
║ callgraph          generate flow diagram and save into png file             ║▒▒
║ conn_open          check database connection status (openned , closed)      ║▒▒
║ copydbtbl          append the dataframe into another table in RDS           ║▒▒
║ copy2omdb          append the dataframe into another table in SQLite        ║▒▒
║ decrypt            to decrypt text using fernet cryptography                ║▒▒
║ debug              to trace the arguments & return values, just add @debug  ║▒▒
║ edit_fields        update record based on a list of (key, value) pairs      ║▒▒
║ edit_records       list of (key, value) pairs in a string                   ║▒▒
║ email_lookup       email address search when the field is encrypted         ║▒▒
║ encrypt            to encrypt text using fernet cryptography                ║▒▒
║ encrypt_email      to encrypt email address field on a database record      ║▒▒
║ get_attachment     download the telegram attachement file locally           ║▒▒
║ get_columns        product the dataframe header into python list            ║▒▒
║ html_report        process tabulated data into formatted telegram message   ║▒▒
║ html_tbl           process tabulated data into formatted html document      ║▒▒
║ printdict          print the item details of the given dictionary object    ║▒▒
║ pycmd              execute python codes via eval()                          ║▒▒
║ querydf            output sql query on SQLite database into dataframe       ║▒▒
║ rds_connector      database connection to RDS database by type of client    ║▒▒
║ rds_df             output sql query on RDS database into dataframe          ║▒▒
║ rds_engine         mysql engine for connecting existing RDS database        ║▒▒
║ rds_param          get a value from RDS parameters table with a key given   ║▒▒
║ rds_update         perform SQL update query for RDS database                ║▒▒
║ render_table       output dataframe into HTML table in picture format       ║▒▒
║ shellcmd           to execute system commands from the server shell access  ║▒▒
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
import subprocess 
import numpy as np
import matplotlib.pyplot as plt
import six
import math
import sqlite3
import wget
import json
import cryptography
import pickle
from cryptography.fernet import Fernet
import telepot
from telepot.namedtuple import ReplyKeyboardMarkup
import pymysql
import pymysql.cursors
import pymysqlpool
#from pymysql_manager import ConnectionManager
#from pymysqlpool.pool import Pool
import sqlite3
from sqlalchemy import create_engine
import random
import inspect

global edxcon, rdscon, rds_connstr, rds_pool, rdsdb 

piece = lambda txtstr,seperator,pos : txtstr.split(seperator)[pos]
#matplotlib.use('Agg')

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

def conn_open():
    global rds_connstr, rdscon
    if 'ssl_ca' in rds_connstr:
        stat = rdscon.open
        return stat
    if rds_connstr == 'omdb.db':
        return rdscon is not None
    return rdscon.open

def copydbtbl(df, tblname):
    global rdscon
    #try:
    rdscon = rds_connector()
    if '.db' in rds_connstr:
        df.to_sql(tblname, con=rdscon,index=False, if_exists='append') 
    else:
        df.reset_index()
        rdsEngine = rds_engine()
        rdscon = rdsEngine.connect()
        df.to_sql(tblname, con=rdsEngine, if_exists = 'append', index=False, chunksize = 1000)
        #rdscon.close()
        #ok=True
    #except:
    #    ok=False
    #return ok
    return

def copy2omdb(df, tbl):
    sqldb = "omdb.db"
    conn = sqlite3.connect(sqldb)
    df.to_sql(tbl, con=conn,index=False, if_exists='replace') 
    conn.close()
    return

def decrypt(msg):
    key = '4jYUFl-wbMZ4NIiI2kG3LMFD5KTTKiT5ZiE6Yhoshp0='
    cto = Fernet(key)
    try:
        txt = msg.encode()
    except:
        txt = msg
    resp = cto.decrypt(txt)
    return resp.decode()

def edit_fields(client_name, courseid, tbl, idx, sid, resp, edit_list=[]):
    global vmbot
    if resp == "0":
        txt = "Nothing to update"
    else:
        updqry = ''
        try:
            for sqlvar in resp.split('\n') :
                if sqlvar != "":
                    sqlcode = sqlvar.split(':') 
                    updqry += ',' + sqlcode[0] + ' = ' + sqlcode[1] + ' '
            updqry = 'update ' + tbl + ' set ' + updqry[1:] 
            if edit_list == []:
                if sid > 0:
                    updqry += " where " + idx + " = " + str(sid)
            else:
                updqry += " where " + idx + " in (" + ','.join([str(x) for x in edit_list]) + ")"
            try:
                updqry += " and courseid = '" + courseid + "' and client_name ='" + client_name + "';"
                rds_update(updqry)
            except:
                txt = "unable to update the record !"
                return txt
            txt = ""
            if sid > 0:
                txt += " for " + idx + " = " + str(sid) 
            if txt == "":
                txt = "Nothing to update"
            else:
                txt += " is now updated."
        except:
            txt = "unable to update the record !"
    return txt

def edit_records(client_name, courseid, tbl, idx, sid,  fld_prefix = ""):
    txt = ""
    if sid > 0:
        qry = "select * from " + tbl + " where client_name = '_c_' and courseid = '_x_';"
        qry = qry.replace('_c_', client_name)
        qry = qry.replace('_x_', courseid)
        df = rds_df( qry)
        if df is None:
            return txt
        df.columns = get_columns(tbl)         
        rec_match = ( df[idx] == sid ) 
        if len(df[rec_match]) > 0:
            vars = []
            txt = ''
            rec_match = df[rec_match]
            for fld in list(rec_match):
                fldname = fld.lower()
                if fld_prefix in fldname :
                    fldvar = list(rec_match[fld])[0]
                    txt += fldname + ":" + str(fldvar) + "\n"
    return txt

def email_lookup(df, email):
    student_list = [x for x in  df.studentid]
    email_list = [x for x in  df.email]        
    if email in email_list:
        n = email_list.index(email)
        sid = student_list[n]
        return str(sid)
    #email_list = [decrypt(x) for x in  df.email]
    email_list = [x.lower() for x in  df.email]
    user_dict = dict(zip(email_list,student_list))
    if email in email_list:
        sid = user_dict[email]
        return str(sid)
    return ""

def encrypt(msg):
    key = '4jYUFl-wbMZ4NIiI2kG3LMFD5KTTKiT5ZiE6Yhoshp0='
    cto = Fernet(key)
    txt = cto.encrypt(msg.encode())
    return txt

def encrypt_email(clt):
    try:
        qry = f"select studentid, email from user_master where client_name = '{clt}' ;"
        df = rds_df(qry)
        if df is None:
            return
        student_list = [x for x in  df.studentid]
        email_list = [x for x in  df.email]
        user_dict = dict(zip(student_list,email_list))
        for sid in student_list:
            email_str = user_dict[sid]
            if email_str[:6]=='gAAAAA':
                enc_email = email_str
            else:
                enc_email = encrypt(user_dict[sid]).decode()
            query = f"update user_master set email = '{enc_email}' where client_name = '{clt}' and studentid = {sid};"
            try:
                rds_update(query)
                #email_str = decrypt(enc_email)
                #print(sid, email_str)
            except:                
                #print(f"Error encrypting for #{sid}")
                pass
        #print("Email fields encrypted")
    except:
        pass
    return

def get_attachment(bot, fid):
    fpath = bot.getFile(fid)['file_path']
    fn = "https://api.telegram.org/file/bot" + bot._token + "/"  + fpath
    fname = wget.download(fn)
    return fname
    
def get_columns(tablename):
    if '.db' in rds_connstr:
        df = querydf(rds_connstr, f"PRAGMA table_info('{tablename}');")
        cols = [x for x in df.name]
        return cols
    query = f"SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_NAME = '{tablename}';"
    df = rds_df(query)
    df.columns = ['COLUMN_NAME']
    cols = [ x for x in df.COLUMN_NAME ]
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
            #result += msg
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
    
def pycmd(resp):
    result = ""
    try:        
        pycode = compile(resp, 'test', 'eval')
        result = str(eval(pycode))
    except:
        result = ""
    return result

def querydf(sqldb, query):
    df = None    
    try:        
        conn = sqlite3.connect(sqldb)
        df = pd.read_sql_query(query, conn)
        conn.close()        
    except:
        pass
    return df

def rds_connector():
    global rds_connstr, rds_pool, rdsdb
    if rds_connstr=="":
        with open("vmbot.json") as json_file:  
            bot_info = json.load(json_file)
        rds_connstr = bot_info['omdb']
    if ':' in rds_connstr:
        if 'ssl_ca' in rds_connstr:
            if rds_pool == 0:
                conn_info = rds_connstr.split(":")    
                user = conn_info[1].replace('/','')    
                pwhost = conn_info[2].split('/')[0].split('@')
                passwd = pwhost[0]
                host = pwhost[1]
                dbase = conn_info[2].split('/')[1].split('?')[0]
                ssl_pem=rds_connstr.split('=')[1]
                rds_pool = 20
                config={'host':host, 'user':user, 'password':passwd, 'database':dbase, 'autocommit':True, 'ssl':{'ca': ssl_pem}}
                rdsdb = pymysqlpool.ConnectionPool(size=rds_pool,name='pool', **config)            
            rdscon = rdsdb.get_connection()
            rdscon.ping(True)                
        else:
            rdscon = pymysql.connect(host=host, port=3306, user=user,passwd=passwd,db=db)
            rdscon.ping(True)
    elif '.db' in rds_connstr:
        rdscon = sqlite3.connect(rds_connstr)
    else:
        rdscon = None
    return rdscon

#@debug
def rds_df(query):
    global rdscon
    df = None
    #try:
    rdscon = rds_connector()
    #if rdscon is None:
    #if not rdscon.open:
    if not conn_open():
        rdscon = rds_connector()
        #if rdscon is None:
        #if not rdscon.open:
        if not conn_open():
            syslog("RDS connection unsuccessful !")
            return    
    rdscur = rdscon.cursor()
    rdscur.execute(query)
    rows = rdscur.fetchall()    
    #rdscon.commit()
    rdscon.close()
    if len(rows) == 0:
        syslog("rds_df returns no data")
        return None
    else:
        df = pd.DataFrame.from_dict(rows)   
    #except:
    #    pass
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
            #sqlvar = df[fld].iloc[0]
            if type(retval) != type(sqlvar):                
                sqlvar = eval( str(sqlvar) )
    except:
        pass
    return sqlvar

#@debug
def rds_update(query):
    global rdscon
    rdscon = rds_connector()
    df = None
    #if rdscon is None:
    if not conn_open():
        rdscon = rds_connector()
        #if rdscon is None:
        if not conn_open():
            syslog("RDS connection unsuccessful !")
            return
    rdscur = rdscon.cursor()
    rdscur.execute(query)
    try:
        rdscon.commit()
    except:
        pass
    rdscon.close()    
    return 

def render_table(data, col_width=3.0, row_height=0.625, font_size=14,
                    header_color='#40466e', row_colors=['#f1f1f2', 'w'], edge_color='w',
                    bbox=[0, 0, 1, 1], header_columns=0, title_name='', ax=None, **kwargs):
    if ax is None:
        size = (np.array(data.shape[::-1]) + np.array([0, 1])) * np.array([col_width, row_height])
        if size[1] < 2:
            size[1] = 2
        fig, ax = plt.subplots(figsize=size)
        ax.axis('off')
    if title_name != '':
        plt.title(title_name)
    mpl_table = ax.table(cellText=data.values, bbox=bbox, colLabels=data.columns, **kwargs)

    mpl_table.auto_set_font_size(False)
    mpl_table.set_fontsize(font_size)

    for k, cell in six.iteritems(mpl_table._cells):
        cell.set_edgecolor(edge_color)
        if k[0] == 0 or k[1] < header_columns:
            cell.set_text_props(weight='bold', color='w')
            cell.set_facecolor(header_color)
        else:
            cell.set_facecolor(row_colors[k[0]%len(row_colors) ])
    return ax

def shellcmd(cmd):
    try:
        ps = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        output = (ps.communicate()[0]).decode("utf8")
    except:
        output = ''
    return output

def syslog(msg):
    s = inspect.stack()[1]
    date_now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime() )
    result = f"{date_now} :: INFO :: {s[3]} :: {s[2]} :: {msg}\n"
    f = open('syslog.log','a')
    f.write(result)
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
    global rdscon, rds_connstr, rdsdb, rds_pool
    rds_connstr = ""
    #rds_connstr = "mysql+pymysql://omnimentor@db-sambaashplatform-cluster-2a:omnimentor@db-sambaashplatform-cluster-2a.mysql.database.azure.com/omnimentor?ssl_ca=BaltimoreCyberTrustRoot.crt.pem"
    rds_pool = 0
    rdsdb = None
    rdscon = None
    rdscon = rds_connector()        
    df = rds_df("select * from userdata")    
    df.columns = get_columns("userdata")    
    #copy2omdb(df,"userdata")
    print(df.head(10))
    #xls2sqldb('userdata.csv', 'omdb.db')
    #
    print("End of vmsvclib.py")
