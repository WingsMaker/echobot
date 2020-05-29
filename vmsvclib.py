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
║ bot_prompt         bot response with text and optional menu buttons         ║▒▒
║ build_menu         build telegram reply-to menu buttons with a list         ║▒▒
║ copydbtbl          import .db file into sqlite database(replace mode)       ║▒▒
║ csv2sqldb          import .csv file into sqlite database(replace mode)      ║▒▒
║ decrypt            to decrypt text using fernet cryptography                ║▒▒
║ edit_fields        update record based on a list of (key, value) pairs      ║▒▒
║ edit_records       list of (key, value) pairs in a string                   ║▒▒
║ edx_connect        connect to EdX database client session                   ║▒▒
║ edx_disconnect     disconnect from EdX database client session              ║▒▒
║ edx_query          sql query from EdX database into HTML file               ║▒▒
║ edxsql             edx_connect + edx_query + edx_disconnect                 ║▒▒
║ email_lookup       email address search when the field is encrypted         ║▒▒
║ encrypt            to encrypt text using fernet cryptography                ║▒▒
║ encrypt_email      to encrypt email address field on a database record      ║▒▒
║ get_attachment     download the telegram attachement file locally           ║▒▒
║ list_table         output sql query into HTML table in picture format       ║▒▒
║ load_data          output a table in SQLite database into dataframe/list    ║▒▒
║ mass_encrypt_email to encrypt email address field across the entire folder  ║▒▒
║ pycmd              execute python codes via eval()                          ║▒▒
║ querydf            output sql query on SQLite database into dataframe       ║▒▒
║ render_table       output dataframe into HTML table in picture format       ║▒▒
║ shellcmd           to execute system commands from the server shell access  ║▒▒
║ sql2var            extract a value from params table in sysconf.db          ║▒▒
║ sqldb2xls          export a table in SQLite database into .xlsx             ║▒▒
║ text2voice         convert text to audo using google gTTS api               ║▒▒
║ time_hhmm          local time in hhmm numeric format                        ║▒▒
║ update_playbooklist add record into the playbooks table in the pbconfig.db  ║▒▒
║ updatesql          perform SQL update query for SQLite database             ║▒▒
║ write2html         output dataframe content into HTML file                  ║▒▒
║ xls2sqldb          import .xlsx file into sqlite database(replace mode)     ║▒▒
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
from cryptography.fernet import Fernet
import telepot
from telepot.namedtuple import ReplyKeyboardMarkup
import pymysql
import pymysql.cursors
import sqlite3

global edxcon

sysconfig = "sysconf.db"
pbconfig = "pbconfig.db"
nlpconfig = "nlp-conf.db"
option_back = "◀️"

def banner_msg(banner_title, banner_msg):
    txt = "▓▓▓▒▒▒▒▒▒▒░░░  " + banner_title + "  ░░░▒▒▒▒▒▒▒▓▓▓"
    txt += "\n" + banner_msg + "\n"  
    return txt

def bot_prompt(bot, chat_id, txt="", buttons=[], opt_resize = True):
    if (chat_id == 0) or (txt == ""):
        return
    if chat_id < 0:
        sent = bot.sendMessage(chat_id, txt)
        #edited = telepot.message_identifier(sent)
        return
    if buttons == []:
        hide_keyboard = {'hide_keyboard': True}
        sent = bot.sendMessage(chat_id, txt, reply_markup=hide_keyboard)
        return
    try:
        if chat_id > 0:
            mark_up = ReplyKeyboardMarkup(keyboard=buttons,one_time_keyboard=True,resize_keyboard=opt_resize)
            sent = bot.sendMessage(chat_id, txt, reply_markup=mark_up)
        else:
            sent = bot.sendMessage(chat_id, txt)
    except:
        sent = bot.sendMessage(chat_id, txt)
    #edited = telepot.message_identifier(sent)
    return

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

def copydbtbl(fn, sqldb, tblname):
    try:
        conn2 = sqlite3.connect(fn)        
        df = pd.read_sql_query("SELECT * FROM " + tblname, conn2)
        conn1 = sqlite3.connect(sqldb)
        df.to_sql(tblname, conn1 , index=False, if_exists="replace")
        conn1.commit()
        conn1.close()
        conn2.close()
        ok=True
    except:
        ok=False
    return ok

def csv2sqldb(csv_data, sqldb, tblname):
    try:
        con = sqlite3.connect(sqldb)
        df = pd.read_csv(csv_data)
        df.to_sql(tblname, con , index=False, if_exists="replace")
        ok = True
        con.commit()
        con.close()
    except:
        ok = False
    return ok

def decrypt(msg):
    key = '4jYUFl-wbMZ4NIiI2kG3LMFD5KTTKiT5ZiE6Yhoshp0='
    cto = Fernet(key)
    try:
        txt = msg.encode()
    except:
        txt = msg
    resp = cto.decrypt(txt)
    return resp.decode()

def edit_fields(sqldb, tbl, idx, sid, resp, edit_list=[]):
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
                updatesql(sqldb, updqry)
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

def edit_records(sqldb, tbl, idx, sid,  fld_prefix = ""):
    txt = ""
    if sid > 0:
        txt = ""
        df  = load_data(sqldb, tbl, idx, True)
        rec_match = ( df[idx] == sid ) 
        if len(df[rec_match])==0:
            rc = 0
        else:
            rc=1
            vars = []
            txt = ''
            rec_match = df[rec_match]
            for fld in list(rec_match):
                fldname = fld.lower()
                if fld_prefix in fldname :
                    fldvar = list(rec_match[fld])[0]
                    txt += fldname + ":" + str(fldvar) + "\n"
    return txt

def edx_connect(conn_str = ''):
    global edxcon
    def get_conn_str():
        connect_string = sql2var(sysconfig, "select value from params where key = 'edxapp';", "")
        # mysql://sambaash:bHyyZ3krZ4fguwcrAD7v@127.0.0.1:33306/edxapp
        return connect_string
    if conn_str == "":
        conn_str = get_conn_str()
    try:
        host = conn_str.split('@')[1].split('/')[0]
        user = conn_str.split('@')[0].split('/')[2].split(':')[0]
        pw = conn_str.split('@')[0].split('/')[2].split(':')[1]
        if ':' in host:
            port=host.split(':')[1]
            host=host.split(':')[0]
        else:
            port = "3306"
        edxcon = pymysql.connect(host=host,
                port=int(port),
                user=user,
                password=pw,
                db='edxapp',
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor)
        edxcur = edxcon.cursor()
        print("connected to EDX host : " + host)
        return 1
    except:
        print("unable to connect to EDX\n",conn_str)
        return 0

def edx_disconnect():
    global edxcon
    try:
        edxcon.commit()
        edxcon.close()        
        print("Disconnected from EDX")
    except:
        pass
    return

def edx_query(query, df_mode = False):
    global edxcon
    result = None
    try:
        edxcur = edxcon.cursor()    
        edxcur.execute(query)
        rows = edxcur.fetchall()
        if df_mode:
            result = pd.DataFrame.from_dict(rows)
        else:
            rr = dict(rows[0])
            hdr = list(rr)[0]
            result = rr[hdr]   
    except:
        pass
    return result

def edxsql(query, fn):    
    stat=edx_connect()
    if stat == 0:
        df = sql2var(nlpconfig, query, "", True)
        return 1
    df = edx_query(query, True)
    edx_disconnect()
    ok = 0
    if df is not None :
        try:
            write2html(df, title=query, filename=fn)
            ok = 1
        except:
            pass
    return ok

def email_lookup(sqldb, email):
    sid =  sql2var(sqldb, "select studentid from userdata where email = '" + email + "';", 0)
    if sid == 0:        
        qry = "select studentid, email from userdata;"
        df = querydf(sqldb, qry)
        student_list = [x for x in  df.studentid]
        email_list = [decrypt(x) for x in  df.email]        
        user_dict = dict(zip(email_list,student_list))
        if email in email_list:
            sid = user_dict[email]
    sid_str = "" if sid==0 else str(sid)        
    return sid_str

def encrypt(msg):
    key = '4jYUFl-wbMZ4NIiI2kG3LMFD5KTTKiT5ZiE6Yhoshp0='
    cto = Fernet(key)
    txt = cto.encrypt(msg.encode())
    return txt

def encrypt_email(sqldb):
    try:
        qry = "select studentid, email from userdata;"
        df = querydf(sqldb, qry)
        student_list = [x for x in  df.studentid]
        email_list = [x for x in  df.email]
        user_dict = dict(zip(student_list,email_list))
        qry = "update userdata set email = b'_x_' where studentid = _y_;"
        for sid in student_list:
            email_str = user_dict[sid]
            if email_str[:6]=='gAAAAA':
                enc_email = email_str
            else:
                enc_email = encrypt(user_dict[sid]).decode()
            query = "update userdata set email = '" + enc_email + "' where studentid = " + str(sid) + ";"
            try:
                updatesql(sqldb, query)
                #email_str = decrypt(enc_email)
                #print(sid, email_str)
            except:                
                #print(f"Error encrypting for #{sid}")
                pass
        print("Email fields encrypted")
    except:
        pass
    return

def get_attachment(bot, fid):
    fpath = bot.getFile(fid)['file_path']
    fn = "https://api.telegram.org/file/bot" + bot._token + "/"  + fpath
    fname = wget.download(fn)
    return fname

def list_table(sqldb, query, title_name):
    try:
        df = pd.read_sql(query, con = sqlite3.connect(sqldb))
        table = render_table(df, header_columns=0, col_width=3, title_name=title_name)
    except:
        return None
    return table

def load_data(fn, config, fld, df_mode):
    try:
        conn = sqlite3.connect(fn)
        fn = fn.replace(".xlsx",".db")
        query = "select * from " + config
        df = pd.read_sql_query(query, conn)
    except:
        df = None
    if df_mode:
        return df
    else:
        try:
            the_list = list(df[fld])
        except:
            the_list = []
        return the_list

def mass_encrypt_email():
    df = querydf(pbconfig, "select userdata from playbooks;")
    return [encrypt_email(x) for x in df.userdata]

def pycmd(resp, parentbot):
    vars = parentbot.vars
    if 'print' in resp:
        resp = resp.replace('print','str')
    if '"' in resp:
        resp = resp.replace('"',"'")
    try:
        if '=' in resp:
            var_name = resp.split('=')[0].strip()
            var_expr = resp.split('=')[1].strip()
            vars[var_name] = eval(var_expr)
            result = "vars['" + var_name + "'] := " + str(vars[var_name])
        else:
            result = eval(eval('f"@"'.replace('@',resp.replace('{',"{vars['").replace('}',"']}"))))
    except:
        result = ""
    parentbot.vars = vars
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

def sql2var(fn, sql, retval, dfmode=False):
    try:
        conn = sqlite3.connect(fn)
        cursor = conn.cursor()    
        df = pd.read_sql_query(sql, conn)            
        if dfmode :
            sqlvar = df.copy()
        else:
            fld = list(df)[0]
            sqlvar = df[fld].iloc[0]
            if type(retval) != type(sqlvar):                
                sqlvar = eval( str(sqlvar) )
    except:
        sqlvar = retval
    finally:
        cursor.close()
        conn.close()
    return sqlvar

def sqldb2xls(fn, sqldb, tbl_list, ind_list):
    try:
        writer = pd.ExcelWriter(fn) 
        conn = sqlite3.connect(sqldb)
        for n in range(len(tbl_list)):
            df = pd.read_sql_query(sql="SELECT * FROM " + tbl_list[n], con=conn,index_col=ind_list[n])
            df.to_excel(writer, sheet_name=tbl_list[n])
        writer.save()
        writer.close()
        ok = 1
    except:
        ok = 0
    return ok

#def text2voice(bot, chat_id, lang, resp):
#    try:
#        mp3 = 'echobot' + str(chat_id) + '.mp3'
#        myobj = gTTS(text=resp, lang=lang, slow=False)
#        myobj.save(mp3)
#        fn = convert_audio(mp3, "ogg")
#        if fn != "":
#            bot.sendAudio(chat_id, (fn, open(fn, 'rb')), title='text to voice')
#            os.remove(fn)
#        os.remove(mp3)
#    except:
#        pass
#    return

def time_hhmm(gmt):
    hh = int(datetime.datetime.now().strftime('%H'))
    mm = int(datetime.datetime.now().strftime('%M'))
    hrs = (hh+gmt+24) % 24
    return hrs*100+mm

def update_playbooklist(sqldb, course_id):
    try:
        conn = sqlite3.connect(pbconfig)
        cursor = conn.cursor()
        query = """delete from playbooks where course_id='_x_';"""
        query = query.replace("_x_", course_id)
        cursor.execute(query)
        query = """insert into playbooks(course_id,userdata) values('_x_','_y_');"""
        query = query.replace("_x_", course_id)
        query = query.replace("_y_", sqldb)
        cursor.execute(query)
        conn.commit()
    except:
        print("Unable to update playbook list")
    return

def updatesql(sqldb, updqry):
    try:
        conn = sqlite3.connect(sqldb)
        cursor = conn.cursor()
        cursor.execute(updqry)
        conn.commit()
        cursor.close()
        return True
    except:
        return False

def write2html(df, title='', filename='report.html'):    
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
    result += '<h2> %s </h2>\n' % title
    if type(df) == pd.io.formats.style.Styler:
        result += df.render()
    else:
        result += df.to_html(classes='wide', escape=False)
    result += '''
</body>
</html>
'''
    with open(filename, 'w') as f:
        f.write(result)
    return

def xls2sqldb(fn, sqldb):
    try:
        con = sqlite3.connect(sqldb)        
        try:
            wb = pd.ExcelFile(fn)
            for sheet in wb.sheet_names:
                    df = pd.read_excel(fn, sheet_name=sheet)
                    df.to_sql(sheet, con , index=False, if_exists="replace")
            ok = True
        except:
            ok = False
        con.commit()
        con.close()
    except:
        ok = False
    return ok

if __name__ == "__main__":
    #encrypt_email("FOS-1219A.db")
    fn = "edx_local.html"
    query="""select * from course_overviews_courseoverview limit 5;"""
    result = edxsql(query, fn)
    print(result)
    print("This is vmsvclib")
