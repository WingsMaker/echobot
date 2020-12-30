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
# Async task scheduler                                                  
#------------------------------------------------------------------------------------------------------
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore")

import pandas as pd
import pandas.io.formats.style
import matplotlib.pyplot as plt
import matplotlib.ticker as mtk

import calendar
import os, re, sys, time, datetime, string, random
import subprocess
import requests
import telepot
import asyncio
import telepot.aio
from telepot.namedtuple import ReplyKeyboardMarkup
from telepot.aio.loop import MessageLoop
from telepot.aio.delegate import pave_event_space, per_chat_id, create_open
import wget
import json
import wikipedia

import nltk
from nltk.chat.eliza import eliza_chatbot
from nltk.sentiment.vader import SentimentIntensityAnalyzer # nltk.download('vader_lexicon')

import vmnlplib
import vmaiglib
import vmmcqdlib
import vmsvylib     # refer to vmsvylib.py, next task is to migrate eoc survey into it
import vmsvclib
import vmedxlib
from vmsvclib import *

global bot_intance, ft_model, dt_model, mcq_analysis, survey_analysis

#use_mailapi = False
string2date = lambda x,y : datetime.datetime.strptime(x,y).date()
piece = lambda txtstr,seperator,pos : txtstr.split(seperator)[pos]
sorted_numlist = lambda y : list(set([int(x) for x in ''.join([z for z in y if z.isnumeric() or z==',']).split(',') if x!='']))
iu_reading = lambda z: ','.join([ str(x+1) for x in range(max(z))])
getnumstr = lambda q : int('0'+(( ''.join([x for x in q if x.isnumeric() or x==' '])).strip()).split(' ')[0])
getcohort = lambda x : '-'.join(x.split(':')[1].replace('+','-').split('-')[:-1][1:])

def do_main():
    global bot_intance, dt_model
    #vmsvclib.rds_connstr = "mysql://omnimentor@db-sambaashplatform-cluster-2a:omnimentor@localhost:33327/omnimentor"
    vmsvclib.rds_connstr = ""
    vmsvclib.rdscon = None
    vmsvclib.rdsdb = None
    vmsvclib.rds_pool = 0
    syslog('Starting up vmbot')
    print("starting up vmbot")
    if not loadconfig():
        syslog("error loading config")
        return
    if dt_model.model_name=="" :
        txt = "AI grading model data file dt_model.bin is missing"
        syslog('system : '+ txt)
    syslog("Running telepot async library")
    bot_intance = BotInstance()
    vmbot = bot_intance.bot
    try:
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(getbotinfo())
        loop.create_task(MessageLoop(vmbot).run_forever())
        loop.create_task(job_scheduler())
        bot_intance.loop = loop
        loop.run_forever()
    except KeyboardInterrupt:
        txt='Thank you for using OmniMentor bot. Goodbye!'
        print(txt)
        loop.close()
        os.kill(os.getpid(), 9)
    finally:
        pass
    return

def load_respdict():
    mydict = dict()
    df = rds_df("select * from progress;")
    if df is not None:
        df.columns = ['key','response']
        keys = [ x for x in df.key]
        resp = [ x for x in df.response]
        mydict=dict(zip(keys,resp))
    return mydict

async def getbotinfo():
    global bot_intance
    info = await bot_intance.bot.getMe()
    bot_intance.bot_name = info['username']
    bot_intance.bot_id = info['id']
    print('Frontend bot : ', bot_intance.bot_id)
    print('Client name : ', bot_intance.client_name)
    msg = bot_intance.bot_name + ' started running. URL is https://t.me/' + bot_intance.bot_name
    print(msg)
    return

async def job_scheduler():
    global bot_intance
    client_name = bot_intance.client_name
    resp_dict = bot_intance.resp_dict
    pass_rate = bot_intance.pass_rate
    adminchatid = bot_intance.adminchatid
    edx_cnt = 0
    edx_time = bot_intance.edx_time
    automsg = bot_intance.automsg
    gmt = bot_intance.gmt  # azure version : gmt = 8
    while True :
        timenow = time_hhmm(gmt)
        if (timenow==automsg) and (automsg>0):
            syslog("running auto_notify")
            await auto_notify(client_name, resp_dict, pass_rate, adminchatid)
            syslog("completed auto_notify, running auto_intervent")
            await auto_intervent(client_name, resp_dict, pass_rate, adminchatid)
            syslog("completed auto_intervent")
            await asyncio.sleep(60)
        if (edx_time > 0) and (timenow==edx_time) and (edx_cnt==0) :
            edx_cnt = 1
            syslog(f"running load_edxdata, edx_time={edx_time}")
            vmedxlib.load_edx(client_name)
            syslog("completed with load_edxdata")
            await asyncio.sleep(60)
        if (edx_time > 0) and (timenow > edx_time) and (edx_cnt==1):
            edx_cnt = 0
        if (list(bot_intance.job_items) == []) and (vmsvclib.rdsdb is not None):
            if (timenow % 10 == 0):
                vmsvclib.rdscon = vmsvclib.rds_connector()
                vmsvclib.rdscon.ping(True)
                vmsvclib.rdscon.close()
        else:
            await checkjoblist()
        await asyncio.sleep(1)
    return

def loadconfig():
    global ft_model, dt_model, mcq_analysis, survey_analysis
    ok = True
    try:
        dt = "dt_model.bin"
        ft = "ft_model.bin"
        ft_model = vmnlplib.NLP_Parser()
        ft_model.load_modelfile(ft)
        dt_model = vmaiglib.MLGrader()
        dt_model.load_model(dt)
        if dt_model.model_name == "":
            syslog("Error loading dt_model")
            ok = False
        mcq_analysis = vmmcqdlib.MCQ_Diff()
        survey_analysis = vmsvylib.Omni_Survey()
    except:
        ok = False
    return ok

class BotInstance():
    def __init__(self):
        global bot_intance
        bot_intance = self
        self.Token = ""
        self.bot_name = ""
        self.bot_id = ""
        self.client_name = "demo"
        self.lang = "en"
        self.bot = None
        self.loop =  None
        self.adminchatid = 0
        self.developerid = 0
        self.svcbot_id = 0
        self.automsg = 0
        self.gmt = 0
        self.pass_rate = 0.7
        self.att_rate = 0.75
        self.match_score = 0.95
        self.edx_time = 0
        self.max_iu = 20
        self.max_duration = 300
        self.use_regexpr = 1
        self.adm_list = []
        self.user_list = {}
        self.chat_list = {}
        self.job_items = {}
        self.vars = dict()
        self.keys_dict = dict()
        self.menu_keys = dict()
        self.menu_txt = dict()
        self.menu_items = dict()
        self.debug_mode = False
        self.schema = "omnimentor"
        self.super_admin_list = []
        try:
            with open("vmbot.json") as json_file:
                bot_info = json.load(json_file)
        except:
            bot_info = dict()
        if 'debug' in list(bot_info):
            self.debug_mode = (int(bot_info['debug'])==1)
        if "BotToken" in list(bot_info):
            self.Token = bot_info['BotToken']
        if "client_name" in list(bot_info):
            self.client_name = bot_info['client_name']
        if "schema" in list(bot_info):
            self.schema = bot_info['schema']
        vmsvclib.rds_schema = self.schema
        self.get_system_config()
        vmedxlib.max_iu = self.max_iu
        self.get_client_config()
        self.define_menukeys()
        try:
            syslog(self.Token)
            self.bot = telepot.aio.DelegatorBot(self.Token, [
                pave_event_space()( per_chat_id(),
                create_open, MessageCounter, timeout=self.max_duration),
            ])
        except:
            pass
        return

    def __str__(self):
        return "Telegram chatbot service class"

    def __repr__(self):
        return 'BotInstance()'

    def get_system_config(self):
        df = rds_df("select * from params where client_name = 'System';")
        if df is None:
            syslog("Unable to access params table from RDS")
            return
        df.columns = ['client_name','key', 'value', 'paramId']
        par_val = ['' + str(x) for x in df.value]
        par_key = [x for x in df.key]
        par_dict = dict(zip(par_key, par_val))
        self.resp_dict = load_respdict()
        if "superadmin" in list(par_dict):
            self.super_admin_list = [int(x) for x in par_dict['superadmin'].split(',')]
        if "ServiceBot" in list(par_dict):
            self.svcbot_id = int('0' + par_dict['ServiceBot'].split(':')[0])
        if "adminchatid" in list(par_dict):
            self.adminchatid = int(par_dict['adminchatid'])
        if "developerid" in list(par_dict):
            self.developerid = int(par_dict['developerid'])
        if "max_duration" in list(par_dict):
            self.max_duration = int(par_dict['max_duration'])
        if "GMT" in list(par_dict):
            self.gmt = int(par_dict['GMT'])
        if "regexpr" in list(par_dict):
            self.use_regexpr = int(par_dict['regexpr'])
        if "match_score" in list(par_dict):
            self.match_score = float(par_dict['match_score'])
        if "pass_rate" in list(par_dict):
            self.pass_rate = float(par_dict['pass_rate'])
        if "att_rate" in list(par_dict):
            self.att_rate = float(par_dict['att_rate'])
        if "max_iu" in list(par_dict):
            self.max_iu = int(par_dict['max_iu'])
        if self.client_name == "":
            self.client_name = par_dict['client_name']
        return

    def get_client_config(self):
        df = rds_df(f"select * from params where client_name = '{self.client_name}';")
        if df is None:
            syslog("Unable to access params table from RDS")
            return
        df.columns = ['client_name','key', 'value', 'paramId']
        par_val = ['' + str(x) for x in df.value]
        par_key = [x for x in df.key]
        par_dict = dict(zip(par_key, par_val))
        email_filter = 'sambaash.com'
        if "email_filter" in list(par_dict):
            email_filter = par_dict['email_filter']
        self.efilter = email_filter.split(',')
        self.edx_time = int(par_dict['edx_import']) if 'edx_import' in par_key else 0
        self.automsg = int(par_dict['automsg']) if 'automsg' in par_key else 0
        self.stages_cols = get_columns("stages")
        self.userdata_cols = get_columns("userdata")
        self.usermaster_cols = get_columns("user_master")
        hdr = par_dict['edx_api_header']
        self.edx_api_header = eval(hdr)
        self.edx_api_url = 'http://localhost:8080/edx/v1'
        if "edx_api_url" in list(par_dict):
            self.edx_api_url = par_dict['edx_api_url']
        vmedxlib.edx_api_url = self.edx_api_url
        vmedxlib.edx_api_header = self.edx_api_header
        if "lang" in list(par_dict):
            self.lang = par_dict['lang']
        if self.Token == "":
            self.Token = par_dict['BotToken']
        return

    def define_menukeys(self):
        mtxt = lambda x :  [[self.menu_txt[z] for z in y.split(',')] for y in x.replace('\n','').strip().split(';')]
        button_list = lambda x : str(x).replace('[','').replace(']','').replace(", ",",").replace("'","").split(',')
        df1 = rds_df(f"select * from menu_text where lang='{self.lang}' and menu_key <> '';")
        df2 = rds_df(f"select * from menu_group where menu_name <> '' order by id;")
        if (df1 is None) or (df2 is None):
            syslog("Unable to access related menu tables from RDS")
            return
        df1.columns = ['menu_key', 'lang', 'text']
        df2.columns = ['id', 'menu_name', 'menu_key', 'text']
        key_list = [x for x in df1.menu_key]
        txt_list = [x for x in df1.text]
        self.menu_keys = dict(zip(txt_list,key_list))
        self.menu_txt = dict(zip(key_list,txt_list))
        name_list = [x for x in df2.menu_name]
        key_list = [x for x in df2.menu_key]
        txt_list = [mtxt(x) for x in df2.text]
        self.menu_items = dict(zip(name_list,txt_list))
        menu_root = dict(zip(name_list,key_list))
        self.keys_dict[self.menu_txt['option_mainmenu']] = 1
        for mnu in list(self.menu_items):
            txt = menu_root[mnu]
            if txt!="":
                mk = (self.keys_dict[self.menu_txt[txt]])*100 + 1
                for mt in button_list(self.menu_items[mnu]):
                    self.keys_dict[mt] = mk
                    mk += 1
        idx = max([ self.keys_dict[x] for x in self.keys_dict]) + 1000
        for mnu in self.menu_txt:
            txt = self.menu_txt[mnu]
            if txt not in self.keys_dict:
                self.keys_dict[txt] = idx
                idx += 1
        return

class MessageCounter(telepot.aio.helper.ChatHandler):
    def __init__(self, *args, **kwargs):
        super(MessageCounter, self).__init__(*args, **kwargs)
        self.new_session = True
        self.is_admin = False
        self.super_admin = False
        self.client_name = ""
        self.chatid = 0
        self.chatname = ""
        self.binded = 0
        self.student_id = 0
        self.email = ''
        self.username = ''
        self.userdata = ''
        self.courseid = ''
        self.coursename = ''
        self.stagetable = ''
        self.stage_name = ''
        self.stagedate = ''
        self.stage_list = []
        self.stage_days = []
        self.tablefields = ''
        self.edited = 0
        self.tableindex = 0
        self.tablerows = []
        self.list_courseids = []
        self.list_coursename = []
        self.course_selection = []
        self.mentor_email = ""
        self.asst_email = ""
        self.records = dict()
        self.menu_id = 0
        self.menu_home = []
        self.connection_dict = dict()

    def reset(self):
        self.__init__()
        return

    async def logoff(self, txt = "Have a great day!"):
        global bot_intance
        try:
            if self.chatid in [d for d in bot_intance.user_list]:
                bot_intance.user_list.pop(self.chatid)
            if self.student_id in bot_intance.adm_list:
                bot_intance.adm_list.remove(self.student_id)
        except:
            pass
        try:
            await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup([[bot_intance.menu_txt['btn_hellobot']]]))
        except:
            pass
        syslog(f"telegram user {self.chatid} logged out.")
        self.new_session = True
        self.chatid = 0
        self.student_id = 0
        self.reset
        self.menu_id = 0
        return

    async def on_close(self, exception):
        await self.logoff()
        txt = 'session time out.\nPress /start a few times to awake this bot.'
        await self.sender.sendMessage(txt)
        return

    def mcqas_chart(self, groupcht = False ):
        global bot_intance
        if self.userdata is None:
            return (None,None)
        avg_list = dict()
        if groupcht:
            cohort_id = getcohort(self.courseid)
            fn = 'chart_' + cohort_id + '.png'
            title = f"MCQ and Assignment scores for cohort {cohort_id}"
            df = self.userdata
        else:
            sid = self.student_id
            if sid == 0:
                return (None,None)
            df = self.userdata[self.userdata.studentid==sid]
            fn = 'chart_' + str(sid) + '.png'
            title = f"MCQ and Assignment scores for student #{sid}"
        mcq_cnt = vmedxlib.edx_mcqcnt(self.courseid, self.client_name)
        as_cnt = vmedxlib.edx_ascnt(self.courseid, self.client_name)
        iu_cnt = max(mcq_cnt,as_cnt)
        if iu_cnt == 0:
            return (None,None)

        qry = "SELECT m.iu FROM module_iu m INNER JOIN playbooks p ON m.client_name=p.client_name AND "
        qry += "m.pillar=p.pillar AND m.course_code=p.course_code AND m.module_code=p.module_code WHERE "
        qry += f"m.mcq>0 and p.course_id = '{self.courseid}' AND p.client_name='{self.client_name}';"
        df1 = rds_df(qry)
        if df1 is None:
            return (None,None)
        df1.columns = ['iu']
        mcq_mask = [x for x in df1.iu]

        qry = "SELECT m.iu FROM module_iu m INNER JOIN playbooks p ON m.client_name=p.client_name AND "
        qry += "m.pillar=p.pillar AND m.course_code=p.course_code AND m.module_code=p.module_code WHERE "
        qry += f"m.assignment>0 and p.course_id = '{self.courseid}' AND p.client_name='{self.client_name}';"
        df2 = rds_df(qry)
        if df2 is None:
            return (None,None)
        df2.columns = ['iu']
        as_mask = [x for x in df2.iu]

        avg_list = dict()
        for avgopt in ['mcq_avg','as_avg']:
            avg_list[avgopt] = [ df[ avgopt + str(x) ].mean() for x in range(1,iu_cnt + 1)]
            for n in range(len(avg_list[avgopt]),iu_cnt):
                avg_list[avgopt].append(0.0)
        df1 = pd.DataFrame({
            'Test/IU' : [ '#' + str(n) if n in mcq_mask else 'NA' for n in range(1,iu_cnt + 1) ],
            'test score' : [ avg_list['mcq_avg'][n-1]*100 if n in mcq_mask else 0 for n in range(1,iu_cnt + 1) ]
        })
        df2 = pd.DataFrame({
            'Test/IU' : [ '#' + str(n) if n in as_mask else 'NA' for n in range(1,iu_cnt + 1) ],
            'test score' : [ avg_list['as_avg'][n-1]*100 if n in as_mask else 0 for n in range(1,iu_cnt + 1) ]
        })
        df3 = pd.DataFrame({
            'Test/IU' : [ '' ],
            'test score' : [ 0 ]
        })
        df = pd.concat([df1, df2, df3])
        colors = {'mcq':'deepskyblue', 'assignment':'orange'}
        mycolor = [colors['mcq'] for x in range(iu_cnt)] + [colors['assignment'] for x in range(iu_cnt)] + ['white']
        df.set_index('Test/IU').plot(kind='bar',figsize=(10,4), rot = 90)
        plt.title(title)
        ax = plt.gca()
        plt.bar( [x for x in range(len(df))], [ x for x in df['test score']], color = mycolor)
        cols = [c for c in df.columns]
        label_col = cols[0]
        xcol = cols[1]
        label_list = [ x for x in df[label_col] ]
        width = 0.8
        plt.xlim([-width, len(df[xcol])-width])
        ax.set_xticklabels((label_list))
        ax.yaxis.set_major_formatter(mtk.PercentFormatter())
        clabels = list(colors.keys())
        handles = [plt.Rectangle((0,0),1,1, color=colors[label]) for label in clabels]
        plt.legend(handles, clabels, bbox_to_anchor=(0.8,1.1), loc="upper left")
        plt.axhline(y=bot_intance.pass_rate * 100, xmin=0, xmax=1, color='red')
        plt.draw()
        plt.savefig(fn, dpi=100)
        plt.clf()
        f = open(fn, 'rb')
        if groupcht:
            df = pd.DataFrame({
                'Test/IU' : [ '#' + str(n) for n in range(1,iu_cnt + 1) ],
                'mcq test' : [ "{:.2%}".format(avg_list['mcq_avg'][n-1]) if n in mcq_mask else 'NA' for n in range(1,iu_cnt+1) ],
                'assignment test' : [ "{:.2%}".format(avg_list['as_avg'][n-1]) if n in as_mask else 'NA' for n in range(1,iu_cnt+1) ]
            })
            return (df,f)
        del df
        return (None, f)

    def session_info(self):
        global bot_intance
        txt = "Summary:\n"
        if self.is_admin :
            txt += "\nYou are the faculty adm\n"
            txt += "Student id : " + str(self.student_id) + "\n\n"
            if len(bot_intance.user_list)==0:
                txt += 'No students online\n'
            else:
                cnt = len(bot_intance.user_list)
                txt += f"{cnt} students online:\n"
        else:
            if self.new_session:
                txt += '\nsession already logged out.'
            else:
                txt += '\nStudent id : ' + str(self.student_id)
                txt += '\nCourse ID : ' + self.courseid
                txt += '\nCourse Name : ' + self.coursename
                cc = [x for x in self.list_courseids if self.courseid != x]
                if len(cc)>0:
                    txt += "\nOther courses (cohort-id):\n"
                    for x in cc:
                        txt += "\t" + x + "\n"
                txt += '\nLearning Stage : ' + self.stage_name
                txt += '\nOutstanding Amount : ' + str(self.records['amt']) + "\n"
                txt += '\nMentor email : ' + self.mentor_email
                txt += '\nLA email : ' + self.asst_email
        txt += f"\nYour username is {self.username}"
        txt += f"\nYour email is {self.email}"
        txt += f"\nYour telegram chat_id is {self.chatid}"
        txt += f"\nYour telegram chat name is {self.chatname}"
        txt += f"\nSystem client name is {self.client_name}"
        return txt

    def userinfo(self, sid):
        global bot_intance
        txt = "Unable to find the matching record. Please try again."
        if sid > 0:
            query = f"select * from user_master where client_name = '{self.client_name}' and studentid = {sid} ;"
            df = rds_df(query)
            if df is None:
                self.student_id = 0
                return txt
        if sid == 0:
            return txt
        else:
            df.columns = bot_intance.usermaster_cols
            rec = df.iloc[0]
            sid = rec['studentid']
            self.student_id = sid
            username = rec['username']
            email = rec['email']
            tid = rec['chat_id']
            binded = 'Yes' if rec['binded']==1 else 'No'
            telegid = str(tid) if rec['binded']==1 else 'None'
            txt = f"Student-ID : #{sid}\nName : {username}\nEmail : {email}\nBinded :{binded}\nTelegramID : {telegid}\n"
            query = "select distinct a.courseid from userdata a inner join user_master b "
            query += " on a.client_name=b.client_name  and a.studentid=b.studentid where "
            query += f" a.client_name = '{self.client_name}' and a.studentid={sid} "
            query += "order by a.courseid;"
            df = rds_df(query)
            if df is not None:
                df.columns = ['courseid']
                n=len(df)
                if n > 0:
                    if n <=10:
                        txt += "Courses:\n" + '\n'.join([x for x in df.courseid])
                    else:
                        txt += f"You have {n} courses registered"
            txt += "\nWhat would you like to do ?"
        return txt

    def mentor_chatid(self):
        # read from mentor_email of course_master table ?
        query = "select u.chat_id from playbooks p inner join user_master u on p.client_name=u.client_name "
        query += f"and p.mentor=u.email where p.course_id='{self.courseid}' and p.client_name = '{self.client_name}';"
        mentorchatid = rds_param(query)
        if mentorchatid == '':
            return 0
        return int(mentorchatid)

    def livechat(self, sid=0, telegram_id=0):
        global bot_intance
        list_learners = []
        cnt = 0
        if self.is_admin:
            cnt = len(bot_intance.adm_list) - 1
        cnt += len(bot_intance.user_list)
        if cnt==0:
            return (0, "There is no online users at the moment")
        elif sid>0:
            sid_list = []
            tlist = [x for x in list(bot_intance.user_list) if bot_intance.user_list[x][1] == sid]
            if self.is_admin:
                sid_list = [x for x in bot_intance.adm_list if x == sid]
            if (tlist == []) and (sid_list == []):
                txt = f"User #{sid} is not online at the moment"
                return (0,txt)
            else:
                if sid in bot_intance.adm_list:
                    query = f"select username, chat_id from user_master where client_name = '{self.client_name}' and studentid = {sid};"
                    df = rds_df(query)
                    if df is None:
                        txt = f"User #{sid} is not online at the moment"
                        return (0,txt)
                    df.columns = ['username','chat_id']
                    tname = df.username.values[0]
                    tid = df.chat_id.values[0]
                    info = [sid, tname, tid]
                    return (3, info)
                else:
                    if self.chatid in bot_intance.user_list:
                        user_from = bot_intance.user_list[self.chatid][2]
                    else:
                        user_from = self.username
                    if telegram_id==0:
                        tid = tlist[0]
                    else:
                        tid = telegram_id
                    tname = bot_intance.user_list[tid][2]
                    bot_intance.chat_list[tid] = self.chatid
                    bot_intance.chat_list[self.chatid] = tid
                    return (1, tname)
        else:
            if self.is_admin:
                list_learners = [ ['     '.join([str(d) for d in bot_intance.user_list[r]])] for r in bot_intance.user_list ]
                sid_list = [x for x in bot_intance.adm_list if x != self.student_id]
                if sid_list==[]:
                    df = None
                else:
                    sid_list = list(set(sid_list))
                    slist = ','.join([str(x) for x in sid_list])
                    query = f"select studentid,username,chat_id from user_master where client_name = '{self.client_name}' and studentid in ({slist}) limit 50;"
                    df = rds_df(query)
                if df is None:
                    sid_list = []
                    uname_list = []
                    tid_list = []
                else:
                    df.columns = ['studentid','username','chat_id']
                    sid_list = [x for x in df.studentid]
                    uname_list = [x for x in df.username]
                    tid_list = [x for x in df.chat_id]
                cnt = len(sid_list)
                for n in range(cnt):
                    txt = "Mentor/Admin     " + str(sid_list[n]) + "     " + uname_list[n] + "     " + str(tid_list[n])
                    list_learners.append([txt])
            else:
                list_learners = [ ['     '.join([str(d) for d in bot_intance.user_list[r]])] for r in bot_intance.user_list \
                    if (bot_intance.user_list[r][0] == self.courseid) and (bot_intance.user_list[r][1] != self.student_id) ]
            if len(list_learners) > 0:
                txt = 'Chat with online learners 🗣'
                list_learners = list_learners + [ [ bot_intance.menu_txt['option_back'] ] ]
                return(2, self.reply_markup(list_learners) )
            else:
                txt = "There is no online learners at the moment"
                return (0,txt)
        return

    def endchat(self):
        global bot_intance
        chat_id = self.chatid
        chat_found = False
        tid = 0
        if chat_id in bot_intance.chat_list:
            chat_found = True
            tid = bot_intance.chat_list[chat_id]
            try:
                bot_intance.chat_list.pop(chat_id)
                if tid != chat_id:
                    bot_intance.chat_list.pop(tid)
            except:
                pass
            for c in [chat_id , tid] :
                if c in list(bot_intance.user_list) :
                    bot_intance.user_list[ c ][4] = ""
        return tid

    async def runfaq(self, resp):
        global ft_model, bot_intance
        accuracy = 0
        match_score = bot_intance.match_score
        use_regexpr = bot_intance.use_regexpr
        user_resp = resp.lower()
        txt = ""
        if use_regexpr == 1 :
            txt = ft_model.match_resp(resp)
            if (txt != ''):
                syslog( "REG : " + txt )
        if txt == '':
            (result, accuracy) = ft_model.get_response(resp)
            if accuracy >= match_score:
                txt = result
                syslog( "NLP : " + txt )
        if txt == '':
            ( result, accuracy ) = ft_model.find_matching( resp )
            if accuracy > 0:
                if accuracy < match_score:
                    txt = 'do you mean this ? =>\n' + result
                    await self.sender.sendMessage(txt)
                    user_resp = result.lower()
                txt = ft_model.match_resp(user_resp)
                if (txt != ''):
                    syslog( "REG : " + txt )
        if txt == '':
            txt = eliza_chatbot.respond(resp)

        if (txt != '') and re.search('.*\{.*', txt):
            stagedate = str(self.stagedate)
            mcqlist = str(self.records['mcqlist']) if 'mcqlist' in list(self.records) else ''
            aslist = str(self.records['aslist']) if 'aslist' in list(self.records) else ''
            amt = str( self.records['amt'] )
            mcqas_chart = ""
            mcqdate = self.records['mcqdate']
            asdate = self.records['asdate']
            eldate = self.records['eldate']
            fcdate = self.records['fcdate']
            wikimode = False
            if '{wiki}' in txt:
                wikimode = True
                txt = txt.replace('{wiki}' , '')
            if '{lf}' in txt:
                txt = txt.replace('{lf}' , '\n')
            if '{sos}' in txt:
                bot_intance.user_list[ self.chatid ][4] = "👋"
                txt = txt.replace('{sos}' , '')
            if '{mcqas_chart}' in txt:
                (df,f) = self.mcqas_chart()
                if f is not None:
                    await self.bot.sendPhoto(self.chatid, f)
                txt = txt.replace('{mcqas_chart}' , '')
            if '{mcq_att_balance}' in txt:
                result = self.track_attempts()
                txt = txt.replace('{mcq_att_balance}' , result)
            if '{mentor_email}' in txt:
                txt = txt.replace('{mentor_email}' , self.mentor_email)
            if '{assistance_email}' in txt:
                txt = txt.replace('{assistance_email}' , self.asst_email)
            txt = eval("f'@'".replace('@', txt.replace('\n','~~~'))).replace('~~~','\n')
            if wikimode:
                result = resp.lower().replace('who is','').replace('what is','').replace('?','')
                txt = 'may be you can google search for it.'
                try:
                    txt = str(wikipedia.summary(result, sentences=2))
                except:
                    pass

        if txt == '':
            txt = "I'm sorry, I do not understand you but could you be more specific about your question related to "
            txt += "'" + resp + "' ?"

        recommendation = ft_model.recommend_list(resp)
        return (txt, recommendation)

    def reload_userdata(self):
        global bot_intance
        client_name = bot_intance.client_name
        qry = "SELECT u.* FROM userdata u INNER JOIN user_master m ON u.client_name=m.client_name AND u.studentid=m.studentid "
        qry += f" WHERE u.deferred = 0 and u.client_name='{client_name}' AND u.courseid = '{self.courseid}' "
        qry += ''.join([ " and lower(m.email) not like '%" + x + "'"  for x in bot_intance.efilter])
        df = rds_df( qry )
        if df is not None:
            df.columns = bot_intance.userdata_cols
            self.userdata = df
        return

    def load_tables(self):
        global bot_intance
        client_name = bot_intance.client_name
        syslog(f"mcq update, assignment update {self.courseid} student #{self.student_id}")
        if not bot_intance.debug_mode:
            if self.is_admin:
                job_request(self.chatid,self.client_name,"update_assignment",self.courseid)
                job_request(self.chatid,self.client_name,"update_mcq",self.courseid)
                job_request(self.chatid,self.client_name,"update_schedule",self.courseid)
            else:
                vmedxlib.update_mcq(self.courseid, client_name, self.student_id) #it takes 5 seconds
                vmedxlib.update_assignment(self.courseid, client_name, self.student_id) #it takes 3 seconds
        self.reload_userdata()
        qry = "select * from stages where client_name = '_c_' and courseid = '_x_';"
        qry = qry.replace('_c_', self.client_name)
        qry = qry.replace('_x_', self.courseid)
        df = rds_df( qry)
        if df is None:
            self.stagetable = None
        else:
            df.columns = bot_intance.stages_cols
            self.stagetable = df
        syslog("end of update")
        return

    def load_courseinfo(self, resp):
        if len(self.list_courseids)==0:
            return 0
        self.courseid = ""
        self.userdata = ""
        self.coursename = ""
        ok = 0
        if resp in self.list_courseids:
            n = self.list_courseids.index(resp)
            self.coursename = self.list_coursename[n]
            self.courseid = resp
            self.load_tables()
            ok = 1
        return ok

    def check_student(self, sid, chat_id):
        global bot_intance
        self.student_id = 0
        txt = ''
        if self.new_session == False :
            return ('', [])
        if self.userdata is None:
            return ('', [])
        syslog("verify_student")
        (txt, self.records) = verify_student(self.client_name, self.userdata, sid, self.courseid, None)
        self.mentor_email = self.records['mentor_email']
        self.asst_email = self.records['asst_email']
        if sid > 0:
            cid = rds_param(f"select chat_id FROM user_master where client_name='{self.client_name}' and studentid={sid};")
            binded = self.binded
            if binded==0 and cid==0:
                query = f"update user_master set chat_id={chat_id} where client_name='{self.client_name}' and studentid={sid};"
                rds_update(query)
        err = 0
        if (self.records=={}) :
            txt = "Hi, there is incomplete information at the moment, the session is not ready yet.\n\n"
            txt += "Please select the course from below list."
            btn_course_list = build_menu(self.list_courseids,1)
            self.menu_id = bot_intance.keys_dict[bot_intance.menu_txt['option_learners']]
            return (txt, btn_course_list)
        elif self.records['deferred']==1:
            updqry = f"update user_master set binded=0 where client_name='{self.client_name}' and studentid={sid};"
            rds_update(updqry)
            txt += "\nClick /start to start a new session"
            self.list_courseids.remove(self.courseid)
            btn_course_list = build_menu(self.list_courseids,1)
            self.menu_id = bot_intance.keys_dict[bot_intance.menu_txt['option_learners']]
            return (txt, btn_course_list)
        else:
            self.is_admin = False
            self.student_id = sid
            self.new_session = False
            bot_intance.user_list[chat_id]=[self.courseid, self.student_id, self.username, chat_id, ""]
            self.records = load_vars(self.userdata, sid)
            vars = display_progress(self.userdata, self.stagetable, sid, self.records, self.client_name, bot_intance.resp_dict, bot_intance.pass_rate, bot_intance.att_rate)
            for v in list(vars):
                self.records[v] = vars[v]
            txt  = vars['notification']
            tlist = txt.split('\n')
            txt = '\n'.join(tlist[1:])
            self.stage_name = vars['stage']
            if vars['course_alive'] == 1:
                mcq_cnt = vmedxlib.edx_mcqcnt(self.courseid, self.client_name)
                as_cnt = vmedxlib.edx_ascnt(self.courseid, self.client_name)
                vars['iu_cnt'] = max(mcq_cnt,as_cnt)
                txt += grad_pred_text(vars, self.client_name)
            if txt == "":
                txt = "Welcome back."
            self.menu_id = bot_intance.keys_dict[bot_intance.menu_txt['lrn_student']]
            txt = "Hi" + ", " + self.username + " !\n" + txt
            return (txt, self.menu_home)
        syslog("completed")
        return ('', [])

    def track_attempts(self):
        global bot_intance
        vars = load_vars(self.userdata, self.student_id)
        (t0, t1, vars) = load_progress(self.userdata, self.student_id, vars, self.client_name, bot_intance.resp_dict, bot_intance.pass_rate, bot_intance.att_rate, self.stagetable)
        if vars['mcq_att_balance'] == "":
            txt = "There is no outstand MCQs for futher attempts."
        else:
            txt = vars['mcq_att_balance']
        return txt

    def grad_prediction(self):
        global dt_model, bot_intance
        if dt_model.model_name == "" :
            syslog("please load the model first")
            return ( [] , None )
        txt = ""
        df0 = self.userdata
        if self.userdata is None:
            syslog("there is no data")
        df = df0[df0.deferred == 0]
        list_sid = [str(x) for x in df.studentid]
        if len(list_sid)==0:
            return ( [] , None )
        client_name = list(df['client_name'])[0]
        courseid = list(df['courseid'])[0]
        mcq_cnt = vmedxlib.edx_mcqcnt(courseid, client_name)
        as_cnt = vmedxlib.edx_ascnt(courseid, client_name)
        iu_cnt = max(mcq_cnt,as_cnt)
        max_iu_cnt = bot_intance.max_iu
        iu_cnt = max_iu_cnt if iu_cnt > max_iu_cnt else iu_cnt
        progress_df = dict()
        progress_tt = dict()
        tbl = []
        new_sidlist = []
        syslog(courseid)
        for vv in list_sid:
            sid = int(vv)
            vars = load_vars(self.userdata, sid)
            uu = vars['username']
            fw = lambda u,v : f"\nTest Results for Student #{v} {u}\n    MCQ Tests\t\tAssignment Tests\n"
            fx = lambda n : vars["mcq_avg"+str(n)]
            fy = lambda n : vars["as_avg"+str(n)]
            gw = lambda n : vars["mcq_attempts"+str(n)]
            gx = lambda n : 0 if fx(n)==0 else (1 if gw(n)==0 else gw(n))
            gy = lambda n : vars["as_attempts"+str(n)]
            gz = lambda n : gx(n) + gy(n) if gx(n) is not None and  gy(n) is not None else 0
            fz = lambda n : [ "#" + str(n) , "{:.2%}".format(fx(n)), str(gx(n)), "{:.2%}".format(fy(n)) , str(gy(n)) ]
            mscores = [fx(n) for n in range(1,iu_cnt + 1) if gx(n) > 0]
            ascores = [fy(n) for n in range(1,iu_cnt + 1) if gy(n) > 0]
            mcnt = len(mscores)
            acnt = len(ascores)
            grades = 0
            mavg = 0
            aavg = 0
            if (mcnt+acnt)>0:
                mavg = 0 if mcnt == 0 else sum(mscores) / mcnt
                aavg = 0 if acnt == 0 else sum(ascores) / acnt
                grad_pred = dt_model.predict(mavg , aavg, mcnt, acnt)
                grades = grad_pred[0]

            progress_list = [ fz(n) for n in range(1,max_iu_cnt + 1) if gz(n) > 0]
            progress_list.append(['Avg', "{:.2%}".format(mavg) , " ",  "{:.2%}".format(aavg) , " "])

            df =  pd.DataFrame( progress_list )
            df.columns = ['Test #', 'MCQ', '#Attempts', 'Assignment', '# Attempts']
            progress_df[sid] = df

            progress_tt[sid] = f"\nTest Results for Student #{vv} {uu}"
            new_sidlist.append(str(sid))
            tbl.append( [vv , uu , "{:.2%}".format(grades)])
        self.records['progress_df'] = progress_df
        self.records['progress_tt'] = progress_tt
        df1 =  pd.DataFrame( tbl )
        if len(df1) == 0:
            txt = "There is no data for this course id"
            syslog(txt)
            return ( [] , None )
        df1.columns = ['Student ID#','Name', 'Prediction']
        df1= df1.sort_values(by ='Prediction')
        syslog("completed")
        return (new_sidlist , df1)

    def find_course(self, stud):
        client_name =  self.client_name
        course_id_list = []
        course_name_list = []
        logon_list = []
        date_today = datetime.datetime.now().date()
        yr_str = str(date_today.strftime('%Y'))
        yrnow = yr_str[-2:]
        if client_name == "":
            return (course_id_list, course_name_list, logon_list)
        syslog(f"student #{stud}")
        if stud==0:
            qry = f"SELECT DISTINCT a.course_id, a.course_name FROM playbooks a "
            qry += f"INNER JOIN course_module c ON a.client_name=c.client_name and a.module_code=c.module_code "
            qry += f"where a.eoc=0 and c.enabled=1 AND a.client_name='{client_name}' and SUBSTRING(course_id,-2)='{yrnow}' ORDER BY a.course_id;"
            df = rds_df(qry)
            if df is None:
                syslog("There is no courses found")
            else:
                df.columns = ['course_id','course_name']
                course_id_list = [x for x in df.course_id]
                course_name_list = [x for x in df.course_name]
            return (course_id_list, course_name_list, course_id_list)
        (course_id_list, course_name_list, logon_list) = rds_loadcourse(client_name, stud)
        syslog("completed")
        return (course_id_list, course_name_list, logon_list)

    def pycmd(self, cmdstr):
        result = ""
        try:
            pycode = compile(cmdstr, 'test', 'eval')
            result = str(eval(pycode))
        except:
            result = ""
        return result

    def reply_markup(self, buttons=[], opt_resize = True):
        global bot_intance
        mark_up = None
        if buttons == []:
            mark_up = {'hide_keyboard': True}
        else:
            mark_up = ReplyKeyboardMarkup(keyboard=buttons,one_time_keyboard=True,resize_keyboard=opt_resize)
        return mark_up

    async def on_chat_message(self, msg):
        global ft_model, dt_model, mcq_analysis, bot_intance
        try:
            content_type, chat_type, chat_id = telepot.glance(msg)
            syslog(f"incoming msg : {content_type} {chat_type} {chat_id}")
            bot = self.bot
            self.chatid = chat_id
            keys_dict = bot_intance.keys_dict
            menu_keys = bot_intance.menu_keys
            menu_txt = bot_intance.menu_txt
            menu_items = bot_intance.menu_items
            option_back = menu_txt['option_back']
            adminchatid = bot_intance.adminchatid
            developerid = bot_intance.developerid
            lang = bot_intance.lang
            self.super_admin = False
        except:
            return

        resptxt = ""
        resp = ""
        txt = ""
        retmsg = ""
        title = ""
        html_msg_dict = dict()
        if content_type == 'text':
            resp = msg['text'].strip()
            resptxt = resp.lower()
            if 'from' in list(msg):
                username = msg['from']['first_name']
                self.records['username'] = username
                self.chatname = username
                if self.username=="":
                    self.username = username
        elif content_type != "text":
            txt = "Thanks for the " + content_type + " but I do not need it for now."
            await self.sender.sendMessage(txt)
            return
        else:
            syslog( f"{content_type} , {msg}" )

        if (chat_id <0) and (adminchatid>0):
            txt = resp + " from " + str(chat_id)
            await self.bot.sendMessage(adminchatid, txt)
            return

        if (resp not in list(keys_dict)) and (resp != option_back) and (resp[0]!='/'):
            syslog(f"chat_id = {self.chatid} response = {resp}")

        if resp=='/end':
            tid = self.endchat()
            if tid > 0:
                txt = "Live chat session disconnected. 👋"
                await self.bot.sendMessage(tid, txt)
                await self.bot.sendMessage(chat_id, txt)
            txt = "sesson closed."
            await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup([['/start']]))
            self.is_admin = (chat_id == adminchatid)
            syslog("telegram user " + str(chat_id) + " offine.")
            await self.logoff()

        elif resp.startswith('/?'):
            if len(resp)>2:
                txt = '<a href="tg://user?id=' + resp[2:] + '">Click me to find out who is this user.</a>'
                await bot.sendMessage(chat_id, txt, parse_mode='HTML')
            else:
                txt = 'Your chat_id is ' + str(chat_id)
                await self.sender.sendMessage(txt)
                return

        # ================================================================================================
        #elif resp == menu_txt['an_survey']: 
        elif resp in ["/testeoc","/testpss"]:
            self.client_name = bot_intance.client_name
            self.courseid = "course-v1:Lithan+CCN-1119A+28Feb20"
            table_name = "tb_pss" if resp=="/testpss" else "tb_eoc"
            tname = "PSS" if resp=="/testpss" else "EOC"
            if not survey_analysis.load_data(self.client_name, self.courseid, table_name):
                txt = "There is no information available at the moment"
                await self.sender.sendMessage(txt)
                return
            await self.sender.sendMessage(f"{tname} survey data has been loaded")
            
            ( f, txt ) = survey_analysis.feedback_by_index()
            if f is not None:
                await self.bot.sendPhoto(self.chatid, f)
            if txt != "":
                await bot.sendMessage(chat_id, txt, parse_mode='HTML')
                
            ( f, txt ) = survey_analysis.feedback_by_votes()
            if f is not None:
                await self.bot.sendPhoto(self.chatid, f)
            if txt != "":
                await bot.sendMessage(chat_id, txt, parse_mode='HTML')

            msg = survey_analysis.feedback_by_comments()
            if msg != []:
                for txt in msg:
                    await bot.sendMessage(chat_id, txt, parse_mode='HTML')
        # ================================================================================================

        elif resp=='/stop' and (chat_id in [adminchatid, developerid]):
            txt = 'System shutting down.'
            for d in bot_intance.user_list:
                await bot.sendMessage(d, txt)
            await self.logoff()
            result ='<pre> ▀▄▀▄▀▄ OmniMentor ▄▀▄▀▄▀\n Powered by Sambaash</pre>\nContact <a href=\"@OmniMentor">@OmniMentor</a>'
            await bot.sendMessage(chat_id,result,parse_mode='HTML')
            syslog('system : ' + txt)
            bot_intance.loop.stop()

        elif resp == '/start' or resp == '/hellobot' or resp == menu_txt['btn_hellobot']:
            self.reset
            self.new_session = True
            self.is_admin = (chat_id == adminchatid)
            self.menu_home = menu_items['learners_menu']
            self.edited = 0
            self.client_name = bot_intance.client_name
            if chat_id <= 0 :
                # chatgroup support not ready
                return
            tid = self.endchat()
            if tid > 0:
                txt = "Live chat session disconnected. 👋"
                await self.bot.sendMessage(tid, txt)
                await self.bot.sendMessage(chat_id, txt)
            syslog("system : " + "telegram user " + str(chat_id) + " online.")
            if (bot_intance.svcbot_id==bot_intance.bot_id):
                if (chat_id in bot_intance.super_admin_list):
                    self.email = rds_param(f"SELECT email FROM user_master WHERE chat_id={chat_id} and client_name ='{self.client_name}' AND email LIKE '%sambaash.com';")
                    self.super_admin = True
                    self.is_admin = True
                    usertype = 21
                    self.menu_id = 1
                    self.menu_home = menu_items['svcbot_menu']
                    (self.list_courseids, self.list_coursename,self.course_selection) = self.find_course(0)
                    menu_item = self.menu_home.copy()
                    txt = banner_msg("Welcome " + self.username,f"You are now connected to client {self.client_name}.")
                    await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                else:
                    txt = "Sorry your account is blocked, please contact the super admin."
                    await self.sender.sendMessage(txt)
                    await self.logoff()
                return            
            query = "select * from user_master where binded=1 and chat_id =" + str(chat_id) + " and client_name = '" + self.client_name + "';"
            df = rds_df(query)
            if df is None:
                sid = 0
                self.student_id = 0
                usertype = 0
                courseid = ""
                binded = 0
            else:
                df.columns = bot_intance.usermaster_cols
                sid = int(df.studentid.values[0])
                self.student_id = sid
                self.username = df.username.values[0]
                usertype = df.usertype.values[0]
                courseid = df.courseid.values[0]
                binded =  df.binded.values[0]
                self.email = df.email.values[0]
                self.binded = binded
                if (usertype == 1) and (courseid == ""):
                    binded = 0
            txt = "Please wait for a while."
            try:
                await self.sender.sendMessage(txt)
            except:
                pass
            if (binded==1) and (sid > 0):
                if usertype == 1:
                    (self.list_courseids, self.list_coursename,self.course_selection) = self.find_course(sid)
                    query = f"select course_name from playbooks where course_id ='{courseid}' and client_name='{self.client_name}';"
                    coursename = rds_param(query)
                    self.courseid = courseid
                    self.load_tables()
                    msg = "You are in course:\n" + courseid
                    await bot.sendMessage(chat_id, msg)
                    (txt, menu_item) = self.check_student(self.student_id, self.chatid)
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                    return
                elif usertype in [11,21]:
                    self.is_admin = True
                    self.menu_id = 1
                    usertype = 11
                    bot_intance.adm_list.append(sid)
                    txt = banner_msg("Welcome " + self.username,"You are now connected to Mentor mode.")
                    self.menu_home = menu_items['mentor_menu']
                    (self.list_courseids, self.list_coursename,self.course_selection) = self.find_course(0)
                    menu_item = self.menu_home.copy()
                    await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                    return
                else:
                    txt = "Sorry your account is blocked, please contact the admin."
                    await self.sender.sendMessage(txt)
                    await self.logoff()
                    return
            else:
                self.list_courseids = []
                txt = 'My name is OmniMentor, I am your friendly learning facilitator at ' + self.client_name + '.\n'
                txt += "\nplease enter your student id or email address :"
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup([]))
                self.menu_id = keys_dict[menu_txt['option_learners']]

        elif self.menu_id == keys_dict[menu_txt['option_mainmenu']]:
            if resp == menu_txt['option_fct']:
                txt = 'You are in faculty admin mode.'
                menu_item = menu_items['faculty_menu']
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                menu_item = []
                self.menu_id = keys_dict[menu_txt['option_fct']]
            elif resp == menu_txt['option_pb']:
                txt = 'You are in playbooks maintainence mode.'
                menu_item = menu_items['playbook_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_pb']]
            elif resp == menu_txt['option_chat']:
                (status, info) = self.livechat()
                if status == 2:
                    txt = 'Chat with online learners 🗣'
                    await self.bot.sendMessage(self.chatid, txt, reply_markup = info )
                    self.menu_id = keys_dict[menu_txt['option_chatlist']]
                else:
                    txt = info
                    await self.sender.sendMessage(txt)
            elif resp == menu_txt['option_analysis']:
                txt = "Let's take a look on the following courses."
                courseid_menu = build_menu([x for x in self.list_courseids],1)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(courseid_menu))
                self.menu_id = keys_dict[menu_txt['opt_analysis']]
            elif resp == menu_txt['option_bindadm']:
                txt += "\nDo you want me to activate auto-login without entering admin id each time ?"
                opt_yes = "Yes, " + "enable auto-login"
                opt_no = "No, " + "I would like to login manually each time"
                yesno_menu = build_menu([opt_yes,opt_no],1,option_back)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(yesno_menu))
                txt = ""
                self.menu_id = keys_dict[menu_txt['option_bind']]
            elif resp == menu_txt['option_usermgmt']:
                txt = 'Please enter Student-ID or Email-Address or Username to search for details :'
                menu_item = menu_items['users_menu']
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                menu_item = []
                self.menu_id = keys_dict[menu_txt['option_usermgmt']]
            elif resp == menu_txt['option_admin']:
                if self.email.lower().endswith('sambaash.com'):
                    txt = 'You are in system admin mode'
                    menu_item = menu_items['admin_menu']
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                    menu_item = []
                    self.menu_id = keys_dict[menu_txt['option_admin']]
                else:
                    txt = 'This option is strickly for Sambaash admin.'
                    await self.sender.sendMessage(txt)
            elif resp == menu_txt['opt_client']:
                qry = "SELECT client_name, conn_str FROM ( "
                qry += "SELECT  client_name, `key` ,value AS conn_str from omnimentor.params union all "
                qry += "SELECT client_name, `key`,value AS conn_str from omnimentor_test.params union all "
                qry += "SELECT client_name, `key`,value AS conn_str from omnimentor_prod.params) zz WHERE `key` = 'omdb';"
                df = rds_df(qry)
                if df is None:
                    retmsg = "Unable to the client change at the moment."
                else:
                    df.columns = ['client_name', 'conn_str']
                    client_list = [x for x in df.client_name]
                    connection_list = [x for x in df.conn_str]
                    self.connection_dict = dict(zip(client_list,connection_list))
                    btn_client_list = build_menu(client_list, 1)
                    txt = f"Switch client_name from {self.client_name} to the below:"
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(btn_client_list))
                    self.menu_id = keys_dict[menu_txt['opt_setclient']]
            elif (resp == option_back) or (resp == "0"):
                tid = self.endchat()
                if tid > 0:
                    txt = "Live chat session disconnected. 👋"
                    await self.bot.sendMessage(tid, txt)
                    await self.bot.sendMessage(chat_id, txt)
                syslog("system : telegram user " + str(chat_id) + " offine.")
                await self.logoff()
                txt = "Session closed."
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup([[menu_txt['btn_hellobot']]]))

        elif self.menu_id == keys_dict[menu_txt['opt_setclient']] :
            if resp in list(self.connection_dict):
                conn_str = self.connection_dict[resp]
                bot_intance.schema = piece(piece(piece(conn_str,"?",0),'@',2),'/',1)
                vmsvclib.rdscon.close()
                vmsvclib.rds_connstr = conn_str
                vmsvclib.rdscon = None
                vmsvclib.rdsdb = None
                vmsvclib.rds_pool = 0
                vmsvclib.rds_schema = bot_intance.schema
                self.client_name = resp
                bot_intance.client_name = resp
                df = rds_df(f"select client_name,course_id from playbooks where client_name = '{resp}' limit 10;")
                txt = f"You are connected to client_name {resp}"
            else:
                txt = "You are back to the svcbot menu"
            menu_item = menu_items['svcbot_menu']
            await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
            self.menu_id = 1

        elif self.menu_id == keys_dict[menu_txt['option_learners']] :
            if (resp == option_back) or (resp == "0"):
                await self.logoff()
                txt= "End of session."
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup([[menu_txt['btn_hellobot']]]))
                return
            if '@' in resp :
                if self.edited == 0:
                    df = rds_df( "select distinct studentid, email from user_master order by email")
                    if df is not None:
                        df.columns = ['studentid','email']
                        result = email_lookup(df, resptxt)
                        if result != "":
                            resp = result
                            self.edited = int("0" + str(result))
                if self.edited > 0:
                    sid = self.edited
                    condqry = f" WHERE studentid={str(sid)} and lower(email)='{resptxt}' and client_name = '{self.client_name}';"
                    result = rds_param("SELECT email FROM user_master " + condqry)
                    if result == "":
                        txt = "Sorry your account is not found, please contact the admin."
                        await self.sender.sendMessage(txt)
                        await self.logoff()
                        return
                    uname = rds_param("SELECT username FROM user_master " + condqry)
                    self.username = uname
            if resp.isnumeric():
                sid = int(resp)
                query=f"SELECT COUNT(*) AS cnt FROM user_master WHERE studentid={resp} and client_name ='{self.client_name}';"
                result = rds_param(query)
                cnt = int("0" + str(result))
                if cnt == 0:
                    txt = "Sorry your account is not found, please contact the admin."
                    await self.sender.sendMessage(txt)
                    await self.logoff()
                    return
                query = "select * from user_master where studentid = " + resp + " and client_name = '" + self.client_name + "';"
                df = rds_df(query)
                if df is None:
                    txt = "Sorry your account is not found, please contact the admin."
                    await self.sender.sendMessage(txt)
                    await self.logoff()
                    return
                df.columns = bot_intance.usermaster_cols
                self.email = df.email.values[0]
                usertype = df.usertype.values[0]
                self.username = df.username.values[0]
                binded =  df.binded.values[0]
                if binded == 1:
                    txt = "Sorry your account is binded by someone else, please contact the admin."
                    await self.sender.sendMessage(txt)
                    await self.logoff()
                    return
                elif usertype not in [1, 11, 21]:
                    txt = "Sorry your account is blocked, please contact the admin."
                    await self.sender.sendMessage(txt)
                    await self.logoff()
                    return
                elif usertype in [11,21]:
                    usertype = 11
                    self.is_admin = True
                    self.menu_id = 1
                    self.student_id = sid
                    bot_intance.adm_list.append(sid)
                    self.menu_home = menu_items['mentor_menu']
                    txt = banner_msg("Welcome " + self.username,"You are now connected to Mentor mode.")
                    (self.list_courseids, self.list_coursename,self.course_selection) = self.find_course(0)
                    menu_item = self.menu_home.copy()
                    await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                    return
                if sid in bot_intance.user_list:
                    txt = "Sorry you can't logon using another telegram account\nPlease try again later."
                    await self.sender.sendMessage(txt)
                    await self.logoff()
                    return
                (self.list_courseids, self.list_coursename,self.course_selection) = self.find_course(sid)
                stud_courselist = self.list_courseids
                slen = len(self.course_selection)
                self.student_id = sid
                if (slen == 0) :
                    txt = "Sorry, we are not unable to find your record.\nPlease contact the course admin."
                    await self.sender.sendMessage(txt)
                    await self.logoff()
                    return
                if slen == 1:
                    self.chatid = chat_id
                    self.courseid = self.course_selection[0]
                    n = self.list_courseids.index(self.courseid)
                    self.coursename = self.list_coursename[n]
                    self.load_tables()
                    if self.userdata is None:
                        txt = f"we do not have any data for course id\n{self.courseid}"
                        await self.sender.sendMessage(txt)
                        return
                    else:
                        txt = "Please wait for a while."
                        await self.sender.sendMessage(txt)
                        (txt, menu_item) = self.check_student(self.student_id, chat_id)
                        await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                elif slen < 20:
                    btn_course_list = build_menu(stud_courselist, 1)
                    txt = "Please select the course id from below:"
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(btn_course_list))
                    self.menu_id = keys_dict[menu_txt['option_learners']]
                else:
                    date_today = datetime.datetime.now().date()
                    yr_str = str(date_today.strftime('%Y'))
                    yrnow = yr_str[-2:]
                    course_list = [x for x in stud_courselist if x[-2:]==yrnow]
                    btn_course_list = build_menu(course_list, 1)
                    txt = "Please select the course id from below:"
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(btn_course_list))
                    self.menu_id = keys_dict[menu_txt['option_learners']]
            else:
                if self.load_courseinfo(resp) == 0:
                    txt = 'Your selection is not available !\n'
                    txt += 'Please select the course from below list'
                    date_today = datetime.datetime.now().date()
                    course_list = [x for x in self.list_courseids if resptxt in x.lower()]
                    btn_course_list  = build_menu(course_list, 1)
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(btn_course_list))
                else:
                    self.chatid = chat_id
                    if self.student_id == 0 or self.chatid == 0 :
                        txt = 'My name is OmniMentor, I am your friendly learning facilitator at ' + self.client_name + '.\n'
                        txt += "\nplease enter your student id or email address :"
                        await self.bot.sendMessage(self.chatid, txt, [])
                        txt = ''
                        self.menu_id = keys_dict[menu_txt['lrn_start']]
                    else:
                        sid = self.student_id
                        ch_id = self.chatid
                        txt = "Please wait for a while."
                        await self.sender.sendMessage(txt)
                        (txt, menu_item) = self.check_student(sid, ch_id)
                        await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))

        elif self.menu_id == keys_dict[menu_txt['lrn_start']] :
            userdata = self.userdata
            if self.new_session and '@' in resp :
                resptxt = email_lookup(self.userdata, resptxt)
                resp = resp if resptxt=="" else resptxt
            if resp.isnumeric() and self.new_session :
                sid = int(resp)
                usertype = rds_param(f"SELECT usertype FROM user_master WHERE studentid={resp} and client_name ='{self.client_name}';")
                if usertype==3:
                    txt = "Sorry your account is blocked, please contact the admin."
                    await self.sender.sendMessage(txt)
                    await self.logoff()
                else:
                    txt = "Please wait for a while."
                    await self.sender.sendMessage(txt)
                    (txt, menu_item) = self.check_student(sid, chat_id)
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                    self.menu_id = keys_dict[menu_txt['lrn_student']]
            else:
                retmsg = "please enter your student id or email address :"

        elif self.menu_id == keys_dict[menu_txt['lrn_student']] and resp in [menu_txt[x] for x in "option_mycourse,option_schedule,option_faq,option_mychat,option_mychart,option_binduser,option_gethelp,option_info,option_back".split(",")]:
            if resp == option_back:
                await self.logoff()
            elif resp == menu_txt['option_mycourse']:
                date_today = datetime.datetime.now().date()
                course_list = [x for x in self.list_courseids ][:20]
                btn_course_list = build_menu(course_list, 1)
                txt = "Please select the course id from below:"
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(btn_course_list))
                self.menu_id = keys_dict[menu_txt['option_mycourse']]
            elif resp == menu_txt['option_faq']:
                txt = 'These are the FAQs :'
                faq_list = ft_model.faq_list
                faq_menu = build_menu(faq_list,1,option_back,[])
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(faq_menu))
                self.menu_id = keys_dict[menu_txt['option_faq']]
            elif resp == menu_txt['option_gethelp']:
                mentor_id = self.mentor_chatid()
                if mentor_id <= 0:
                    retmsg = 'Sorry the mentor information is not registered yet.' # or mentor not binded yet
                else:
                    result = '<b>Learner needs assistance 👋</b>'
                    result +='\n<pre>Course id    : ' +  self.courseid
                    result += '\nStudent ID   : ' + str(self.student_id)
                    result += '\nStudent Name : ' + self.username + '</pre>'
                    result += '\nContact : <a href=\"tg://user?id=' + str(self.chatid) + '">@' + self.chatname + '</a>'
                    bot_intance.user_list[ self.chatid ][4] = "👋"
                    try:
                        await bot.sendMessage(mentor_id,result,parse_mode='HTML')
                        retmsg = "Please wait, our faculty admin will connect with you on a live chat"
                    except:
                        retmsg = 'Sorry we are not able to reach the mentor at the moment.'
            elif resp == menu_txt['option_mychat']:
                (status, info) = self.livechat()
                if status == 2:
                    txt = 'Chat with online learners 🗣'
                    await self.bot.sendMessage(self.chatid, txt, reply_markup = info )
                    self.menu_id = keys_dict[menu_txt['option_chatlist']]
                else:
                    txt = info
                    await self.sender.sendMessage(txt)
            elif resp == menu_txt['option_mychart']:
                self.menu_id = keys_dict[menu_txt['lrn_student']]
                if self.student_id > 0:
                    (df,f) = self.mcqas_chart()
                    if f is None:
                        txt = "There is no data available for the chart."
                    else:
                        await self.bot.sendPhoto(chat_id, f)
                        txt = 'You are at the main menu.'
                    menu_item = self.menu_home
                    await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
            elif resp == menu_txt['option_binduser']:
                txt += "\nDo you want me to activate auto-login without entering student id each time ?"
                opt_yes = "Yes, " + "enable auto-login"
                opt_no = "No, " + "I would like to login manually each time"
                yesno_menu = build_menu([opt_yes,opt_no],1,option_back)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(yesno_menu))
                txt = ""
                self.menu_id = keys_dict[menu_txt['option_bind']]
            elif resp == menu_txt['option_schedule']:
                df = self.stagetable
                title = "Course Schedule for :\n" + self.courseid
                msg = stage_calendar(df)
                html_msg_dict[ title ] = [ msg ]
            elif resp == menu_txt['option_info']:
                retmsg = self.session_info()
                vars = dict()
                for v in list(self.records):
                    vars[v] = self.records[v]
                retmsg += "\n"
                mcqvars = vars['mcqvars']
                retmsg += learner_progress(vars, mcqvars, bot_intance.pass_rate,"mcq","MCQ")
                retmsg += "\n"
                asvars = vars['asvars']
                retmsg += learner_progress(vars, asvars, bot_intance.pass_rate,"as","Assignment")

        elif self.menu_id == keys_dict[menu_txt['option_mycourse']] :
            if (resp == option_back) or (resp == "0"):
                txt = "You are back to this cohort : " + self.courseid
                menu_item = self.menu_home.copy()
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['lrn_student']]
            elif resp in self.list_courseids:
                n = self.list_courseids.index( resp )
                self.courseid = resp
                self.coursename = self.list_coursename[n]
                self.load_tables()
                sid = self.student_id
                txt = "Please wait for a while."
                await self.sender.sendMessage(txt)
                (txt, menu_item) = self.check_student(self.student_id, chat_id)
                if len(txt)>0:
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                txt = "You are now with this cohort : " + self.courseid
                menu_item = self.menu_home.copy()
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['lrn_student']]
                self.student_id = sid
            else:
                btn_course_list = build_menu(self.list_courseids, 1)
                txt = "Please select the course id from below:"
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(btn_course_list))
                self.menu_id = keys_dict[menu_txt['option_mycourse']]
                self.new_session = True

        elif self.menu_id == keys_dict[menu_txt['option_bind']] :
            sid = str(self.student_id)
            cid = self.courseid
            if "yes," in resptxt:
                query = f"select count(*) as cnt from user_master where binded=1 and studentid={sid} and client_name = '{self.client_name}';"
                cnt = rds_param(query)
                if cnt==0:
                    updqry = f"update user_master set binded=1, chat_id={str(chat_id)}, courseid='{cid}' where client_name = '{self.client_name}' and studentid={sid};"
                    rds_update(updqry)
                    txt = "Auto-Login option enabled"
                    syslog(f"user {sid} binded to chat_id {chat_id} and course-id {cid}")
                else:
                    txt = "Sorry unable to bind due to more than one Student ID"
                    syslog(f"user {sid} chat_id {chat_id} binding rejected")
            elif "no," in resptxt:
                updqry = f"update user_master set binded=0 where client_name = '{self.client_name}' and studentid={sid};"
                rds_update(updqry)
                txt = "Auto-Login option disabled"
                syslog(f"user {sid} undo binding")
            elif (resp == option_back) or (resp == "0"):
                txt = "you are back to main menu"
            syslog(str(self.chatid) + " : " + txt)
            if self.is_admin :
                if self.super_admin:
                    menu_item = menu_items['svcbot_menu']
                else:
                    menu_item = menu_items['mentor_menu']
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = 1
            else:
                menu_item = self.menu_home.copy()
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['lrn_student']]

        elif self.menu_id == keys_dict[menu_txt['option_fct']]:
            if (resp == option_back) or (resp == "0"):
                txt = 'Please select the following mode:'
                menu_item = self.menu_home.copy()
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = 1
            elif resp == menu_txt['fc_edxupdate'] :
                txt = "LMS Import for which course ?"
                playbooklist_menu = [[x] for x in self.list_courseids]
                playbooklist_menu.append([option_back])
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(playbooklist_menu))
                self.menu_id = keys_dict[menu_txt['fc_edxupdate']]
            elif resp == menu_txt['fc_schedule']:
                txt = "Schedule update for which course ?"
                playbooklist_menu = [[x] for x in self.list_courseids]
                playbooklist_menu.append([option_back])
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(playbooklist_menu))
                self.menu_id = keys_dict[menu_txt['fc_schedule']]
            elif resp == menu_txt['fc_mentor']:
                txt = "This will assign mentors based on a list of course ids.\nSend me the list for course_id,mentor_email now:"
                await self.sender.sendMessage(txt)
                self.menu_id = keys_dict[menu_txt['fc_mentor']]

        elif self.menu_id == keys_dict[menu_txt['fc_schedule']]:
            if resp in self.list_courseids:
                txt = "Updating schedule now."
                await self.sender.sendMessage(txt)
                vmedxlib.update_schedule(resp, self.client_name)
                retmsg = f"Schedule for {resp} has been updated."
                syslog(retmsg)
            elif (resp == '*') and self.super_admin :
                job_request(self.chatid,self.client_name,"mass_update_schedule",None)
            txt = 'You are in faculty admin mode.'
            menu_item = menu_items['faculty_menu']
            await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
            self.menu_id = keys_dict[menu_txt['option_fct']]

        elif self.menu_id == keys_dict[menu_txt['fc_edxupdate']]:
            course_id = ""
            if (resp == option_back) or (resp == "0"):
                txt = 'You are in faculty admin mode.'
                menu_item = menu_items['faculty_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_fct']]
            elif (resp == '*') and self.super_admin :
                job_request(self.chatid,self.client_name,"edx_mass_import",None)
            elif resp in self.list_courseids:
                course_id = resp
            else:
                course_list = vmedxlib.search_course_list("%" + resp + "%" )
                if len(course_list)==1:
                    course_id = course_list[0]
                elif course_list == []:
                    txt = "There is no matching course_id found"
                    await self.sender.sendMessage(txt)
                else:
                    txt = "Which course_id should it be ?\n"
                    txt += '\n'.join(course_list)
                    await self.sender.sendMessage(txt)
            if course_id != "":
                txt = "Running LMS import now."
                await self.sender.sendMessage(txt)
                vmedxlib.edx_import(course_id, self.client_name, True)
                qry = f"update playbooks set eoc=0 where course_id = '{course_id}' and client_name = '{self.client_name}';"
                rds_update(qry)
                retmsg = f"LMS import for {course_id} has been completed."
                syslog(retmsg)
            txt = "you are back to the menu"
            menu_item = menu_items['faculty_menu']
            await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
            self.menu_id = keys_dict[menu_txt['option_fct']]

        elif self.menu_id == keys_dict[menu_txt['option_pb']]:
            if resp==menu_txt['pb_config']:
                qry = "SELECT distinct b.course_id FROM userdata a INNER JOIN playbooks b "
                qry += "ON a.client_name=b.client_name AND a.courseid=b.course_id INNER JOIN user_master c "
                qry += "ON a.client_name=c.client_name AND a.studentid=c.studentid "
                qry += f"WHERE b.eoc=0 AND a.client_name='{self.client_name}'"
                qry += "order by b.course_id;"
                df = rds_df(qry)
                if df is None:
                    n = 0
                else:
                    df.columns = ['course_id']
                    title = "List of active courses in the playbooks configurators."
                    html_msg_dict[title] = html_report(df, df.columns, [50], 25)
                    n = len(df)
                retmsg = f"Total number of active courses = {n}"
            elif resp == menu_txt['pb_userdata']:
                txt = "Please select from below list\nYou can type the course_id/cohort_id directly\nEnter 0 to return back"
                playbooklist_menu = [[x] for x in self.list_courseids]
                playbooklist_menu.append([option_back])
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(playbooklist_menu))
                self.menu_id = keys_dict[menu_txt['pb_userdata']]
            elif resp == menu_txt['pb_riskuser']:
                txt = "Generate report for which course ?"
                playbooklist_menu = [[x] for x in self.list_courseids]
                playbooklist_menu.append([option_back])
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(playbooklist_menu))
                self.menu_id = keys_dict[menu_txt['pb_riskuser']]
            elif (resp == option_back) or (resp == "0"):
                txt = 'Please select the following mode:'
                menu_item = self.menu_home.copy()
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = 1

        elif self.menu_id == keys_dict[menu_txt['pb_riskuser']]:
            self.menu_id = keys_dict[menu_txt['option_pb']]
            txt = 'You are in playbook maintainence mode.'
            if (resp in self.list_courseids) or ( resp == '*' ):
                dt_str = str(datetime.datetime.now().date().strftime('%Y-%m-%d'))
                fn = f"risk_report_{self.student_id}_{dt_str}.html"
                qry = f"SELECT courseid, studentid, username, (case risk_level when 1 then 'Yes' ELSE 'No' end) AS at_risk, "
                qry += f"mcq_zero AS mcq_pending, mcq_failed, "
                qry += f"as_zero AS assignment_pending, as_failed AS assignment_failed, f2f as absent "
                qry += f"FROM userdata WHERE client_name = '{self.client_name}' AND deferred = 0 "
                if resp in self.list_courseids:
                    qry += f" and courseid = '{resp}' "
                qry += f"ORDER BY courseid, studentid;"
                df = rds_df( qry)
                if df is None:
                    txt = "There is no information available at the moment"
                else:
                    df.columns = ['courseid','studentid','username','at_risk','mcq_pending','mcq_failed','assignment_pending','assignment_failed','absent']
                    title=f"Learners at risk - dated {dt_str}"
                    write2html(df, title=title, filename=fn)
                    await bot.sendDocument(chat_id, document=open(fn, 'rb'))
            menu_item = menu_items['playbook_menu']
            await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
            return

        elif self.menu_id == keys_dict[menu_txt['fc_mentor']]:
            self.menu_id = keys_dict[menu_txt['option_fct']]
            if ',' in resp:
                for mapitems in resp.split('\n'):
                    if ',' not in mapitems:
                        continue
                    maplist = mapitems.strip().split(',')
                    course_id = maplist[0]
                    email = maplist[1]
                    updqry = f"UPDATE playbooks SET mentor = '{email}' WHERE client_name = '{self.client_name}' AND course_id='{course_id}'"
                    rds_update(updqry)
                retmsg = "Course-to-mentor mapping imported succesfully"
            else:
                retmsg = "There is nothing to process for now"

        elif self.menu_id == keys_dict[menu_txt['pb_userdata']]:
            txt = "Please wait for a moment"
            await self.sender.sendMessage(txt)
            if self.load_courseinfo(resp) == 1:
                self.courseid = resp
                txt = course_status(self.client_name, resp)
                menu_item = menu_items['course_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['opt_pbusr']]
                if "not" in txt:
                    self.stagetable = None
            elif (resp == option_back) or (resp == "0"):
                txt = 'You are in playbook maintainence mode.'
                menu_item = menu_items['playbook_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_pb']]
            else:
                qry = f"select course_id from playbooks where client_name = '{self.client_name}' and course_id like '%{resp}%' limit 1;"
                txt = rds_param(qry)
                if txt == "":
                    txt = 'You are in playbook maintainence mode.'
                    menu_item = menu_items['playbook_menu']
                    await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                    self.menu_id = keys_dict[menu_txt['option_pb']]
                else:
                    self.courseid = txt
                    self.load_tables()
                    txt = course_status(self.client_name, self.courseid)
                    menu_item = menu_items['course_menu']
                    await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                    self.menu_id = keys_dict[menu_txt['opt_pbusr']]
                    if "not" in txt:
                        self.stagetable = None

        elif self.menu_id == keys_dict[menu_txt['opt_pbusr']]:
            if resp == menu_txt['ps_userdata']:
                query = "SELECT u.studentid, m.username, m.email, u.stage FROM userdata u INNER JOIN user_master m ON u.client_name=m.client_name "
                query += f" AND u.studentid=m.studentid WHERE u.client_name='{self.client_name}' AND u.courseid = '{self.courseid}' and u.deferred = 0 "
                query += ''.join([ " and lower(m.email) not like '%" + x + "'"  for x in bot_intance.efilter])
                df = rds_df(query)
                if df is None:
                    retmsg = "There is no information for this course at the moment"
                else:
                    df.columns = ['stud#','username','email','stage']
                    title = "List of learners from " + self.courseid
                    html_msg_dict[title] = html_report(df, df.columns, [5,20,25,9], 30)
                    n = len(df)
                    retmsg = f"Total number of active learners = {n}"
            elif resp == menu_txt['ps_schedule']:
                if (self.stagetable is None) or ( len(self.stagetable)==0):
                    retmsg = "The schedule information is not available"
                else:
                    df = self.stagetable
                    title = "Course Schedule for :\n" + self.courseid
                    msg = stage_calendar(df)
                    html_msg_dict[ title ] = [ msg ]
            elif resp == menu_txt['ps_stage']:
                if self.stagetable is None:
                    retmsg = "The unit guides information is not available"
                else:
                    title = "Unit guides for " + self.courseid
                    cols = ['stage', 'name', 'mcq', 'assignment']
                    df = self.stagetable[cols]
                    if len(df)==0:
                        retmsg = "The unit guides information is not available"
                    else:
                        html_msg_dict[title] = html_report(df, cols, [4,9,39,39], 8)
            elif resp == menu_txt['ps_mcqzero']:
                self.reload_userdata()
                if self.userdata is None:
                    retmsg = "Learners information is not available"
                else:
                    title = "Learners with MCQ test pending for " + self.courseid
                    cols = ['studentid', 'username', 'stage', 'mcq_zero']
                    df = self.userdata[cols]
                    df = df[df.mcq_zero!='[]']
                    n = len(df)
                    if n==0:
                        retmsg = "no one missed the mcq test so far"
                    else:
                        df['mcq_zero'] = df.apply(lambda x: str(x['mcq_zero']).replace(' ',''), axis=1)
                        html_msg_dict[title] = html_report(df, cols, [10, 15, 10, 40], 8)
                        retmsg = f"Total number of learners = {n}"
            elif resp == menu_txt['ps_mcqfailed']:
                self.reload_userdata()
                if self.userdata is None:
                    retmsg = "Learners information is not available"
                else:
                    title = "Learners with MCQ test failed for " + self.courseid
                    cols = ['studentid', 'username', 'stage', 'mcq_failed']
                    df = self.userdata[cols]
                    df = df[df.mcq_failed!='[]']
                    n = len(df)
                    if n==0:
                        retmsg = "no one failed the mcq test so far"
                    else:
                        df['mcq_failed'] = df.apply(lambda x: str(x['mcq_failed']).replace(' ',''), axis=1)
                        html_msg_dict[title] = html_report(df, cols,  [10, 15, 10, 40], 8)
                        retmsg = f"Total number of learners = {n}"
            elif resp == menu_txt['ps_aszero']:
                self.reload_userdata()
                if self.userdata is None:
                    retmsg = "Learners information is not available"
                else:
                    title = "Learners with Assignment test pending for " + self.courseid
                    cols = ['studentid', 'username', 'stage', 'as_zero']
                    df = self.userdata[cols]
                    df = df[df.as_zero!='[]']
                    n = len(df)
                    if n==0:
                        retmsg = "no one missed the assignment test so far"
                    else:
                        df['as_zero'] = df.apply(lambda x: str(x['as_zero']).replace(' ',''), axis=1)
                        html_msg_dict[title] = html_report(df, cols,  [10, 15, 10, 40], 8)
                        retmsg = f"Total number of learners = {n}"
            elif resp == menu_txt['ps_asfailed']:
                self.reload_userdata()
                if self.userdata is None:
                    retmsg = "Learners information is not available"
                else:
                    title = "Learners with Assignment test failed for " + self.courseid
                    cols = ['studentid', 'username', 'stage', 'as_failed']
                    df = self.userdata[cols]
                    df = df[df.as_failed!='[]']
                    n = len(df)
                    if n==0:
                        retmsg = "no one failed the assignment test so far"
                    else:
                        df['as_failed'] = df.apply(lambda x: str(x['as_failed']).replace(' ',''), axis=1)
                        html_msg_dict[title] = html_report(df, cols, [10, 15, 10, 40], 8)
                        retmsg = f"Total number of learners = {n}"
            elif resp == menu_txt['ps_progress']:
                if self.stagetable is None:
                    retmsg = "Learners progress information is not available"
                else:
                    query = "SELECT u.studentid FROM userdata u INNER JOIN user_master m ON u.client_name=m.client_name AND u.studentid=m.studentid "
                    query += f" WHERE u.deferred = 0 and u.client_name='{self.client_name}' AND u.courseid = '{self.courseid}' "
                    query += ''.join([ " and lower(m.email) not like '%" + x + "'"  for x in bot_intance.efilter])
                    df = rds_df(query)
                    if (df is None) or (len(df)==0):
                        retmsg = "Learners progress information is not available"
                    else:
                        df.columns = ['studentid']
                        sid_list = sorted([str(x) for x in df.studentid])
                        btn_list = build_menu( sid_list, 6, option_back, [])
                        txt = "Select a valid student id to see the progress :"
                        await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(btn_list))
                        self.menu_id = keys_dict[menu_txt['ps_progress']]
            elif (resp == option_back) or (resp == "0"):
                txt = 'You are in playbooks maintainence mode.'
                menu_item = menu_items['playbook_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_pb']]

        elif self.menu_id == keys_dict[menu_txt['ps_progress']] :
            if (resp == option_back) or (resp == "0"):
                txt = 'Please select the following :'
                menu_item = menu_items['course_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['opt_pbusr']]
            elif resp.isnumeric():
                self.student_id = int(resp)
                self.records = load_vars(self.userdata, self.student_id)
                if len(self.records)==0:
                    retmsg = "Please select valid student id."
                elif self.records['deferred']==1:
                    retmsg = "This learner already deferred from the course."
                else:
                    vars = display_progress(self.userdata, self.stagetable, self.student_id, self.records, self.client_name, bot_intance.resp_dict, bot_intance.pass_rate, bot_intance.att_rate)
                    txt  = vars['notification']
                    if vars['course_alive'] == 0:
                        await self.sender.sendMessage(txt)
                        return
                    tlist = txt.split('\n')
                    uname = self.records['username']
                    txt = '\n'.join(tlist[1:])
                    txt = "Hi" + ", " + uname + " !\n" + txt
                    await self.sender.sendMessage(txt)
                    title = "Reminder (T-1) and intervention messages"
                    txt = vars['reminder']
                    tlist = txt.split('\n')
                    txt1 = '\n'.join(tlist[1:])
                    txt = vars['intervention']
                    tlist = txt.split('\n')
                    txt2 = '\n'.join(tlist[1:])
                    mcqvars = vars['mcqvars']
                    txt = learner_progress(vars, mcqvars, bot_intance.pass_rate,"mcq","MCQ")
                    asvars = vars['asvars']
                    txt += learner_progress(vars, asvars, bot_intance.pass_rate,"as","Assignment")
                    html_msg_dict[title] = [ txt1, txt2 ]
                    retmsg = txt
            else:
                retmsg = "Please select valid student id."

        elif self.menu_id == keys_dict[menu_txt['opt_analysis']]:
            if (resp == option_back) or (resp == "0"):
                txt = 'Please select the following mode:'
                menu_item = self.menu_home.copy()
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = 1
            elif self.load_courseinfo(resp) == 1:
                self.courseid = resp
                if self.userdata is None:
                    txt = "Sorry, there is not enough data for analytics for this course"
                    menu_item = self.menu_home.copy()
                    await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                    self.menu_id = 1
                else:
                    txt = 'Please select the following mode:'
                    menu_item = menu_items['analysis_menu']
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                    self.menu_id = keys_dict[menu_txt['option_analysis']]
            else:
                qry = f"select course_id from playbooks where client_name = '{self.client_name}' and course_id like '%{resp}%' limit 1;"
                txt = rds_param(qry)
                if txt == "":
                    txt = 'Please select the following mode:'
                    menu_item = self.menu_home.copy()
                    await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                    self.menu_id = 1
                else:
                    self.courseid = txt
                    self.load_tables()
                    txt = 'Please select the following mode:'
                    menu_item = menu_items['analysis_menu']
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                    self.menu_id = keys_dict[menu_txt['option_analysis']]

        elif self.menu_id == keys_dict[menu_txt['option_analysis']]:
            if (resp == option_back) or (resp == "0"):
                txt = 'Please select the following mode:'
                menu_item = self.menu_home.copy()
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = 1
            elif resp == menu_txt['ml_grading']:
                (sid_list , df)= self.grad_prediction()
                df.columns = ['Student ID#','Name', 'Prediction']
                n = len(sid_list)
                title = "AI Grading for " + self.courseid
                html_msg_dict[title] = html_report(df, df.columns, [10, 15, 10], 20)
                btn_list = build_menu( sid_list, 6, option_back, [])
                self.records['progress_sid'] = sid_list
                n = len(sid_list) - 1
                txt = f"total {n} learners in the list.\nSelect the student id to see the progress :"
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(btn_list))
                self.menu_id = keys_dict[menu_txt['opt_aig']]
                retmsg = f"Total number of learners = {n}"
            elif resp == menu_txt['an_mcq']:
                (tbl1,tbl2,tbl3,n) = analyze_cohort(self.courseid, self.userdata, self.bot, self.chatid)
                if (tbl1 == []) or (tbl2 + tbl3 == []):
                    txt = "There is no information available at the moment"
                    await self.sender.sendMessage(txt)
                    return
                title  = "Assignment & MCQ Score Summary for\n" + self.courseid
                df =  pd.DataFrame( tbl1 )
                df.columns = ['MCQ Test #', 'Average Score' , 'Assignment Test #', 'Average Score']
                html_msg_dict[title] = html_report(df, df.columns, [15,15,15,15], 20)

                if tbl2 != []:
                    title  = "MCQ Grouping for " + self.courseid
                    df =  pd.DataFrame( tbl2 )
                    df.columns = ['Grouping']
                    html_msg_dict[title] = html_report(df, df.columns, [120], 28)

                if tbl3 != []:
                    title  = "Assignment Grouping for " + self.courseid
                    df =  pd.DataFrame( tbl3 )
                    df.columns = ['Grouping']
                    html_msg_dict[title] = html_report(df, df.columns, [120], 28)

                retmsg = f"Total number of learners = {n}"
            elif resp == menu_txt['an_mcqd']:
                txt = "MCQ Difficulty Analysis by:"
                menu_item = menu_items['mcqdiff_menu']
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['opt_mcqd']]
            elif resp == menu_txt['an_chart']:
                (df,f) = self.mcqas_chart(True)
                if f is None:
                    retmsg = "There is no data available for the chart."
                else:
                    await self.bot.sendPhoto(chat_id, f)
                    if df is not None:
                        df.columns = ['Test/IU','mcq test','assignment test']
                        cohort_id = getcohort(self.courseid)
                        title = f"MCQ and Assignment scores for cohort {cohort_id}"
                        html_msg_dict[title] = html_report(df, df.columns, [10, 10, 10], 15)
                        
        elif self.menu_id == keys_dict[menu_txt['opt_mcqd']]:
            if (resp == option_back) or (resp == "0"):
                txt = 'Please select the following mode:'
                menu_item = menu_items['analysis_menu']
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_analysis']]
            elif resp == menu_txt['an_avgatt']:
                mcq_analysis.load_mcqdata(self.client_name, self.courseid)
                df = mcq_analysis.top10attempts()
                if df is None:
                    retmsg = 'There is no data for this course.'
                else:
                    title = "MCQ Analysis Difficulty By MCQ Attempts"
                    cols = ['MCQ No. Question No.', 'Average Attempts']
                    df =  df[cols]
                    html_msg_dict[title] = html_report(df, cols, [25,18], 10)
            elif resp == menu_txt['an_avgscore']:
                mcq_analysis.load_mcqdata(self.client_name, self.courseid)
                df = mcq_analysis.top10score()
                if df is None:
                    retmsg = 'There is no data for this course.'
                else:
                    title = "MCQ Analysis Difficulty By MCQ Scores"
                    cols = ['MCQ No. Question No.', 'Average Score %', 'Average Attempts']
                    df = df[cols]
                    html_msg_dict[title] = html_report(df, df.columns, [25,18,18], 10)
            elif resp == menu_txt['an_mcqavg']:
                df = self.userdata
                client_name = list(df['client_name'])[0]
                course_id = list(df['courseid'])[0]
                self.client_name = client_name
                self.courseid = course_id
                qry = f"select * from mcq_data where client_name = '{client_name}' and course_id = '{course_id}';"
                df = rds_df(qry)
                if df is None:
                    retmsg = 'There is no data for this course.'
                else:
                    df.columns = get_columns("mcq_data")
                    mcq_list = [x for x in df.mcq]
                    sorted_list = sorted(list(set([int(x) for x in mcq_list])))
                    mcq_list = [str(x) for x in sorted_list ] + [option_back]
                    m = int((len(mcq_list)+4)/5)
                    mcq_menu = [  mcq_list[n*5:][:5] for n in range(m) ]
                    txt = "Please select from the list of MCQs below:"
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(mcq_menu))
                    self.menu_id = keys_dict[menu_txt['opt_mcqavg']]

        elif self.menu_id == keys_dict[menu_txt['opt_mcqavg']]:
            if resp.isnumeric() :
                mcq_analysis.load_mcqdata(self.client_name, self.courseid)
                df = mcq_analysis.mcq_summary(int(resp))
                if df is None:
                    retmsg = 'There is no data for this course.'
                else:
                    title = f"MCQ Analysis Difficulty By MCQ Average for MCQ Test {resp}"
                    html_msg_dict[title] = html_report(df, df.columns, [10, 18, 18], 20)
            elif (resp == option_back) or (resp == "0"):
                self.menu_id = keys_dict[menu_txt['opt_mcqd']]
                txt = "MCQ Difficulty Analysis by:"
                menu_item = menu_items['mcqdiff_menu']
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))

        elif self.menu_id == keys_dict[menu_txt['opt_aig']]:
            if resp.isnumeric() :
                sid_list = self.records['progress_sid']
                sid = int(resp)
                if resp in sid_list:
                    df = self.records['progress_df'][sid]
                    title = self.records['progress_tt'][sid]
                    html_msg_dict[title] = html_report(df, df.columns, [10,10,10,10,10], 10)
                else:
                    retmsg = "Please select valid learner student_id"
            elif (resp == option_back) or (resp == "0"):
                txt = 'Select your option:'
                menu_item = menu_items['analysis_menu']
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_analysis']]
                return

        elif self.menu_id ==  keys_dict[menu_txt['option_usermgmt']]:
            if (resp == option_back) or (resp == "0"):
                txt = "You are back to the main menu."
                menu_item = self.menu_home.copy()
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_mainmenu']]
                return
            if resp.isnumeric():
                sid = int(resp)
                self.student_id = sid
                txt = self.userinfo(sid)
                if self.student_id > 0:
                    txt += "\nHit " + option_back + " or enter 0 to go back."
                    menu_item = menu_items['useraction_menu']
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                    self.menu_id = keys_dict[menu_txt['option_resetuser']]
                else:
                    retmsg = txt
            elif resp == menu_txt['option_searchbyname']:
                txt = "Search Student-ID by name"
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup([[option_back]]))
                self.menu_id = keys_dict[menu_txt['option_searchbyname']]
            elif resp == menu_txt['option_searchbyemail']:
                txt = "Search Student-ID by email"
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup([[option_back]]))
                self.menu_id = keys_dict[menu_txt['option_searchbyemail']]
            elif resp == menu_txt['option_resetuser']:
                txt = "Please enter valid Student-ID :"
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup([[option_back]]))
                self.menu_id = keys_dict[menu_txt['option_resetuser']]
            elif resp == menu_txt['option_admin_users']:
                query = f"select studentid,username,email from user_master where client_name = '{self.client_name}' and usertype=11 limit 50;"
                result = "List of admin users (top 50)\n"
                df = rds_df(query)
                if df is None:
                    txt = "Sorry, no results found."
                    await self.sender.sendMessage(txt)
                    return
                df.columns = ['studentid','username','email']
                title = result
                html_msg_dict[title] = html_report(df, df.columns, [10,30,40], 25)
            elif resp == menu_txt['option_blocked_users']:
                query = f"select studentid,username,email from user_master where client_name = '{self.client_name}' and usertype=0 limit 50;"
                result = "List of blocked users (top 50)\n"
                df = rds_df(query)
                if df is None:
                    txt = "Sorry, no results found."
                    await self.sender.sendMessage(txt)
                    return
                df.columns = ['studentid','username','email']
                title = result
                html_msg_dict[title] = html_report(df, df.columns, [10,30,40], 25)
            elif resp == menu_txt['option_binded_users']:
                query = f"UPDATE user_master SET binded=0 WHERE chat_id=0 and client_name = '{self.client_name}';"
                rds_update(query)
                query = f"select studentid,username,email,chat_id from user_master where client_name = '{self.client_name}' and binded=1 limit 50;"
                result = "List of binded users (top 50)\n"
                df = rds_df(query)
                if df is None:
                    txt = "Sorry, no results found."
                    await self.sender.sendMessage(txt)
                    return
                df.columns = ['studentid','username','email','chat_id']
                title = result
                html_msg_dict[title] = html_report(df, df.columns, [10,30,40,20], 25)
            elif resp == menu_txt['option_active_users']:
                query = f"select studentid,username,email,chat_id from user_master where client_name = '{self.client_name}' and usertype = 1 limit 50;"
                result = "List of activated learners (top 50)\n"
                df = rds_df(query)
                if df is None:
                    txt = "Sorry, no results found."
                    await self.sender.sendMessage(txt)
                    return
                df.columns = ['studentid','username','email','chat_id']
                title = result
                html_msg_dict[title] = html_report(df, df.columns, [10,30,40,20], 25)
            elif resp == menu_txt['option_deferred_users']:
                query = "SELECT u.studentid,u.username,u.courseid FROM userdata u INNER JOIN playbooks p "
                query += "ON u.client_name=p.client_name and u.courseid=p.course_id "
                query += f"WHERE u.client_name = '{self.client_name}' AND p.eoc=0 AND u.deferred=1 limit 50;"
                result = "List of deferred learners & courses (top 50)\n"
                df = rds_df(query)
                if df is None:
                    txt = "Sorry, no results found."
                    await self.sender.sendMessage(txt)
                    return
                df.columns = ['studentid','username','courseid']
                title = result
                html_msg_dict[title] = html_report(df, df.columns, [10,30,50], 25)
            elif resp == menu_txt['option_whitelist']:
                txt = "Let's take a look on the following courses."
                courseid_menu = build_menu([x for x in self.list_courseids],1)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(courseid_menu))
                self.menu_id = keys_dict[menu_txt['option_whitelist']]

            elif "@" in resp:
                query = f"select studentid,username,email from user_master where client_name = '{self.client_name}' and lower(email) like '%{resptxt}%' ;"
                df = rds_df(query)
                if df is None:
                    txt = "Sorry not matching results found"
                    await self.sender.sendMessage(txt)
                    return
                df.columns = ['studentid','username','email']
                title = "Email search matching " + resp + "\n"
                html_msg_dict[title] = html_report(df, df.columns, [10,30,40], 20)
            else:
                query = f"select studentid,username,email from user_master where client_name = '{self.client_name}' and lower(username) like '%{resptxt}%' ;"
                df = rds_df(query)
                if df is None:
                    txt = "Sorry not matching results found"
                    await self.sender.sendMessage(txt)
                    return
                df.columns = ['studentid','username','email']
                title = "Name title matching " + resp + "\n"
                html_msg_dict[title] = html_report(df, df.columns, [10,30,40], 20)

        elif self.menu_id in [ keys_dict[menu_txt[x]] for x in "option_searchbyname,option_searchbyemail".split(",") ] :
            if (resp == option_back) or (resp == "0"):
                txt = "You are back to the user management menu."
                menu_item = menu_items['users_menu']
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_usermgmt']]
                return
            idx = [ keys_dict[menu_txt[x]] for x in "option_searchbyname,option_searchbyemail".split(",") ].index(self.menu_id)
            if idx==0:
                query = f"select studentid,username,email from user_master where client_name = '{self.client_name}' and lower(username) like '%{resptxt}%' ;"
                result = "Name search matching " + resp + "\n"
            if idx==1:
                query = f"select studentid,username,email from user_master where client_name = '{self.client_name}' and lower(email) like '%{resptxt}%' ;"
                result = "Email search matching " + resp + "\n"
            df = rds_df(query)
            if df is None:
                txt = "Search Student-ID by name"
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup([[option_back]]))
                self.menu_id = keys_dict[menu_txt['option_searchbyname']]
                return
            df.columns = ['studentid','username','email']
            title = result
            html_msg_dict[title] = html_report(df, df.columns, [10,30,40], 20)

        elif self.menu_id == keys_dict[menu_txt['option_resetuser']]:
            if (resp == option_back) or (resp == "0"):
                txt = "You are back to the user management menu."
                menu_item = menu_items['users_menu']
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_usermgmt']]
                return
            sid = 0
            if resp.isnumeric():
                sid = int(resp)
                txt = self.userinfo(sid)
                txt += "\nHit " + option_back + " or enter 0 to go back."
                menu_item = menu_items['useraction_menu']
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
            elif resp == menu_txt['opt_blockuser']:
                query = f"update user_master set usertype = 0 where client_name = '{self.client_name}' and studentid = {self.student_id} ;"
                rds_update(query)
                retmsg = f"User with Student-ID {self.student_id} has been blocked."
            elif resp == menu_txt['opt_setadmin']:
                query = f"update user_master set usertype = 11 where client_name = '{self.client_name}' and studentid = {self.student_id} ;"
                rds_update(query)
                retmsg = f"User with Student-ID {self.student_id} has been set as admin."
            elif resp == menu_txt['opt_setlearner']:
                query = f"update user_master set usertype = 1 where client_name = '{self.client_name}' and studentid = {self.student_id} ;"
                rds_update(query)
                retmsg = f"User with Student-ID {self.student_id} has been set as learner."
            elif resp == menu_txt['opt_resetemail']:
                retmsg = "Please enter the new email address:"
                self.menu_id = keys_dict[menu_txt['opt_resetemail']]
            elif resp == menu_txt['opt_defer_course']:
                qry = "SELECT u.courseid FROM userdata u INNER JOIN playbooks p ON u.client_name=p.client_name "
                qry += f"and u.courseid=p.course_id WHERE u.client_name = '{self.client_name}' AND p.eoc=0 "
                qry += f"AND u.deferred=0 AND studentid={self.student_id};"
                df = rds_df(qry)
                if df is None:
                    retmsg = "There is nothing to defer."
                else:
                    df.columns = ['courseid']
                    btn_course_list = [x for x in df.courseid]
                    btn_course_list = build_menu(btn_course_list, 1, option_back)
                    txt = "Please select the course id from below:"
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(btn_course_list))
                    self.menu_id = keys_dict[menu_txt['opt_defer_course']]
            elif resp == menu_txt['opt_undefer_course']:
                qry = "SELECT u.courseid FROM userdata u INNER JOIN playbooks p ON u.client_name=p.client_name "
                qry += f"and u.courseid=p.course_id WHERE u.client_name = '{self.client_name}' AND p.eoc=0 "
                qry += f"AND u.deferred=1 AND studentid={self.student_id};"
                df = rds_df(qry)
                if df is None:
                    retmsg = "There is nothing to undefer."
                else:
                    df.columns = ['courseid']
                    btn_course_list = [x for x in df.courseid]
                    btn_course_list = build_menu(btn_course_list, 1, option_back)
                    txt = "Please select the course id from below:"
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(btn_course_list))
                    self.menu_id = keys_dict[menu_txt['opt_undefer_course']]
            elif resp == menu_txt['opt_unbind']:
                query = f"update user_master set binded = 0, chat_id = 0 where client_name = '{self.client_name}' and studentid = {self.student_id} ;"
                rds_update(query)
                retmsg = f"User telegram account has been unbinded from Student-ID {self.student_id}."

        elif self.menu_id == keys_dict[menu_txt['opt_resetemail']]:
                if '@' in resp:
                    query = f"update user_master set email = '{resp}' where client_name = '{self.client_name}' and studentid = {self.student_id};"
                    rds_update(query)
                    txt = f"Email address for Student-ID {self.student_id} has been set to "
                    retmsg = txt + resp
                self.menu_id = keys_dict[menu_txt['option_resetuser']]

        elif self.menu_id == keys_dict[menu_txt['opt_defer_course']]:
            if (resp == option_back) or (resp == "0"):
                txt = "You are back to the reset user menu."
                menu_item = menu_items['useraction_menu']
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_resetuser']]
                return
            query=f"update userdata set deferred=1 where client_name = '{self.client_name}' and studentid={self.student_id} and courseid='{resp}';"
            rds_update(query)
            txt = f"course_id {resp} has been deferred for student_id = {self.student_id}"
            menu_item = menu_items['useraction_menu']
            await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
            self.menu_id = keys_dict[menu_txt['option_resetuser']]

        elif self.menu_id == keys_dict[menu_txt['opt_undefer_course']]:
            if (resp == option_back) or (resp == "0"):
                txt = "You are back to the reset user menu."
                menu_item = menu_items['useraction_menu']
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_resetuser']]
                return
            query=f"update userdata set deferred=0 where client_name = '{self.client_name}' and studentid={self.student_id} and courseid='{resp}';"
            rds_update(query)
            txt = f"course_id {resp} has been undeferred for student_id = {self.student_id}"
            menu_item = menu_items['useraction_menu']
            await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
            self.menu_id = keys_dict[menu_txt['option_resetuser']]

        elif self.menu_id == keys_dict[menu_txt['option_whitelist']]:
            if resp in self.list_courseids:
                txt = "Please wait for a moment."
                await self.sender.sendMessage(txt)
                query = f"select studentid FROM user_master where client_name = '{self.client_name}' "
                query += ''.join([ " and lower(email) not like '%" + x + "'"  for x in bot_intance.efilter])
                query += f" AND studentid IN (select studentid FROM userdata WHERE client_name = '{self.client_name}' "
                query += f" AND courseid = '{resp}');"
                df = rds_df(query)
                if df is not None:
                    if len(df)>0:
                        df.columns = ['studentid']
                        slist = ','.join([ str(x) for x in df.studentid ])
                        query = f"update user_master set usertype = 1 where client_name = '{self.client_name}' and studentid IN ({slist});"
                        try:
                            rds_update(query)
                            txt = "Learners has been whitelisted for course_id : " + resp
                        except:
                            syslog("unable to whitelist users")
                            txt = "Unable to whitelist for course_id : " + resp
            else:
                txt = "You are back to the user management menu."
            menu_item = menu_items['users_menu']
            await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
            self.menu_id = keys_dict[menu_txt['option_usermgmt']]

        elif self.menu_id == keys_dict[menu_txt['option_admin']]:
            if resp == menu_txt['option_nlp']:
                txt = "This section maintain NLP corpus and trains model.\n"
                txt += "You can test your NLP dialog from here."
                txt = banner_msg("NLP", txt)
                menu_item = menu_items['nlp_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_nlp']]
            elif resp == menu_txt['option_ml']:
                txt = 'You are in ML options.'
                menu_item = menu_items['ml_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_ml']]
            elif resp == menu_txt['option_restful']:
                self.records['cid'] = ""
                self.records['sid'] = 0
                txt = 'You are in restful api menu.'
                menu_item = menu_items['rest_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_restful']]
            elif resp == menu_txt['option_alerts']:
                txt = "You are in the system alerts menu"
                menu_item = menu_items['alerts_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_alerts']]
            elif resp == menu_txt['option_syscfg']:
                txt = "You are in the system configuration"
                menu_item = menu_items['syscfg_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_syscfg']]
            elif resp == menu_txt['option_datacheck']:
                txt = "You are in the data integrity check menu"
                menu_item = menu_items['datacheck_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_datacheck']]
            elif resp == menu_txt['option_client']:
                txt = "You are in the client management menu"
                menu_item = menu_items['client_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_client']]
            elif resp == menu_txt['option_export']:
                fn = "omdata.xlsx"
                try:
                    writer = pd.ExcelWriter(fn)
                    for tbl in ['course_module','module_iu','playbooks', 'stages_master', "stages"]:
                        qry = f"select * from {tbl} where client_name = '{self.client_name}';"
                        df = rds_df(qry)
                        if df is not None:
                            cols = get_columns(tbl)
                            df.columns = cols
                            new_cols = [c for c in cols if c != 'client_name']
                            df = df[new_cols]
                            df.to_excel(writer, sheet_name=tbl)
                    writer.save()
                    writer.close()
                    await self.bot.sendDocument(chat_id, document=open(fn, 'rb'))
                    retmsg = "Tables exported as "+ fn
                except:
                    retmsg = "Unable to export to excel file for some reason"
            elif resp == option_back:
                txt = 'Please select the following mode:'
                menu_item = self.menu_home.copy()
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = 1

        elif self.menu_id == keys_dict[menu_txt['option_client']]:
            if resp == option_back:
                txt = "You are back in the main menu"
                menu_item = menu_items['admin_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_admin']]
            elif resp in [ menu_txt[x] for x in "sys_clientcopy,sys_clientdelete".split(",") ]:
                df = rds_df("select distinct client_name from user_master order by client_name;")
                if df is None:
                    retmsg = "Information not available"
                else:
                    df.columns = ['client_name']
                    client_list = [x for x in df.client_name] + [option_back]
                    if resp == menu_txt['sys_clientcopy']:
                        txt = "Client copy from client :"
                    else:
                        txt = "Select the client for deletion :"
                    await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup([client_list]))
                    self.menu_id = keys_dict[menu_txt['opt_tgtclt']]
                    self.records['clt_opt'] = keys_dict[resp]
            elif resp == menu_txt['sys_clientconfig']:
                df = rds_df(f"select `key`,`value` from params where client_name = '{self.client_name}';")
                if df is None:
                    retmsg = "Unable to access client configuration"
                else:
                    df.columns = ['key', 'value']
                    title = "client configuration"
                    html_msg_dict[title] = html_report(df, df.columns, [15, 70], 25)
                bot_intance.get_client_config()
                retmsg = "Client configuration has been reloaded."

        elif self.menu_id == keys_dict[menu_txt['opt_tgtclt']]:
            if resp == option_back:
                txt = "You are back in the client menu"
                menu_item = menu_items['client_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_client']]
                return
            if self.records['clt_opt'] == keys_dict[menu_txt['sys_clientcopy']]:
                self.records['src_clt'] = resp
                retmsg = "Enter the target client name (case sensitive (0 to exit):"
                self.menu_id = keys_dict[menu_txt['sys_clientcopy']]
            if self.records['clt_opt'] == keys_dict[menu_txt['sys_clientdelete']]:
                await self.sender.sendMessage(f"deleting data for client_name = {resp}....")
                table_list = ['course_module' , 'module_iu', 'mcqas_info', 'mcq_data', 'mcq_score']
                table_list += ['playbooks', 'stages', 'stages_master', 'userdata' , 'user_master']
                for tbl in table_list:
                    qry = f"DELETE FROM {tbl} WHERE client_name = '{resp}';"
                    rds_update(qry)
                await self.sender.sendMessage(f"Client deletion has been completed. Pls proceed to clear parameters.")
                txt = "You are back in the client menu"
                menu_item = menu_items['client_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_client']]

        elif self.menu_id == keys_dict[menu_txt['sys_clientcopy']]:
            if resp != '0':
                src_clt = self.records['src_clt']
                tgt_clt = resp
                await self.sender.sendMessage(f"Copying from {src_clt} to {tgt_clt}....")
                table_list = ['course_module' , 'module_iu', 'mcqas_info', 'mcq_data', 'mcq_score']
                table_list += ['playbooks', 'stages', 'stages_master', 'userdata' , 'user_master']
                for tbl in table_list:
                    qry =  f"select * from {tbl} WHERE client_name = '{src_clt}';"
                    df = rds_df(qry)
                    if df is not None:
                        df.columns = get_columns(tbl)
                        df['client_name'] = tgt_clt
                        qry = f"DELETE FROM {tbl} WHERE client_name = '{tgt_clt}';"
                        rds_update(qry)
                        copydbtbl(df, tbl)
                await self.sender.sendMessage("Client copy  has been completed. Pls proceed to define parameters.")
            txt = "You are back in the client menu"
            menu_item = menu_items['client_menu']
            await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
            self.menu_id = keys_dict[menu_txt['option_client']]

        elif self.menu_id == keys_dict[menu_txt['sys_clientdelete']]:
            bot_intance.client_name = self.client_name = resp
            txt = "Default client_name set to {resp}"
            menu_item = menu_items['client_menu']
            await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
            self.menu_id = keys_dict[menu_txt['option_client']]

        elif self.menu_id == keys_dict[menu_txt['option_syscfg']]:
            if (resp == option_back) or (resp == "0"):
                txt = "You are back in the system admin menu"
                menu_item = menu_items['admin_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_admin']]
            elif resp == menu_txt['option_cfg_playbooks']:
                txt = "View playbook for which course ?"
                playbooklist_menu = [[x] for x in self.list_courseids]
                playbooklist_menu.append([option_back])
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(playbooklist_menu))
                self.menu_id = keys_dict[menu_txt['option_cfg_playbooks']]
            elif resp == menu_txt['option_cfg_stagesmaster']:
                txt = "View stages_master for which course ?"
                playbooklist_menu = [[x] for x in self.list_courseids]
                playbooklist_menu.append([option_back])
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(playbooklist_menu))
                self.menu_id = keys_dict[menu_txt['option_cfg_stagesmaster']]
            elif resp == menu_txt['option_cfg_module_iu']:
                txt = "View module_iu for which course ?"
                playbooklist_menu = [[x] for x in self.list_courseids]
                playbooklist_menu.append([option_back])
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(playbooklist_menu))
                self.menu_id = keys_dict[menu_txt['option_cfg_module_iu']]

        elif self.menu_id == keys_dict[menu_txt['option_cfg_playbooks']]:
            qry = f"select * from playbooks where course_id='{resp}' and client_name = '{self.client_name}';"
            title = "playbooks record for " + resp
            cols = get_columns("playbooks")
            df = rds_df(qry)
            if df is not None:
                df.columns = cols
                txt=""
                for c in cols:
                    x = list(df[c])[0]
                    txt += (c + ' '*15)[:16] + str(x) + '\n'
                html_msg_dict[title] = [txt]
            txt = "You are back in the system config menu"
            menu_item = menu_items['syscfg_menu']
            await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
            self.menu_id = keys_dict[menu_txt['option_syscfg']]

        elif self.menu_id == keys_dict[menu_txt['option_cfg_stagesmaster']]:
            qry =  "SELECT a.* FROM stages_master a INNER JOIN playbooks b ON a.client_name=b.client_name"
            qry += " AND a.module_code=b.module_code AND a.course_code=b.course_code AND a.pillar=b.pillar"
            qry += f" WHERE b.client_name = '{self.client_name}' and b.course_id = '{resp}';"
            title = "stages_master record for " + resp
            cols = ['pillar','course_code','module_code']
            df = rds_df(qry)
            if df is not None:
                df.columns = get_columns('stages_master')
                txt=""
                for c in cols:
                    x = list(df[c])[0]
                    txt += (c + ' '*15)[:16] + str(x) + '\n'
                cols1 = ['id', 'stage', 'name', 'desc']
                df1 = df[cols1]
                txt1 = html_report(df1, cols1, [2,5,9,35], 30)
                cols1 = ['id', 'stage', 'mcq', 'assignment']
                df1 = df[cols1]
                txt2 = html_report(df1, cols1, [2,5,40,40], 30)
                cols1 = ['id', 'stage', 'days','flipclass', 'IU']
                df1 = df[cols1]
                txt3 = html_report(df1, cols1, [2,5,4,30,40], 30)
                html_msg_dict[title] = [txt] + txt1 + txt2 + txt3
            txt = "You are back in the system config menu"
            menu_item = menu_items['syscfg_menu']
            await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
            self.menu_id = keys_dict[menu_txt['option_syscfg']]

        elif self.menu_id == keys_dict[menu_txt['option_cfg_module_iu']]:
            qry =  "SELECT a.* FROM module_iu a INNER JOIN playbooks b ON a.client_name=b.client_name"
            qry += " AND a.module_code=b.module_code AND a.course_code=b.course_code AND a.pillar=b.pillar"
            qry += f" WHERE b.client_name = '{self.client_name}' and b.course_id = '{resp}';"
            title = "stages_master record for " + resp
            cols = ['pillar','course_code','module_code','project_mentoring','project_implementation']
            df = rds_df(qry)
            if df is not None:
                df.columns = get_columns('module_iu')
                txt=""
                for c in cols[:4]:
                    x = list(df[c])[0]
                    txt += (c + ' '*24)[:25] + str(x) + '\n'
                cols1 = ['iu', 'learning_materials']
                df1 = df[cols1]
                txt1 = html_report(df1, cols1, [2,50], 30)
                cols1 = ['iu', 'mcq', 'number_of_qns', 'assignment','flipped_class','assignment_support']
                df1 = df[cols1]
                txt2 = html_report(df1, cols1, [2,3,13,10,13,18], 30)
                html_msg_dict[title] = [txt] + txt1 + txt2
            txt = "You are back in the system config menu"
            menu_item = menu_items['syscfg_menu']
            await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
            self.menu_id = keys_dict[menu_txt['option_syscfg']]

        elif self.menu_id == keys_dict[menu_txt['option_nlp']]:
            txt = "Information not available"
            if resp == menu_txt['nlp_dict']:
                f=html_tbl("", "dictionary", "DICTIONARY TABLE", "dictionary.html")
                if f is None:
                    await self.bot.sendMessage( chat_id, txt )
                else:
                    await self.bot.sendDocument(chat_id, document=f)
            elif resp == menu_txt['nlp_prompts']:
                f=html_tbl("", "prompts", "RESPONSES TABLE", "prompts.html")
                if f is None:
                    await self.bot.sendMessage( chat_id, txt )
                else:
                    await self.bot.sendDocument(chat_id, document=f)
            elif resp == menu_txt['nlp_response']:
                f=html_tbl("", "progress", "RESPONSE TEXT", "response.html")
                if f is None:
                    await self.bot.sendMessage( chat_id, txt )
                else:
                    await self.bot.sendDocument(chat_id, document=f)
            elif resp == menu_txt['nlp_corpus']:
                f=html_tbl("", "ft_corpus", "FASTTEXT CORPUS", "ft_corpus.html")
                if f is None:
                    await self.bot.sendMessage( chat_id, txt )
                else:
                    await self.bot.sendDocument(chat_id, document=f)
            elif resp == menu_txt['nlp_stopwords']:
                f=html_tbl("", "stopwords", "STOPWORDS TABLE", "stopwords.html")
                if f is None:
                    await self.bot.sendMessage( chat_id, txt )
                else:
                    await self.bot.sendDocument(chat_id, document=f)
            elif resp == menu_txt['nlp_faq']:
                f=html_tbl("", "faq", "FAQ TABLE", "faq.html")
                if f is None:
                    await self.bot.sendMessage( chat_id, txt )
                else:
                    await self.bot.sendDocument(chat_id, document=f)
            elif resp == menu_txt['nlp_train']:
                if ft_model.train_model() :
                    retmsg = "NLP model using the corpus table has been trained with model file saved as ft_model.bin"
                else:
                    retmsg = "NLP model using the corpus table was not trained properly"
            elif (resp == option_back) or (resp == "0"):
                txt = "You are back in the main menu"
                menu_item = menu_items['admin_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_admin']]

        elif self.menu_id == keys_dict[menu_txt['option_ml']]:
            txt = "The section handle all the machine learning related processes."
            await self.sender.sendMessage(txt)
            if resp == menu_txt['ml_data']:
                f=html_tbl(self.client_name, "mcqas_info", "ML Model Data", "mcqas_info.html")
                if f is None:
                    txt = "Information not available"
                    await self.bot.sendMessage( chat_id, txt )
                else:
                    await self.bot.sendDocument(chat_id, document=f)
            elif resp == menu_txt['ml_pipeline']:
                txt = 'please wait for a moment.'
                await self.bot.sendMessage( chat_id, txt )
                vmedxlib.generate_mcq_as(self.client_name)
                retmsg = 'Job completed.'
            elif resp == menu_txt['ml_report']:
                retmsg = "Generating profiler report for quick data analysis."
                txt = 'please wait for a moment.'
                await self.bot.sendMessage( chat_id, txt )
                fn="mcqas_info.html"
                await bot.sendDocument(chat_id=self.chatid, document=open(fn, 'rb'))
            elif resp == menu_txt['ml_graph']:
                retmsg = "Generating decision tree graph to explain the model."
                fn = 'mcqas_info.jpg'
                f = open(fn, 'rb')
                await bot.sendPhoto(self.chatid, f)
            elif resp == menu_txt['ml_train']:
                txt = 'please wait for a moment.'
                await self.bot.sendMessage( chat_id, txt )
                retmsg = dt_model.train_model(self.client_name, "dt_model.bin")
                txt = 'model has been retrained.'
                await self.bot.sendMessage( chat_id, txt )
            elif (resp == option_back) or (resp == "0"):
                txt = "You are back in the main menu"
                menu_item = menu_items['admin_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_admin']]

        elif self.menu_id == keys_dict[menu_txt['option_restful']]:
            sid = self.records['sid']
            cname = self.client_name
            cid = self.records['cid']
            edx_api_url = vmedxlib.edx_api_url
            edx_api_header = vmedxlib.edx_api_header
            if (resp in [ menu_txt[x] for x in "rest_sms,rest_grad,rest_mcq,rest_ass,rest_cal".split(",") ]) and (cid==""):
                txt = "Please select the course id before calling the api."
                await self.bot.sendMessage(self.chatid, txt)
                return
            if (resp in [ menu_txt[x] for x in "rest_grad,rest_mcq,rest_ass".split(",") ]) and (sid==0):
                txt = "Please select the student id for generating the student id listing."
                await self.bot.sendMessage(self.chatid, txt)
                return
            if (resp == option_back) or (resp == "0"):
                txt = 'You are in back to admin menu.'
                menu_item = menu_items['admin_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_admin']]
            elif resp == menu_txt['rest_sid']:
                qry = "SELECT u.studentid FROM userdata u INNER JOIN user_master m ON u.client_name=m.client_name "
                qry += f" AND u.studentid=m.studentid WHERE u.client_name='{cname}' AND u.courseid = '{cid}' and u.deferred = 0 "
                qry += ''.join([ " and lower(m.email) not like '%" + x + "'"  for x in bot_intance.efilter])
                df = rds_df( qry )
                if df is None:
                    txt = "Learners information is not available"
                    await self.sender.sendMessage(txt)
                    return
                self.courseid = resp
                df.columns = ['studentid']
                sid_list = sorted([str(x) for x in df.studentid])
                btn_list = build_menu( sid_list, 6, option_back, [])
                self.records['progress_sid'] = [x for x in sid_list if x.isnumeric()]
                txt = "Select a valid student id to test the api :"
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(btn_list))
                self.menu_id = keys_dict[menu_txt['rest_sid']]
            elif resp == menu_txt['rest_cid']:
                txt = "Let's take a look on the following courses."
                courseid_menu = build_menu([x for x in self.list_courseids],1)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(courseid_menu))
                self.menu_id = keys_dict[menu_txt['rest_cid']]
            elif resp == menu_txt['rest_cal']:
                stage_list = vmedxlib.get_google_calendar(cid, cname)
                if stage_list==[]:
                    txt = "Calendar information is not available"
                    await self.bot.sendMessage(self.chatid, txt)
                txt = ""
                for x in stage_list:
                    txt += str(x) + "\n"
                title = f"Calendar for {cid}"
                html_msg_dict[title] = [ txt ]
            elif resp == menu_txt['rest_sms']:
                sid = int(sid)
                df = vmedxlib.sms_df(cid, sid)
                if (df is None) or (len(df)==0):
                    txt = "The attendance api returns nothing."
                    retmsg = txt
                else:
                    lecturer = df.account_name.values[0]
                    class_code = df.class_code.values[0]
                    cohort_code = df.class_cohort_code.values[0]
                    remarks = df.class_remarks.values[0]
                    txt = ""
                    txt += f"Lecturer = {lecturer}\n"
                    txt += f"class_code = {class_code}\n"
                    txt += f"class_cohort_code = {cohort_code}\n"
                    txt += f"class_remarks = {remarks}\n"
                    date_list = vmedxlib.sms_datelist(df)
                    dlist = [ x.strftime('%d/%m/%Y') for x in date_list ]
                    dlist = list(set(dlist))
                    txt += f"List of attendance date matching to stages table:\n"
                    txt += str(dlist) + "\n"
                    df['Weekday'] = df.apply(lambda x: str(x['timetable_day'])[:3], axis=1)
                    df['Date'] = df.apply(lambda x: str(x['timetable_date'])[:10], axis=1)
                    df['Attended'] = df.apply(lambda x: str(x['attendance_type_desc']), axis=1)
                    df['Status'] = df.apply(lambda x: str(x['status_desc']), axis=1)
                    df['Stages'] = df.apply(lambda x: str(x['class_type_code']), axis=1)
                    if sid==0:
                        cols = ['Stages', 'Status', 'Attended', 'Date', 'timetable_day' , 'email' ]
                        tcols = [10, 10, 10, 12, 10, 30]
                        title = f"SMS Attendances for {cohort_code}"
                    else:
                        cols = get_columns("stages")
                        stage_list = vmedxlib.sms_missingdates(cname, cid, sid, cols, date_list)
                        results = [x for x in stage_list if x != '']
                        txt += "Missing stages : " + str(results) + "\n"
                        att_rate = vmedxlib.sms_att_rate(cname, cid, sid, date_list)
                        txt += f"Attendance rate = {att_rate}"
                        cols = ['Stages', 'Status', 'Attended', 'Date', 'timetable_day' ]
                        tcols = [10, 10, 10, 12, 10]
                        title = f"SMS Attendances for {cohort_code} student# {sid}"
                    df1 = df[cols]
                    html_msg_dict[title] = [ txt ] + html_report(df1, cols, tcols, 20)
            elif resp == menu_txt['rest_grad']:
                url = f"{edx_api_url}/user/fetch/grades/list/{sid}"
                resp = requests.post(url, data=cid, headers=edx_api_header, verify=False)
                found = 0
                if resp.status_code==200:
                    data = resp.content.decode('utf-8')
                    if len(data)>0:
                        result  = eval(str(data))
                        txt = ""
                        for x in result:
                            stud = x['student_id']
                            grade = x['grade']
                            txt += f"student_id : {stud}\ngrade : {grade}\n"
                        if txt == "":
                            txt = "No information received from the api"
                        await self.bot.sendMessage(self.chatid, txt)
                        found = 1
                if found==0:
                    txt = "Sorry, no results found."
                    await self.sender.sendMessage(txt)
            elif resp == menu_txt['rest_mcq']:
                url = f"{edx_api_url}/user/fetch/mcq/scores/list/{sid}"
                resp = requests.post(url, data=cid, headers=edx_api_header, verify=False)
                found = 0
                if resp.status_code==200:
                    data = resp.content.decode('utf-8')
                    if len(data)>0:
                        found = 1
                        result = json.loads(data)
                        title = f"MCQ Scores for {cid}"
                        hdr = "IU score points_possible grade attempts options_display_name correctness\n"
                        txt = hdr
                        msg_list = []
                        for rec in [x for x in list(result)]:
                            sc = rec['score']
                            pp = rec['points_possible']
                            od = ""
                            qn = 99
                            if 'options_display_name' in list(rec):
                                od = rec['options_display_name']
                            if 'option_display_name' in list(rec):
                                od = rec['option_display_name']
                            if od.startswith('Q'):
                                qn = getnumstr(od)
                            iu = str(rec['IU'])
                            state = eval(rec['state'].replace('null','""').replace('false','0').replace('true','1'))
                            corr = ""
                            if 'correct_map' in list(state):
                                map_list = list(state['correct_map'])
                                if len(map_list)>0:
                                    statekey = list(state['correct_map'])[0]
                                    corr = state['correct_map'][statekey]['correctness']
                            qnscore = 1.0 if corr=='correct' else 0
                            grade = 1.0 if pp==0 else float(sc/pp)
                            grade = (str(grade) + "     ")[:5]
                            attempts = 0 if qnscore == 0 else ( state['attempts'] if 'attempts' in list(state) else 0)
                            txt += f"{iu}  {sc}     {pp    }           {grade}   {attempts}        {qn}                  {corr}\n"
                            if len(txt)>=800:
                                msg_list.append(txt)
                                txt = hdr
                        if txt != "":
                            msg_list.append(txt)
                        html_msg_dict[title] = msg_list
                if found==0:
                    txt = "Sorry, no results found."
                    await self.sender.sendMessage(txt)
            elif resp == menu_txt['rest_ass']:
                url = f"{edx_api_url}/user/fetch/assignment/scores/list/{sid}"
                resp = requests.post(url, data=cid, headers=edx_api_header, verify=False)
                found = 0
                if resp.status_code==200:
                    data = resp.content.decode('utf-8')
                    data = data.replace('"IU":null','"IU":"0"')
                    if len(data)>0:
                        result  = eval(str(data))
                        txt = ""
                        for x in result:
                            x.pop('student_id')
                            rec = str(x)
                            txt += rec.replace('{','').replace('}','').replace("'","").replace("IU:","\nIU :") + "\n\n"
                        title = f"Assignment Scores for {cid}"
                        html_msg_dict[title] = [ txt ]
                        found = 1
                if found==0:
                    txt = "Sorry, no results found."
                    await self.sender.sendMessage(txt)

        elif (self.menu_id == keys_dict[menu_txt['rest_cid']]) or (self.menu_id == keys_dict[menu_txt['rest_sid']]):
            txt = 'You are in restful api menu.'
            if (resp != option_back) :
                if self.menu_id == keys_dict[menu_txt['rest_cid']]:
                    self.records['cid'] = resp
                    txt = course_status(self.client_name, resp)
                    if txt=="":
                        txt = 'You are in restful api menu.'
                elif self.menu_id == keys_dict[menu_txt['rest_sid']]:
                    sid_list = self.records['progress_sid']
                    if resp.isnumeric():
                        sid = int(resp)
                        txt = self.userinfo(sid)
                    else:
                        sid = 0
                    if resp in sid_list:
                        self.records['sid'] = sid
                    else:
                        txt = 'Please select valid student_id'
                        await self.sender.sendMessage(txt)
                        return
            menu_item = menu_items['rest_menu']
            await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
            self.menu_id = keys_dict[menu_txt['option_restful']]

        elif self.menu_id == keys_dict[menu_txt['option_datacheck']]:
            if (resp == option_back) or (resp == "0"):
                txt = "You are back in the admin menu"
                menu_item = menu_items['admin_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_admin']]
            elif resp == menu_txt['option_course_module']:
                query = f"SELECT p.course_id FROM playbooks p left JOIN course_module c ON p.client_name=c.client_name and "
                query += f"p.module_code=c.module_code WHERE p.client_name = '{self.client_name}' and c.module_code IS NULL"
                result = "List of courses with pillar/course_code not registered in course_module table\n"
                df = rds_df(query)
                if df is None:
                    txt = "Sorry, no results found."
                    await self.sender.sendMessage(txt)
                    return
                df.columns = ['course_id']
                title = result
                html_msg_dict[title] = html_report(df, df.columns, [50], 25)
            elif resp == menu_txt['option_module_iu']:
                query = "SELECT aa.pillar, aa.course_code, aa.module_code, aa.mcq_cnt as iutbl_mcq,"
                query += "bb.iu_mcq as stage_mcq,aa.as_cnt as iutbl_as,bb.iu_as as stage_as "
                query += "FROM ( SELECT distinct m.pillar , m.course_code , m.module_code, "
                query += "  SUM(m.mcq) AS mcq_cnt,SUM(m.assignment) AS as_cnt,sum(if(m.iu>0,1,0)) AS cnt "
                query += "  FROM module_iu m where m.client_name = '_c_' "
                query += " group by m.pillar , m.course_code , m.module_code ) aa INNER JOIN "
                query += "( SELECT distinct pillar, course_code, module_code, "
                query += "max(CAST( SUBSTRING_INDEX(mcq, ',', -1) as UNSIGNED)) AS iu_mcq, "
                query += "max(CAST( SUBSTRING_INDEX(assignment, ',', -1) as UNSIGNED)) AS iu_as FROM stages_master "
                query += "where client_name = '_c_' GROUP BY pillar, course_code, module_code ) bb "
                query += "ON aa.pillar=bb.pillar AND aa.course_code=bb.course_code AND aa.module_code=bb.module_code "
                query += "WHERE NOT( aa.mcq_cnt=bb.iu_mcq AND aa.as_cnt=bb.iu_as);"
                query = query.replace('_c_',self.client_name)
                result = "# of IUs not match for MCQ or Assignment\n"
                df = rds_df(query)
                if df is None:
                    txt = "Sorry, no results found."
                    await self.sender.sendMessage(txt)
                    return
                df.columns = ['pillar', 'course_code','module_code', 'iutbl_mcq', 'stage_mcq', 'iutbl_as', 'stage_as']
                title = result
                html_msg_dict[title] = html_report(df, df.columns, [10, 15, 10, 10, 10, 10, 10], 25)
            elif resp == menu_txt['option_stages_master']:
                query = "SELECT distinct p.pillar, p.course_code,p.module_code "
                query += "from playbooks p left JOIN stages_master c ON p.client_name=c.client_name "
                query += "AND p.pillar=c.pillar AND p.course_code=c.course_code AND p.module_code=c.module_code "
                query += f"WHERE p.client_name = '{self.client_name}' and c.module_code IS NULL "
                query += "ORDER BY p.pillar, p.course_code,p.module_code;"
                result = "courses with learning stages not registered in stages_master table\n"
                df = rds_df(query)
                if df is None:
                    txt = "Sorry, no results found."
                    await self.sender.sendMessage(txt)
                    return
                df.columns = ['pillar', 'course_code','module_code']
                title = result
                html_msg_dict[title] = html_report(df, df.columns, [10, 25, 15], 25)
            elif resp == menu_txt['option_module_code']:
                query = "select pillar, course_code, module_code, module_name "
                query += f" from course_module a where a.client_name = '{self.client_name}' "
                query += " AND a.module_code IN ( select module_code "
                query += " from (select module_code, count(*) "
                query += f" as cnt from course_module where client_name = '{self.client_name}' "
                query += " group by module_code ) b where cnt>1 ) order by module_code; "
                result = "module code is not unique in course_module table.\n"
                df = rds_df(query)
                if df is None:
                    txt = "Sorry, no results found."
                    await self.sender.sendMessage(txt)
                    return
                df.columns = ['pillar', 'course_code', 'module_code', 'module_name']
                title = result
                html_msg_dict[title] = html_report(df, df.columns, [7, 17, 12, 48], 30)
            elif resp == menu_txt['option_stages_count']:
                query = "SELECT pillar , course_code, module_code, course_id, defined, actual FROM ( "
                query += "SELECT a.pillar , a.course_code, a.module_code, a.course_id, "
                query += "(SELECT COUNT(stage) FROM stages_master WHERE client_name=a.client_name AND pillar=a.pillar "
                query += "AND course_code=a.course_code AND module_code=a.module_code) AS defined, "
                query += "(SELECT COUNT(stage) FROM stages WHERE client_name=a.client_name AND courseid=a.course_id) AS actual "
                query += f"FROM playbooks a WHERE a.client_name = '{self.client_name}') zz WHERE defined <> actual AND defined >0 "
                query += "ORDER BY pillar , course_code, module_code, course_id;"
                result = "Actual # of stages not matched.\n"
                df = rds_df(query)
                if df is None:
                    txt = "Sorry, no results found."
                    await self.sender.sendMessage(txt)
                    return
                df.columns = ['pillar', 'course_code', 'module_code', 'course_id', 'defined', 'actual']
                title = result
                html_msg_dict[title] = html_report(df, df.columns, [7, 17, 12, 48, 8, 10], 25)

        elif self.menu_id == keys_dict[menu_txt['option_alerts']]:
            if resp == option_back:
                txt = "You are back in the main menu"
                menu_item = menu_items['admin_menu']
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                self.menu_id = keys_dict[menu_txt['option_admin']]
                return
            elif resp == menu_txt['option_intervention']:
                txt = "Processing intervention check now."
                await self.sender.sendMessage(txt)
                await auto_intervent(bot_intance.client_name, bot_intance.resp_dict, bot_intance.pass_rate, self.chatid)
                txt = "Auto intervention check completed"
                await self.sender.sendMessage(txt)
            elif resp == menu_txt['option_reminder']:
                txt = "Processing reminder check now."
                await self.sender.sendMessage(txt)
                await auto_notify(bot_intance.client_name, bot_intance.resp_dict, bot_intance.pass_rate, self.chatid)
                txt = "Auto reminder check completed"
                await self.sender.sendMessage(txt)
            elif resp == menu_txt['option_automsg']:
                query = f"select `value` from params WHERE client_name='{self.client_name}' AND `key`='automsg';"
                timing = rds_param(query)
                if int(timing)==0:
                    txt = "Auto alerts timing has been disabled\n"
                else:
                    txt = f"Current alert timing in hhmm format = {timing}\n"
                txt += "Enter the new timing (or 0 to disable):"
                await self.sender.sendMessage(txt)
                self.menu_id = keys_dict[menu_txt['option_automsg']]

        elif self.menu_id == keys_dict[menu_txt['option_automsg']]:
            if resp.isnumeric():
                timing = int(resp)
                if timing >=0 and (int(timing/100) < 24) and ((timing % 100) < 60):
                    query = f"UPDATE params SET `value`='{resp}'  WHERE client_name='{self.client_name}' AND `key`='automsg';"
                    rds_update(query)
                    if timing>0:
                        retmsg = f"Auto alerts timing has been scheduled to {resp} hrs."
                    else:
                        retmsg = "Auto alerts timing has been disabled"
            else:
                retmsg = "Alerts timing has to be in hhmm format (or 0 to disable):"
            self.menu_id = keys_dict[menu_txt['option_alerts']]

        elif self.menu_id == keys_dict[menu_txt['option_chatlist']]:
            if chat_id in bot_intance.chat_list:
                tid = bot_intance.chat_list[ chat_id ]
                await bot.sendMessage(tid , resp)
                txt = "(type bye when you want to end the conversation)"
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup([['bye']]))
                self.menu_id = keys_dict[menu_txt['option_chat']]
            else:
                rlist = resp.replace('     ','*').split('*')
                if (resp == option_back) or (resp == "0"):
                    if self.is_admin :
                        txt = "You are back in the main menu"
                        if self.super_admin:
                            menu_item = menu_items['svcbot_menu']
                        else:
                            menu_item = menu_items['mentor_menu']
                        await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                        self.menu_id = 1
                    else:
                        txt = "bye"
                        menu_item = self.menu_home.copy()
                        await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                        self.menu_id = keys_dict[menu_txt['lrn_student']]
                elif len(rlist)>=2 :
                    sid = rlist[1]
                    tid = rlist[3]
                    if sid.isnumeric():
                        tid = int(tid)
                        if tid in bot_intance.chat_list:
                            retmsg = "User " + sid + " is on another conversation."
                        else:
                            sid = int(sid)
                            (status, info) = self.livechat( sid, tid )
                            if status == 1:
                                tid = bot_intance.chat_list[self.chatid]
                                if self.chatid in bot_intance.user_list:
                                    user_from = bot_intance.user_list[self.chatid][2]
                                else:
                                    user_from = self.username
                                txt = '❚█══ Live Chat ══█❚\nHi ' + info + ', you are in the live chat with : <a href=\"tg://user?id=' + str(self.chatid) + '">' + user_from + '</a>'
                                await self.bot.sendMessage(tid, txt, parse_mode='HTML')
                                txt = '❚█══ Live Chat ══█❚\nHi, you are in the live chat with : <a href=\"tg://user?id=' + str(tid) + '">' + info + '</a>'
                                await self.bot.sendMessage(self.chatid, txt, parse_mode='HTML')
                                txt = "(type bye when you want to end the conversation)"
                                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup([[option_back]]))
                                self.menu_id = keys_dict[menu_txt['option_chat']]
                            elif status == 3:
                                txt = f"<pre>Hi, {self.username}({sid}) would like to private chat with you</pre>"
                                txt += '\nContact : <a href=\"tg://user?id=' + str(self.chatid) + '">@' + self.chatname + '</a>'
                                await self.bot.sendMessage(info[2], txt, parse_mode='HTML')
                                txt = f"Chat request sent to {info[1]} #{info[0]}."
                                if self.super_admin:
                                    menu_item = menu_items['svcbot_menu']
                                else:
                                    menu_item = menu_items['mentor_menu']
                                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                                self.menu_id = 1
                            else:
                                txt = info
                                await self.sender.sendMessage(txt)

        elif self.menu_id == keys_dict[menu_txt['option_chat']]:
            if self.chatid in [d for d in bot_intance.chat_list]:
                tid = bot_intance.chat_list[self.chatid]
                if resp.lower() == 'bye':
                    tid = self.endchat()
                    if tid > 0:
                        txt = "Live chat session disconnected. 👋"
                        await self.bot.sendMessage(tid, txt)
                        await self.bot.sendMessage(chat_id, txt)
                    if self.is_admin :
                        txt = "You are back in the main menu"
                        bot_intance.user_list[ tid ][4] = ""
                        self.menu_id = 1
                    else:
                        txt = "bye"
                        self.menu_id = keys_dict[menu_txt['lrn_student']]
                    menu_item = self.menu_home.copy()
                    await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
                else:
                    await bot.sendMessage(tid, resp)
                    txt = ''
            else:
                if self.is_admin :
                    txt = "Welcome back to main menu"
                    self.menu_id = 1
                else:
                    txt = "Live chat has been ended."
                    self.menu_id = keys_dict[menu_txt['lrn_student']]
                menu_item = self.menu_home.copy()
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))

        elif chat_id in bot_intance.chat_list and (resp.strip() != "") :
            if resp.lower() == 'bye':
                tid = self.endchat()
                if tid > 0:
                    txt = "Live chat session disconnected. 👋"
                    await self.bot.sendMessage(tid, txt)
                    await self.bot.sendMessage(chat_id, txt)
            else:
                if self.menu_id != keys_dict[menu_txt['option_chat']]:
                    await self.bot.sendMessage(chat_id, "(type bye when you want to end the conversation)", reply_markup=self.reply_markup([['bye']]))
                    self.menu_id = keys_dict[menu_txt['option_chat']]
                tid = bot_intance.chat_list[chat_id]
                await bot.sendMessage(tid, resp)

        elif self.menu_id == keys_dict[menu_txt['option_faq']] and resp in ['0', 'exit', option_back]:
            txt = "FAQ option is closed."
            menu_item = self.menu_home.copy()
            await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(menu_item))
            self.menu_id = keys_dict[menu_txt['lrn_student']]

        elif (self.menu_id != keys_dict[menu_txt['option_chat']]) and (self.chatid in [d for d in bot_intance.chat_list]):
            tid = bot_intance.chat_list[ chat_id ]
            await bot.sendMessage(tid , resp)
            txt = "you said : "
            await self.bot.sendMessage(chat_id,txt + resp, reply_markup=self.reply_markup([['bye']]))
            self.menu_id = keys_dict[menu_txt['option_chat']]

        elif (self.menu_id == keys_dict[menu_txt['lrn_student']]) or (self.menu_id == keys_dict[menu_txt['option_faq']]) :
            syslog( str(self.student_id) + " Q:" + resp )
            (txt,recommendation) = await self.runfaq(resp)
            if (txt != ""):
                await self.sender.sendMessage(txt)
                self.menu_id = keys_dict[menu_txt['lrn_student']]
            if len(recommendation) > 0:
                rec_menu = build_menu(recommendation,1,option_back,[])
                txt = "You might want to ask :"
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(rec_menu))
                txt = ""
                self.menu_id = keys_dict[menu_txt['option_faq']]

        for title in list(html_msg_dict):
            for msg in [ x for x in html_msg_dict[title] if x != "" ]:
                txt = "<b>" + title + "</b>\n" + "<pre>" + msg +  "</pre>"
                await bot.sendMessage(chat_id, txt, parse_mode='HTML')

        while retmsg != "":
            txt = retmsg[:4000]
            retmsg = retmsg[4000:]
            await self.sender.sendMessage(txt)
        return

def verify_student(cname, userdata, student_id, courseid, stagedf):
    global bot_intance
    vars = dict()
    amt = 0
    sid = int(student_id)
    if userdata is None:
        query = f"select * from userdata where client_name = '{cname}' and studentid={sid} and courseid='{courseid}';"
        df = rds_df(query)
        if df is not None:
            df.columns = bot_intance.userdata_cols
            userdata = df
    if userdata is None:
        return (msg, vars)

    vars = load_vars(userdata, student_id)
    if vars == {}:
        return (f"Sorry there is no data for this student id {student_id}", vars)
    vars['asst_email'] = ""
    vars['mentor_email'] = ""
    amt = vars['amt']
    student_name = vars['username']
    stage = vars['stage']
    grade = vars['grade']
    if vars['deferred']==1:
        return (f"This student id {student_id} has been deferred from this  course.", vars)
    if stagedf is None:
        condqry = f" client_name = '{cname}' and courseid = '{courseid}'"
        query = "select * from stages where " + condqry
        stage_table = rds_df(query)
        if stage_table is None:
            msg = f'Unable to load stage table for course-id {courseid}'
            return (msg, vars)
        stage_table.columns = bot_intance.stages_cols
    else:
        stage_table = stagedf.copy()

    stage_list = [x for x in stage_table.name]
    date_list = [x for x in stage_table.stagedate]
    vars['stage_dates'] = date_list
    vars['stage_names'] = stage_list
    msg = '\n\nHi ' + student_name + " !\n"
    if stage is None:
        stage = stage_list[0]
        vars['stage'] = stage
    msg += 'You are currently on stage ' + stage + "\n\n"

    qry = f"select cohort_id from playbooks where client_name = '{cname}' and course_id = '{courseid}';"
    cohort_id = rds_param(qry)
    if cohort_id=="":
        cohort_id = getcohort(course_id)
    qry = f"select module_code from playbooks where client_name = '{cname}' and course_id = '{courseid}';"
    module_code = rds_param(qry)
    if module_code=="":
        module_code = piece(cohort_id,'-',0)
    qry = f"select assistance_email from course_module where client_name = '{cname}' and module_code = '{module_code}';"
    asst_email = rds_param(qry)
    query = f"select mentor from playbooks where course_id='{courseid}' and client_name = '{cname}';"
    mentor_email = rds_param(query)
    if mentor_email.strip()=="":
        qry = f"select mentor_email from course_module where client_name = '{cname}' and module_code = '{module_code}';"
        mentor_email = rds_param(qry)
    vars['asst_email'] = asst_email
    vars['mentor_email'] = mentor_email
    return (msg, vars)

def evaluate_progress(vars,iu_list,passingrate,var_prefix,var_title):
    global bot_intance
    max_iu_cnt = bot_intance.max_iu
    avg_prefix = var_prefix + "_avg"
    att_prefix = var_prefix + "_attempts"
    score_zero = [] ; iu_score = [] ; iu_attempts = [] ; score_pass = [] ; score_failed = []
    score_avg = 0 ; tt = '' ; iu_cnt = 0 ; attempts_balance = "" ; list_iu = []
    if iu_list != '0' :
        list_iu = iu_list.split(',')[:max_iu_cnt]
        list_iu = [ x.strip() for x in list_iu]
        iu_attempts = [ vars[x] for x in [ att_prefix + x for x in list_iu ]]
        iu_vars = sorted_numlist( str(iu_list) )[:max_iu_cnt]
        iu_att = dict(zip(iu_vars, iu_attempts))
        vlist = [ vars[avg_prefix + str(x)] for x in iu_vars ]
        score_pass = [ x for x in iu_vars if vars[avg_prefix + str(x)]>=passingrate]
        score_zero = [ x for x in iu_vars if vars[avg_prefix + str(x)] == 0 ]
        score_failed = [ x for x in iu_vars if x not in (score_pass + score_zero) ]
        iu_score = [ vars[x] for x in [ avg_prefix + x for x in list_iu ]]
        iu_score = [ eval(str(x)) for x in iu_score]
        iu_arr = dict(zip(iu_vars, iu_score))
        iu_attempts = [ 1 if iu_arr[x]>0 else iu_att[x] for x in iu_att ]
        score_avg = sum(iu_score)/len(iu_score) if iu_score != [] else 0
    return (tt, score_avg, score_zero, score_pass, score_failed, iu_score , iu_attempts, iu_cnt, attempts_balance)

def get_stageinfo(vars, pass_rate, amt, stagecode, mcqvars, asvars):
    ma_list = [] ; list_att = [] ; tt = "" ; t1 = '' ; t2 = ''
    mcnt = 0 ; mcq_avg = 0 ; mcq_zero = [] ; mcq_pass = [] ; mcq_failed = [] ; mbal = ""
    acnt = 0 ; as_avg = 0 ; as_zero = [] ; as_pass = [] ; as_failed = [] ; abal = ""
    mcq_att = [] ; as_att = []
    (t1, mcq_avg, mcq_zero, mcq_pass, mcq_failed, mcqas_list1, mcq_att, mcnt, mbal) = evaluate_progress(vars, mcqvars, pass_rate, 'mcq','MCQ')
    ma_list += mcqas_list1
    list_att += mcq_att
    (t2, as_avg, as_zero, as_pass, as_failed, mcqas_list2, as_att, acnt, abal) = evaluate_progress(vars, asvars, pass_rate, 'as','Assignment')
    ma_list += mcqas_list2
    list_att += as_att
    mm = len(ma_list)
    has_score = 1 if mm > 0 else 0
    ascore  = sum(ma_list)/mm if mm > 0 else 0
    ascore = round(ascore*100)/100
    mcqas_comp = 1 if len([n+1 for n in range(len(list_att)) if list_att[n]==0])==0 else 0
    max_att = 0 if len(list_att)==0 else max(list_att)

    pass_stage = 0
    if stagecode.lower()=="soc" and amt == 0:
        pass_stage = 1
    if (mcqas_comp==1) and (ascore>= pass_rate) and (mm>0):
        pass_stage = 1
    if "eoc" in stagecode.lower():
        pass_stage = 0 if amt > 0 else 1

    tt = t1 + t2
    risk_level = 0
    if len(mcq_zero) > 0:
        risk_level = 1
    if len(as_zero) > 0:
        risk_level = 1
    if (mm > 0) and (ascore < pass_rate) and (max_att<4):
        risk_level = 1
    if (mm > 0) and (ascore < pass_rate) and (ascore > 0):
        risk_level = 1
    if len(mcq_zero)>0 and (ascore==0):
        risk_level = 1
    if amt > 0 :
        pass_stage = 0

    return (pass_stage, has_score, ascore, ma_list, max_att, list_att, mcq_avg, mcq_zero, mcq_pass, mcq_failed, mcq_att, mcnt, mbal, \
        as_avg, as_zero, as_pass, as_failed, as_att, acnt, abal, mcqas_comp, risk_level, tt)

def load_progress(df, student_id, vars, client_name, resp_dict, pass_rate, att_pass_rate, stagedf):
    vars['reminder'] = ""
    vars['intervention'] = ""
    vars['mcqvars'] = ""
    vars['mcq_attempts'] = ""
    vars['mcq_avg'] = 0
    vars['asvars'] = ""
    vars['as_attempts'] = ""
    vars['as_avg'] = 0
    if (df is None) or (vars == {}) or (stagedf is None):
        return ("", "", vars)
    if len(stagedf)==0:
        return ("", "", vars)
    gmt = bot_intance.gmt
    rec = df[df.studentid==student_id].iloc[0]
    courseid = rec['courseid']
    course_id = courseid
    sid = student_id
    dt = datetime.datetime.now() + datetime.timedelta(hours=gmt)
    dtnow = dt.date().strftime('%Y%m%d')
    list1=[datetime.datetime.strptime(dt,"%d/%m/%Y").strftime('%Y%m%d') for dt in stagedf.startdate]
    list2=[x for x in list1 if x <= dtnow ]
    if list2==[]:
        return ("", "", vars)
    stagebyschedule = stagedf.iloc[ (len(list2) - 1) ]['name']
    stg_list = [x for x in stagedf.stage]
    stglen = len(stg_list)
    if stglen==0:
        return ("", "", vars)
    df = stagedf.copy()
    try:
        begin_date_list = ['' if x is None else x[:10] for x in df.startdate]
    except:
        return ("", "", vars)

    if list1[0] > dtnow:
        return ("_soc_", "", vars)
    qry = f"select email from user_master where client_name = '{client_name}' and studentid = {sid};"
    email = rds_param(qry)

    due_date_list = [x for x in df.stagedate]
    start_date_list = [x for x in df.startdate]
    fcdt = list1[0]
    stg_date = due_date_list[0]
    mcqdate = stg_date
    asdate  = stg_date
    eldate  = stg_date
    fcdate  = stg_date
    asdt = eldt = fcdt = 0
    att_rate = 0
    pm_stage = 0
    for n in range(stglen):
        if (list1[n] >= dtnow) and (stg_list[n][0]=='A') and (asdt==0):
            asdt = 1
            asdate = due_date_list[n]
        if (list1[n] >= dtnow) and (stg_list[n][:2]=='EL') and (eldt==0):
            eldt = 1
            eldate = due_date_list[n]
        if (list1[n] >= dtnow) and (stg_list[n][:2]=='FC') and (fcdt==0):
            fcdt = 1
            mcqdate = fcdate = due_date_list[n]

    stage_names_list = [x for x in df.name]
    stage_desc_list = [x for x in df.desc]
    stage_daysnum_list = [x for x in df.days]
    mcqvars_list = [x for x in df.mcq]
    mcqlist_max = max(mcqvars_list)
    asvars_list = [x for x in df.assignment]
    aslist_max = max(asvars_list)
    mcqvars = ""
    asvars = ""
    amt = vars['amt']
    defer_mode = 0
    try:
        df = vmedxlib.sms_df(courseid, sid)
        att_list = [x for x in df.attendance_type_desc.str.lower() if x.startswith('d') or x.startswith('w')]
        if len(att_list)>0:
            defer_mode = 1
        if df is None:
            date_list = []
        else:
            date_list = vmedxlib.sms_datelist(df)
        missing_dates = vmedxlib.sms_missingdates(client_name, courseid, sid, bot_intance.stages_cols, date_list)
        att_rate = vmedxlib.sms_att_rate(client_name, courseid, sid, date_list)
    except:
        missing_dates = ['' for n in range(stglen)]
        date_list = []
        att_rate = 0.0
    pm_stage = vmedxlib.sms_pmstage(client_name, courseid)
    missing_stages = []
    stagebyprogress = ""
    statusbyprogress = ""
    mcnt = 0
    acnt = 0
    mcq_avg = 0
    as_avg = 0
    overall_passed = 1
    missed_stage = ""
    stgcode = ""
    txt = ''
    stage_date = ""
    stagebyprogress = ""
    mcq_pending = []
    assignment_pending = []
    mfail = []
    afail = []
    n1 = 0
    syslog(f"{courseid} {sid}")
    for n in range(stglen):
        stagecode = stg_list[n]
        stagename = stage_names_list[n]
        stage_missing = missing_dates[n]
        f2f_missing = 0 if stage_missing=='' else 1
        if (f2f_missing==1):
            missing_stages.append(stagecode)
        stg_date = due_date_list[n]
        duedate = due_date_list[n]
        stagedesc = stage_desc_list[n]
        if '(' in stagedesc:
            stagedesc = stagedesc.split('(')[0]
        mcqvars = mcqvars_list[n]
        asvars = asvars_list[n]
        if ('SA' in stagecode) or (stagecode=='EOC'):
            mcqvars = mcqlist_max
            asvars = aslist_max
        (pass_stage,has_score,avg_score,mcqas_list,max_attempts,list_attempts,mcq_avg,mcq_zero,mcq_pass,mcq_failed,mcq_attempts,mcnt,\
        mcq_att_balance,as_avg,as_zero,as_pass,as_failed,as_attempts,acnt,as_att_balance,mcqas_complete,risk_level,tt) \
            = get_stageinfo(vars, pass_rate, amt, stagecode, mcqvars, asvars)
        if (stagebyprogress == "") and (pass_stage == 0):
            stagebyprogress = stagename
            statusbyprogress = f"{stagename} ({stagedesc})"
            overall_passed = 0
            mcq_pending = mcq_zero
            assignment_pending = as_zero
            mfail = mcq_failed
            afail = as_failed
            stgcode = stagecode
        if stagebyschedule == stagename:
            mcqvars = mcqvars_list[n]
            asvars = asvars_list[n]
            stagedesc = stage_desc_list[n]
            stg_date = due_date_list[n]
            duedate = due_date_list[n]
            date_from = begin_date_list[n]
            stage_days = stage_daysnum_list[n]
            vars['stage'] = stagebyschedule
            txt = tt + "\n\n"
            n1 = n
            if stagebyprogress != "":
                mcq_pending = mcq_zero
                assignment_pending = as_zero
                mfail = mcq_failed
                afail = as_failed
                stage_date = stg_date
                break
        if n1 > 0 and n == n1:
            if (stagebyprogress == ""):
                stagebyprogress = stagename
            break

    f2f_stage = ','.join(missing_stages)
    pass_stage = overall_passed
    stg_date = stage_date
    stage = stagebyschedule
    mcq_zero = mcq_pending
    as_zero = assignment_pending
    mcq_failed = mfail
    as_failed = afail
    stagecode = stgcode
    stagedesc = stagedesc.replace('?','')
    stage_desc = stagedesc
    stagebyprogress = stage if stagebyprogress=='' else stagebyprogress

    if stagebyprogress in stage_names_list:
        n1 = stage_names_list.index(stagebyprogress)
        start_dt = start_date_list[n1]
        txt = ''.join([ stage_names_list[n] + ' (' + stage_desc_list[n]+')\n' for n in range(stglen) if start_date_list[n] == start_dt])
        statusbyprogress = '\n' + txt
    else:
        statusbyprogress = f"{stagename} ({stagedesc})"
    vars['stage'] =  stagebyprogress

    if stagecode=="":
        if stagebyprogress in stage_names_list:
            n = stage_names_list.index(stagebyprogress)
            stagecode = stg_list[n]
            stage_desc = stage_desc_list[n]

    if stage in stage_names_list:
        n1 = stage_names_list.index(stage)
        start_dt = start_date_list[n1]
        txt = '\n'.join([ stage_names_list[n] + ' (' + stage_desc_list[n]+')' for n in range(stglen) if start_date_list[n] == start_dt])
        stage = '\n' + txt
        stage_desc = ""

    mcqlist = list(set(mcq_failed + mcq_zero))
    aslist = list(set(as_failed + as_zero))
    mcq_iu_list = '' if mcq_zero == [] else iu_reading(mcq_zero)
    as_iu_list = '' if as_zero == [] else iu_reading(as_zero)
    due_date = duedate

    notif3 = resp_dict['notif3']
    notif4 = resp_dict['notif4']
    notif5 = resp_dict['notif5']
    notif6 = resp_dict['notif6']
    notif7 = resp_dict['notif7']
    notif8 = resp_dict['notif8']
    notif9 = resp_dict['notif9']
    notif10 = resp_dict['notif10']
    notif11 = resp_dict['notif11']
    notif12 = resp_dict['notif12']
    resp8 = resp_dict['resp8']
    uname = vars['username']
    txt = resp_dict['notif0']
    if len(mcq_zero) > 0:
        txt += resp_dict['notif1']
    if len(as_zero) > 0:
        txt += resp_dict['notif2']
    txt += "\n"
    if '{uname}' in txt:
        txt = txt.replace('{uname}', uname)
    if '{stagename}' in txt:
        txt = txt.replace('{stagename}', stagename)
    if '{stagedesc}' in txt:
        txt = txt.replace('{stagedesc}', stagedesc)
    if '{mcq_zero}' in txt:
        txt = txt.replace('{mcq_zero}', str(mcq_zero))
    if '{mcq_failed}' in txt:
        txt = txt.replace('{mcq_failed}', str(mcq_failed))
    if '{as_zero}' in txt:
        txt = txt.replace('{as_zero}', str(as_zero))
    if '{as_failed}' in txt:
        txt = txt.replace('{as_failed}', str(as_failed))
    if '{mcq_iu_list}' in txt:
        txt = txt.replace('{mcq_iu_list}', str(mcq_iu_list))
    if '{as_iu_list}' in txt:
        txt = txt.replace('{as_iu_list}', str(as_iu_list))
    if '{due_date}' in txt:
        txt = txt.replace('{due_date}', str(due_date))
    if '{email}' in txt:
        txt = txt.replace('{email}' , email)
    if '{lf}' in txt:
        txt = txt.replace('{lf}', "\n")
    vars['reminder'] = txt

    iu_list = [int(x) for x in str(mcqvars).split(',') if x!='0' and x.isnumeric() ]
    mcq_zero_iu = [x for x in iu_list if x in mcq_zero]
    mcq_failed_iu = [x for x in iu_list if x in mcq_failed]
    eoc_gap = vmedxlib.edx_eocgap(client_name, courseid, 7)
    f2f_error = 0 if f2f_stage=="" else 1
    qry = f"select cohort_id from playbooks where client_name = '{client_name}' and course_id = '{courseid}';"
    cohort_id = rds_param(qry)
    if cohort_id=="":
        cohort_id = getcohort(courseid)
    qry = f"select module_code from playbooks where client_name = '{client_name}' and course_id = '{courseid}';"
    module_code = rds_param(qry)
    if module_code=="":
        module_code = piece(cohort_id,'-',0)
    qry = f"select enquiry_email from course_module where client_name = '{client_name}' and module_code = '{module_code}';"
    enquiry_email = rds_param(qry)

    txt = ""
    if eoc_gap == 1:
        eoc7 = vmedxlib.edx_eocend(client_name, courseid, 7)
        if eoc7==1:
            txt = notif12
        else:
            txt = notif10
    else:
        if not((len(mcq_zero) + len(as_zero) + len(mcq_failed) + len(as_failed) == 0 ) and (f2f_error==0)):
            txt = notif3
            if len(mcq_zero) > 0:
                txt += notif4
            if len(mcq_failed) > 0:
                txt += notif5
            if len(as_zero) > 0:
                txt += notif6
            if len(as_failed) > 0:
                txt += notif7
            if f2f_error == 1:
                if pm_stage==0:
                    resp11 = notif11
                    txt += notif8
                else:
                    if att_rate >= att_pass_rate:
                        txt += notif8
                    else:
                        txt += notif9
            txt += "\n"

    if '{uname}' in txt:
        txt = txt.replace('{uname}', uname)
    if '{course_id}' in txt:
        txt = txt.replace('{course_id}', courseid)
    if '{sid}' in txt:
        txt = txt.replace('{sid}', str(sid))
    if '{stagename}' in txt:
        txt = txt.replace('{stagename}', stagename)
    if '{stagedesc}' in txt:
        txt = txt.replace('{stagedesc}', stagedesc)
    if '{mcq_zero}' in txt:
        txt = txt.replace('{mcq_zero}', str(mcq_zero))
    if '{mcq_failed}' in txt:
        txt = txt.replace('{mcq_failed}', str(mcq_failed))
    if '{as_zero}' in txt:
        txt = txt.replace('{as_zero}', str(as_zero))
    if '{as_failed}' in txt:
        txt = txt.replace('{as_failed}', str(as_failed))
    if '{iu_list}' in txt:
        txt = txt.replace('{iu_list}', str(iu_list))
    if '{mcq_zero_iu}' in txt:
        txt = txt.replace('{mcq_zero_iu}', str(mcq_zero_iu))
    if '{mcq_failed_iu}' in txt:
        txt = txt.replace('{mcq_failed_iu}', str(mcq_failed_iu))
    if '{due_date}' in txt:
        txt = txt.replace('{due_date}', str(due_date))
    if '{asdate}' in txt:
        txt = txt.replace('{asdate}' ,  str(asdate) )
    if '{f2f_stage}' in txt:
        txt = txt.replace('{f2f_stage}' , f2f_stage)
    if '{enquiry_email}' in txt:
        txt = txt.replace('{enquiry_email}' , enquiry_email)
    if '{email}' in txt:
        txt = txt.replace('{email}' , email)
    if '{lf}' in txt:
        txt = txt.replace('{lf}', "\n")
    vars['intervention'] = txt

    txt = ''
    txt_hdr = resp_dict['stg0']
    if ("eoc" in stagebyschedule.lower()) and (pass_stage == 1) and (att_rate >=att_pass_rate):
        txt = ""
        txt_hdr = "_eoc_"
    else:
        txt_hdr += resp_dict['stg1']
    if '{stage_desc}' in txt_hdr:
        txt_hdr = txt_hdr.replace('{stage_desc}' , stage_desc)
    if '{username}' in txt_hdr:
        txt_hdr = txt_hdr.replace('{username}' , vars['username'])
    if '{stage}' in txt_hdr:
        txt_hdr = txt_hdr.replace('{stage}' , stage)
    if '{stagebyschedule}' in txt_hdr:
        txt_hdr = txt_hdr.replace('{stagebyschedule}' , stagebyschedule)
    if '{stagebyprogress}' in txt_hdr:
        txt_hdr = txt_hdr.replace('{stagebyprogress}' , statusbyprogress)
    if '{lf}' in txt_hdr:
        txt_hdr = txt_hdr.replace('{lf}' , '\n')
    if '()' in txt_hdr:
        txt_hdr = txt_hdr.replace('()', '')

    if (pm_stage==1) and (att_rate < att_pass_rate):
        risk_level = 1

    query = f"update userdata set risk_level = {risk_level}, stage = '{stagebyprogress}', mcq_zero = '{mcq_zero}' "
    query += f" ,mcq_failed = '{mcq_failed}', as_zero = '{as_zero}', as_failed = '{as_failed}', deferred = {defer_mode}"
    query += f" where client_name='{client_name}' and courseid='{courseid}' and studentid={student_id};"
    try:
        rds_update(query)
    except:
        pass

    for vv in ['stage', 'stagecode', 'mcqdate', 'asdate', 'eldate', 'fcdate', 'avg_score', 'mcqas_complete', 'mcq_pass', 'mcq_failed', 'missed_stage', "aslist", "mcqvars", "asvars", \
            'has_score', 'pass_stage', 'max_attempts', 'mcqas_list', 'mcq_zero', 'mcq_avg', 'as_pass', 'as_failed', 'risk_level', 'stg_list', 'mcqlist', 'pm_stage', 'att_rate', \
            'mcnt', 'acnt', 'as_avg', 'as_zero', 'f2f_stage', 'stagebyprogress', 'mcq_attempts',  'mcq_att_balance', 'as_att_balance', 'as_attempts', 'duedate'] :
        vars[vv] = eval(vv)
    syslog("completed")
    return (txt_hdr, txt, vars)

def learner_progress(vars,iu_list,passingrate,var_prefix,var_title):
    global bot_intance
    max_iu_cnt = bot_intance.max_iu
    avg_prefix = var_prefix + "_avg"
    att_prefix = var_prefix + "_attempts"
    score_zero = [] ; iu_score = [] ; iu_attempts = [] ; score_pass = [] ; score_failed = []
    score_avg = 0 ; tt = '' ; iu_cnt = 0 ; attempts_balance = "" ; list_iu = []
    if iu_list != '0' :
        list_iu = iu_list.split(',')[:max_iu_cnt]
        list_iu = [ x.strip() for x in list_iu]
        iu_attempts = [ vars[x] for x in [ att_prefix + x for x in list_iu ]]
        iu_vars = sorted_numlist( str(iu_list) )[:max_iu_cnt]
        iu_att = dict(zip(iu_vars, iu_attempts))
        vlist = [ vars[avg_prefix + str(x)] for x in iu_vars ]
        score_pass = [ x for x in iu_vars if vars[avg_prefix + str(x)]>=passingrate]
        score_zero = [ x for x in iu_vars if vars[avg_prefix + str(x)] == 0 ]
        score_failed = [ x for x in iu_vars if x not in (score_pass + score_zero) ]
        iu_score = [ vars[x] for x in [ avg_prefix + x for x in list_iu ]]
        iu_score = [ eval(str(x)) for x in iu_score]
        iu_arr = dict(zip(iu_vars, iu_score))
        iu_attempts = [ 1 if iu_arr[x]>0 else iu_att[x] for x in iu_att ]
        score_avg = sum(iu_score)/len(iu_score) if iu_score != [] else 0
        tt += "\n" + var_title + " average test score : " + "{:.2%}".format(score_avg)
        if len(score_pass) > 0:
            tt += "\n☑ " + var_title + " test passed : " + str(score_pass)
        if len(score_failed) > 0:
            tt += "\n🙅🏻‍♀️ " + var_title + " test failed : " + str(score_failed)
        if len(score_zero) > 0:
            tt += "\n🤷‍♂️ " + var_title + " test pending : " + str(score_zero)
        #iu_cnt = len(score_pass) + len(score_failed)
        #m = 4 if var_prefix=="mcq" else 1
        #attempts_balance = "".join([ ("\n" + var_title + ' #'+str(x) + " has " + str(m-vars[att_prefix + str(x)])+" attempts left"  ) for x in iu_vars \
        #    if vars[avg_prefix + str(x)] < passingrate ])
        #tt += attempts_balance
    return tt

def display_progress(df, sdf, sid, vars, client_name, resp_dict, pass_rate=0.7, att_pass_rate=0.75):
    if len(list(vars)) == 0:
        return "Your information is incomplete, please do not proceed and inform you faculty admin."
    (txt1, txt2, vars) = load_progress(df, sid, vars, client_name, resp_dict, pass_rate, att_pass_rate, sdf)
    txt = txt1 + txt2
    vars['course_alive'] = 1
    if txt == "":
        vars['notification'] = "Your information is incomplete, please do not proceed and inform you faculty admin."
        vars['course_alive'] = 0
        return vars
    if txt == "_soc_":
        vars['notification'] = "Hi, the course is not yet ready for now."
        vars['course_alive'] = 0
        return vars
    if txt == "_eoc_":
        vars['notification'] = "Congratulations, you have reached the end of the course."
        vars['course_alive'] = 0
        return vars
    syslog("started")
    cid = vars['courseid']
    sid = vars['studentid']
    stg = vars['stage']
    stagebyprogress = vars['stagebyprogress']
    stagecode   = vars['stagecode']
    stg_list    = vars['stg_list']
    pass_stage  = vars['pass_stage']
    pmlist = [ x for x in stg_list if x[:2]=='PM' ]
    f2f_stage   = vars['f2f_stage']
    f2f_error  = 0 if f2f_stage=="" else 1
    risk_level = vars['risk_level']
    mcq_zero   = vars['mcq_zero']
    mcq_failed = vars['mcq_failed']
    mcq_att    = vars['mcq_attempts']
    mcqlist    = vars['mcqlist']
    as_zero    = vars['as_zero']
    as_failed  = vars['as_failed']
    duedate    = vars['duedate']
    amt        = vars['amt']
    att_rate   = vars['att_rate']
    pm_stage   = vars['pm_stage']
    mcq_att_balance = vars['mcq_att_balance']
    course_id = vars['courseid']
    qry = f"SELECT course_name FROM playbooks WHERE client_name = '{client_name}' and course_id = '{course_id}';"
    course_name = rds_param(qry)
    vars['course_name'] = course_name
    if (pm_stage==1) and (att_rate < att_pass_rate):
        f2f_error=1
    if vars['has_score'] == 1:
        txt += resp_dict['avg_score']
    if stg.lower() == "soc days":
        if amt == 0 :
            resp_amt0 = resp_dict['amt0']
            txt += resp_amt0 + "\n\n"
        elif amt > 0:
            resp_amt1 = resp_dict['amt1']
            txt += resp_amt1 + "\n\n"
    else:
        if amt > 0 :
            resp0 = resp_dict['resp0']
            txt += resp0 + "\n\n"

    if vars['mcqas_complete']==1 and vars['avg_score']>= pass_rate :
        resp10 = resp_dict['resp10']
        txt += resp10 + "\n\n"

    if f2f_error == 1:
        if pm_stage==0:
            resp11 = resp_dict['resp11']
            txt += resp11 + '\n\n'
        else:
            if att_rate >= att_pass_rate:
                resp11 = resp_dict['resp11']
                txt += resp11 + '\n\n'
            else:
                resp12 = resp_dict['resp12']
                txt += resp12 + '\n\n'

    if len(vars['mcq_zero']) > 0 and vars['avg_score'] == 0 :
        resp2 = resp_dict['resp2']
        txt += resp2 + "\n\n"
    if len(vars['mcqas_list'])>0 and vars['avg_score'] < pass_rate and vars['max_attempts'] < 4 :
        resp3 = resp_dict['resp3']
        txt += resp3 + "\n\n"
    if len(vars['mcqas_list'])>0 and vars['avg_score'] > 0 and vars['avg_score'] < pass_rate \
        and vars['max_attempts'] >= 4 :
        resp4 = resp_dict['resp4']
        txt += resp4 + "\n\n"
    if len(vars['mcq_zero']) > 0 :
        resp5 = resp_dict['resp5']
        txt += resp5.replace("{mcqlist}", str(vars['mcq_zero'])) + "\n\n"
    if len(vars['as_zero']) > 0 :
        resp6 = resp_dict['resp6']
        txt += resp6.replace("{aslist}" , str(vars['as_zero'])) + "\n\n"

    if '{lf}' in txt:
        txt = txt.replace('{lf}' , '\n')
    if '{course_name}' in txt:
        txt = txt.replace('{course_name}' , course_name)
    if '{f2f_stage}' in txt:
        txt = txt.replace('{f2f_stage}' , f2f_stage)
    if '{stage}' in txt:
        txt = txt.replace('{stage}' , stg)
    if '{avg_score}' in txt:
        txt = txt.replace('{avg_score}' , "{:.2%}".format(vars['avg_score']))
    if '{amt}' in txt:
        txt = txt.replace('{amt}' ,  "{:8.2f}".format(amt).strip() )
    if '{mcqdate}' in txt:
        txt = txt.replace('{mcqdate}' , str(vars['mcqdate']))
    if '{asdate}' in txt:
        txt = txt.replace('{asdate}' ,  str(vars['asdate']))
    if '{eldate}' in txt:
        txt = txt.replace('{eldate}' ,  str(vars['eldate']))
    if '{fcdate}' in txt:
        txt = txt.replace('{fcdate}' ,  str(vars['fcdate']))
    if '{duedate}' in txt:
        txt = txt.replace('{duedate}' ,  str(duedate))
    if '{due_date}' in txt:
        txt = txt.replace('{due_date}' ,  str(duedate))
    if '{mcq_att_balance}' in txt:
        txt = txt.replace('{mcq_att_balance}' ,  mcq_att_balance )

    vars['notification'] = txt
    syslog("completed")
    return vars

def grad_pred_text(vars, client_name, use_neural_network = False):
    txt = ""
    global dt_model
    syslog("started")
    if (dt_model.model_name != "") and ((vars['mcnt'] + vars['acnt']) >0):
        grad_pred = dt_model.predict(vars['mcq_avg'] , vars['as_avg'], vars['mcnt'], vars['acnt'])
        txt += "\n\nAI grading prediction : " +  "{:.2%}".format(grad_pred[0]) + "\n\n"
    syslog("completed")
    return txt

def load_vars(df, sid):
    global bot_intance
    if df is None:
        return {}
    ulist = [x for x in df.studentid]
    if sid not in ulist:
        return {}
    vars = df[df.studentid==sid].iloc[0].to_dict()
    for x in list(vars):
        if (x in ['amt', 'grade']) or ('_avg' in x):
            try:
                vars[x] = eval(str(vars[x]))
            except:
                vars[x] = 0
    vars['mcq_due_dates'] = []
    vars['as_due_dates'] = []
    max_iu_cnt = bot_intance.max_iu
    for n in range(14, max_iu_cnt+1):
        xvar = "mcq_avg" + str(n)
        if xvar not in list(vars):
            vars[xvar] = 0
            xvar = "mcq_attempts" + str(n)
            vars[xvar] = 0
            xvar = "as_avg" + str(n)
            vars[xvar] = 0
            xvar = "as_attempts" + str(n)
            vars[xvar] = 0
    return vars

def job_request(chat_id,client_name,func_req,func_param):
    global bot_intance
    gen_job_id = lambda : (''.join(random.choice( "ABCDEFGHJKLMNPQRTUVWXY0123456789" ) for i in range(12))).upper()
    job_id = gen_job_id()
    bot_intance.job_items[job_id]=[chat_id,func_req,func_param]
    syslog(f"job_request {job_id} {chat_id} {func_req} {func_param}")
    return

def rds_loadcourse(client_name, stud):
    course_id_list = []
    course_name_list = []
    qry0 = "SELECT DISTINCT ud.courseid, pb.course_name, pb.eoc,ud.deferred FROM userdata ud INNER JOIN playbooks pb "
    qry0 += "ON ud.client_name=pb.client_name AND ud.courseid=pb.course_id INNER JOIN "
    qry0 += "course_module m ON m.client_name=pb.client_name AND m.module_code=pb.module_code "
    qry = qry0 + f"WHERE m.enabled=1 and ud.deferred=0 and ud.studentid={str(stud)} and ud.client_name='{client_name}';"
    df = rds_df(qry)
    course_id_list = []
    course_name_list = []
    logon_selection = []
    if df is not None:
        df.columns = ['courseid','course_name','eoc', 'deferred']
        course_id_list = [x for x in df.courseid]
        course_name_list = [x for x in df.course_name]
        eoc_list = [x for x in df.eoc]
        deferred_list = [x for x in df.deferred]
        m = len(course_id_list)
        for n in range(m):
            cid = course_id_list[n]
            deferred = deferred_list[n]
            try:
                eoc = vmedxlib.edx_endofcourse(client_name, cid)
            except:
                eoc = eoc_list[n]
            if eoc == 0:
                logon_selection.append(cid)
            if eoc==1:
                eoc_gap = vmedxlib.edx_eocgap(client_name, cid, 7)
                if (eoc_gap == 1) and (deferred==0):
                    logon_selection.append(cid)
    return (course_id_list, course_name_list, logon_selection)

def analyze_cohort(course_id, userdata, bot, chat_id):
    global dt_model
    max_iu_cnt = bot_intance.max_iu
    try:
        x = dt_model.score
    except:
        dt_model = vmaiglib.MLGrader()
        dt_model.load_model("dt_model.bin")

    itemlist = lambda x,y : "\n".join([ ",".join(x[n*y:][:y]) for n in range(int((len(x)+y-1)/y))])
    above_70 = lambda x : True if x >= 0.7 else False
    between_3070 = lambda x : True if (x > 0.3 and x < 0.7) else False
    below_30 = lambda x : True if x <= 0.3 else False
    above_2 = lambda x : True if x >= 2 else False

    def report_lines(udict, rdict, rpt_summary, title, lambda_fn, fmt = '{:.2%}'):
        rlist = [ r for r in rdict if lambda_fn(rdict[r]) ]
        if rlist!=[]:
            n = len(rlist)
            rpt_summary.append(title + f" ({n} learners)")
            for r in rlist:
                lines = (str(r) + ' '*8)[:8] + (udict[r] + ' '*30)[:30] + fmt.format(rdict[r])
                rpt_summary.append( lines )
            rpt_summary.append("")
        return rpt_summary

    syslog("started")
    result_table = [ [' ' for m in range(4)] for n in range(max_iu_cnt) ]
    summary_mcq = []
    summary_as = []
    df0 = userdata[userdata.deferred == 0]
    sid_list = [x for x in df0.studentid]
    uname_list = [x for x in df0.username]
    udict = dict(zip(sid_list,uname_list))
    cnt = len(sid_list)
    avgsum_list = [ df0[ "mcq_avg" + str(x) ].mean() for x in range( 1, max_iu_cnt + 1 ) ]
    rr = [ 1 if r>0 else 0 for r in avgsum_list ]
    rsum = sum(rr)
    idx = 0
    if rsum>0:
        for n in range(max_iu_cnt) :
            if rr[n]>0:
                result_table[idx][0] = '# ' + str(n+1)
                result_table[idx][1] = "{:.2%}".format(avgsum_list[n])
                idx+=1
        df = df0[[ "mcq_avg" + str(x) for x in range( 1, rsum + 1 ) ]]
        df["mcq_avg"] = df.apply(lambda x: x.mean(), axis=1)
        avg_list = [x for x in df.mcq_avg]
        rdict = dict(zip(sid_list,avg_list))
        df = df0[[ "mcq_attempts" + str(x) for x in range( 1, rsum + 1 ) ]]
        df["mcq_attempts"] = df.apply(lambda x: x.mean(), axis=1)
        avg_list = [x for x in df.mcq_attempts]
        xdict = dict(zip(sid_list,avg_list))
        summary_mcq = report_lines(udict, rdict, summary_mcq, "70% and above", above_70)
        summary_mcq = report_lines(udict, rdict, summary_mcq, "Between 30% and 70%", between_3070)
        summary_mcq = report_lines(udict, rdict, summary_mcq, "Below 30%", below_30)
        summary_mcq = report_lines(udict, xdict, summary_mcq, "2 or more attempts", above_2, '{:.2}')

    avgsum_list = [ df0[ "as_avg" + str(x) ].mean() for x in range( 1, max_iu_cnt + 1 ) ]
    rr = [ 1 if r>0 else 0 for r in avgsum_list ]
    rsum = sum(rr)
    idx = 0
    if rsum>0:
        for n in range(max_iu_cnt) :
            if rr[n]>0:
                result_table[idx][2] = '# ' + str(n+1)
                result_table[idx][3] = "{:.2%}".format(avgsum_list[n])
                idx+=1
        df = df0[[ "as_avg" + str(x) for x in range( 1, rsum + 1 ) ]]
        df["as_avg"] = df.apply(lambda x: x.mean(), axis=1)
        avg_list = [x for x in df.as_avg]
        rdict = dict(zip(sid_list,avg_list))
        summary_as = report_lines(udict, rdict, summary_as, "70% and above", above_70)
        summary_as = report_lines(udict, rdict, summary_as, "Between 30% and 70%", between_3070)
        summary_as = report_lines(udict, rdict, summary_as, "Below 30%", below_30)

    if result_table==[]:
        syslog("completed with no information found.")
        return ([],[],[],0)
    else:
        syslog("completed")

    return (result_table, summary_mcq, summary_as, cnt)

async def checkjoblist():
    global bot_intance
    job_list = list(bot_intance.job_items)
    if job_list==[]:
        return
    job_id = job_list[0]
    jobitem = bot_intance.job_items[job_id]
    chat_id = int(jobitem[0])
    func_req = jobitem[1]
    func_param = jobitem[2]
    #await bot_intance.bot.sendMessage(chat_id, f"Job #{job_id} {func_req} started")
    await runbotjob(job_id,chat_id,func_req,func_param)
    #await bot_intance.bot.sendMessage(chat_id, f"Job #{job_id} {func_req} completed")
    return

async def runbotjob(job_id,chat_id,func_req,func_param):
    global edx_api_header, edx_api_url,bot_intance
    client_name = bot_intance.client_name
    func_svc_list = ["update_assignment" , "update_mcq" , "edx_import", "update_schedule"]
    txt = "job "
    if func_req == "generate_mcq_as":
        try:
            vmedxlib.generate_mcq_as(func_param)
            txt += " completed successfully."
        except:
            txt += " failed."
    elif func_req in ["edx_mass_import", "mass_update_assignment", "mass_update_mcq", "mass_update_schedule", "mass_update_usermaster"]:
        if func_req == "mass_update_assignment":
            vmedxlib.mass_update_assignment(client_name)
        elif func_req == "mass_update_mcq":
            vmedxlib.mass_update_mcq(client_name)
        elif func_req == "mass_update_schedule":
            vmedxlib.mass_update_schedule(client_name)
        elif func_req == "edx_mass_import":
            vmedxlib.edx_mass_import(client_name)
        txt += " completed successfully."
    elif func_req in func_svc_list :
        course_id = func_param
        if func_req == "update_assignment":
            vmedxlib.update_assignment(course_id, client_name)
        elif func_req == "update_mcq":
            vmedxlib.update_mcq(course_id, client_name)
        elif func_req == "update_schedule":
            vmedxlib.update_schedule(course_id, client_name)
        elif func_req == "edx_import":
            vmedxlib.edx_import(course_id, client_name)
        txt += " completed successfully."
    elif func_req == 'load_edxdata':
        load_edxdata(client_name)
    else:
        try:
            func_svc = func_req + "(" + func_param + ")"
            status = eval(func_svc)
            if status:
                txt += " completed successfully."
            else:
                txt += " failed."
        except:
            txt += " failed."
    syslog(f"completed job {job_id}\t" + txt  )
    del bot_intance.job_items[job_id]
    return

def course_status(clt, cid):
    qry = f"select module_code from playbooks where client_name = '{clt}' and course_id = '{cid}';"
    module_code = rds_param(qry)
    if module_code=="":
        txt = "module_code/cohort_id is not defined in the playbooks table\n"
    else:
        txt = f"course_id is _w_\n"
        txt += f"module_code is _x_\n"
        qry = f"select course_code from playbooks where client_name = '{clt}' and course_id = '{cid}';"
        course_code = rds_param(qry)
        txt += f"course_code is _y_\n"
        qry = f"select pillar from playbooks where client_name = '{clt}' and course_id = '{cid}';"
        pillar = rds_param(qry)
        txt += f"pillar is _z_\n"
        qry = f"select count(*) as cnt from course_module where client_name = '{clt}' and module_code = '{module_code}';"
        cnt = rds_param(qry)
        txt += "✖️ not " if cnt==0 else "✔️ "
        txt += " defined in course_module table\n"
        if cnt>1:
            txt += f"⁉️ module code is not unique in course_module table\n"
        qry = f"select count(*) as cnt from module_iu where client_name = '{clt}' and module_code = '{module_code}';"
        cnt = rds_param(qry)
        txt += "✖️ not " if cnt==0 else "✔️ "
        txt += " defined in module_iu table\n"
        qry = f"select count(*) as cnt from stages_master where client_name = '{clt}' and module_code = '{module_code}';"
        cnt = rds_param(qry)
        txt += "✖️  not " if cnt==0 else "✔️ "
        txt += " defined in stages_master table\n"
        txt = txt.replace('_w_', cid)
        txt = txt.replace('_x_', module_code)
        txt = txt.replace('_y_', course_code)
        txt = txt.replace('_z_', pillar)
    return txt

def stage_calendar(df):
    m = 5
    xstr = lambda x : '   ' if x == 0 else ('  ' + str(x))[-3:]
    ystr = lambda x,y,z : y[x] if x in list(z) else '     '
    zstr = lambda x : ((' *' if ',' in x else x) + ' '*m)[:m]
    slist = [(x + ''*m)[:m] for x in df.stage]
    tlist = [datetime.datetime.strptime(dt,"%d/%m/%Y").strftime('%Y%m%d') for dt in df.startdate]
    ylist = list(set([int(t[:4]) for t in tlist]))
    mlist = dict()
    for yy in ylist:
        mlist[yy] = sorted(set([ int(x[4:][:2]) for x in tlist if int(x[:4])==yy]))
    sdict = dict()
    cnt=len(tlist)
    for n in range(cnt):
        dt = str(tlist[n])
        sdict[dt] = ""
    for n in range(cnt):
        stg = slist[n]
        dt = str(tlist[n])
        txt = sdict[dt]
        if txt == "":
            sdict[dt] = stg
        else:
            sdict[dt] = txt + "," + stg
    title_list = []
    calendar.setfirstweekday(calendar.SUNDAY)
    msg = ""
    gap = ' '*m
    for yy in ylist:
        for mm in mlist[yy]:
            weeklist = calendar.monthcalendar(yy,mm)
            title = datetime.datetime(yy, mm, 1).strftime('%Y %B')
            dlist = [ int(x[-2:]) for x in tlist if int(x[4:][:2])==mm and int(x[:4])==yy]
            flist = [ sdict[x] for x in tlist if int(x[4:][:2])==mm and int(x[:4])==yy]
            rdict = dict(zip(dlist,flist))
            msg += title + '\nSun  Mon  Tue  Wed  Thu  Fri  Sat\n'
            for ww in weeklist:
                msg += ''.join([ (xstr(y)+gap)[:m] for y in ww]) + '\n'
                msg += ''.join([ zstr(rdict[x]) if x in list(rdict) else gap for x  in ww]) + '\n'
            msg += '\n'
            for dt in list(rdict):
                tt = rdict[dt]
                if "," in tt:
                    msg += str(dt) + '/' + str(mm) + ' : ' + tt + '\n'
            msg += '\n'
    return msg

async def auto_notify(client_name, resp_dict, pass_rate, adm_chatid):
    global bot_intance
    if adm_chatid>0:
        txt = "auto_notify started"
        await bot_intance.bot.sendMessage(adm_chatid, txt)
    # T -1 days currently, to be generic next time
    qry = "SELECT a.* FROM stages a INNER JOIN playbooks b ON a.client_name=b.client_name AND a.courseid=b.course_id "
    qry += "INNER JOIN course_module c ON b.client_name=c.client_name AND b.module_code=c.module_code WHERE c.enabled=1 and "
    qry += f"a.client_name = '{client_name}' AND "
    qry += f"STR_TO_DATE(a.startdate,'%d/%m/%Y') = DATE_ADD(CURDATE() , INTERVAL 1 DAY);"
    stagedf = rds_df(qry)
    if stagedf is None:
        return
    mdf = rds_df(f"SELECT * FROM user_master where client_name='{client_name}';")
    if mdf is None:
        return
    s_cols = bot_intance.stages_cols
    u_cols = bot_intance.userdata_cols
    mdf.columns = bot_intance.usermaster_cols
    stagedf.columns = s_cols
    course_list = [x for x in stagedf.courseid]
    stg_list = [x for x in stagedf.stage]
    stgname_list = [x for x in stagedf.name]
    stgdesc_list = [x for x in stagedf.desc]
    mcqvars_list = [x for x in stagedf.mcq]
    asvars_list = [x for x in stagedf.assignment]
    duedate_list = [x for x in stagedf.stagedate]
    count = len(course_list)
    if count == 0:
        return
    syslog("auto_notify started")
    efilter = bot_intance.efilter
    notif0 = resp_dict['notif0']
    notif1 = resp_dict['notif1']
    notif2 = resp_dict['notif2']
    for n in range(count):
        course_id = course_list[n]
        stagecode = stg_list[n]
        stagename = stgname_list[n]
        stagedesc = stgdesc_list[n]
        stagedesc = stagedesc.replace('?','')

        due_date = duedate_list[n]
        mcqvars = mcqvars_list[n]
        asvars = asvars_list[n]
        query = f"select * from stages where client_name='{client_name}' and courseid='{course_id}';"
        sdf = rds_df(query)
        if sdf is None:
            continue
        sdf.columns = s_cols

        query = f"select * from userdata where client_name='{client_name}' and courseid='{course_id}' and deferred=0;"
        udf = rds_df(query)
        if udf is None:
            continue
        udf.columns = u_cols

        ulist = [x for x in udf.studentid]
        uname_list = [x for x in udf.username]
        udict = dict(zip(ulist,uname_list))
        err_msg = ""
        err_list = []
        sent_list = []
        for sid in ulist:
            uname = udict[sid]
            df1 = udf[ udf.studentid == sid ].copy()
            (tt, vars) = verify_student(client_name, df1, sid, course_id, sdf)
            df2 = mdf[ mdf.studentid == sid ].copy()
            email = df2.email.values[0]
            if email.lower().split('@')[1] in efilter:
                del df1, df2
                continue
            usrtyp = df2.usertype.values[0]
            if usrtyp != 1:
                del df1, df2
                continue
            tid = df2.chat_id.values[0]
            binded = df2.binded.values[0]
            tid = 0 if binded==0 else tid
            amt = vars['amt']
            (pass_stage, has_score, avg_score, mcqas_list, max_attempts, list_attempts, mcq_avg, mcq_zero, mcq_pass, mcq_failed, mcq_attempts, mcnt, \
            mcq_att_balance, as_avg, as_zero, as_pass, as_failed, as_attempts, acnt, as_att_balance, mcqas_complete, risk_level, tt) \
                = get_stageinfo(vars, pass_rate, amt, stagecode, mcqvars, asvars)
            mcq_iu_list = '' if mcq_zero == [] else iu_reading(mcq_zero)
            as_iu_list = '' if as_zero == [] else iu_reading(as_zero)
            txt = notif0
            if len(mcq_zero) > 0:
                txt += notif1
            if len(as_zero) > 0:
                txt += notif2
            txt += "\n"
            if '{uname}' in txt:
                txt = txt.replace('{uname}', uname)
            if '{stagename}' in txt:
                txt = txt.replace('{stagename}', stagename)
            if '{stagedesc}' in txt:
                txt = txt.replace('{stagedesc}', stagedesc)
            if '{mcq_zero}' in txt:
                txt = txt.replace('{mcq_zero}', str(mcq_zero))
            if '{mcq_failed}' in txt:
                txt = txt.replace('{mcq_failed}', str(mcq_failed))
            if '{as_zero}' in txt:
                txt = txt.replace('{as_zero}', str(as_zero))
            if '{as_failed}' in txt:
                txt = txt.replace('{as_failed}', str(as_failed))
            if '{mcq_iu_list}' in txt:
                txt = txt.replace('{mcq_iu_list}', str(mcq_iu_list))
            if '{as_iu_list}' in txt:
                txt = txt.replace('{as_iu_list}', str(as_iu_list))
            if '{due_date}' in txt:
                txt = txt.replace('{due_date}', str(due_date))
            if '{lf}' in txt:
                txt = txt.replace('{lf}', "\n")

            tlist = txt.split('\n')
            txt1 = "Hi" + ", " + uname + " !\n"
            txt2 = '\n'.join(tlist[1:])
            txt = txt1 + txt2

            if tid <= 0:
                err_list.append(sid)
            else:
                try:
                    await bot_intance.bot.sendMessage(int(tid), txt)
                    sent_list.append(sid)
                except:
                    err_list.append(sid)
            del df1, df2

        msg = ""
        if len(sent_list) > 0:
            msg += f"Succesfully sent reminder to learners from {course_id} :\n{str(sent_list)} \n"
        if len(err_list) > 0:
            msg += f"Unable to send reminder to learners from {course_id} :\n{str(err_list)} \n"
        if (len(msg) > 0) and (adm_chatid > 0):
            try:
                await bot_intance.bot.sendMessage(adm_chatid, msg)
            except:
                syslog(msg)
        syslog(msg)
    syslog("auto_notify completed")
    if adm_chatid > 0:
        txt = "auto_notify completed"
        await bot_intance.bot.sendMessage(adm_chatid, txt)
    return

async def auto_intervent(client_name, resp_dict, pass_rate, adm_chatid):
    global bot_intance
    date_today = datetime.datetime.now().date()
    yrnow = str(date_today.strftime('%Y'))[-2:]
    if adm_chatid > 0:
        txt = "auto_intervent started"
        await bot_intance.bot.sendMessage(adm_chatid, txt)
    query = "SELECT DISTINCT a.courseid FROM userdata a "
    query += " INNER JOIN playbooks b ON a.client_name=b.client_name AND a.courseid=b.course_id "
    query += " INNER JOIN course_module c ON b.client_name=c.client_name AND b.module_code=c.module_code "
    query += f" WHERE c.enabled=1 and a.client_name = '{client_name}' "
    query += f" AND SUBSTRING(courseid,-2)='{yrnow}' ORDER BY courseid;"
    df = rds_df(query)
    if df is None:
        course_list = []
    else:
        df.columns = ['courseid']
        course_list= [x for x in df.courseid]

    mdf = rds_df(f"SELECT * FROM user_master where client_name='{client_name}';")
    if mdf is None:
        return
    mdf.columns = bot_intance.usermaster_cols
    syslog("auto_intervent started")
    vars = dict()
    txt =  ""
    efilter = bot_intance.efilter
    s_cols = bot_intance.stages_cols
    u_cols = bot_intance.userdata_cols
    att_pass_rate = bot_intance.att_rate
    risk_level = 0
    notif3 = resp_dict['notif3']
    notif4 = resp_dict['notif4']
    notif5 = resp_dict['notif5']
    notif6 = resp_dict['notif6']
    notif7 = resp_dict['notif7']
    notif8 = resp_dict['notif8']
    notif9 = resp_dict['notif9']
    notif10 = resp_dict['notif10']
    notif11 = resp_dict['notif11']
    notif12 = resp_dict['notif12']
    resp8 = resp_dict['resp8']
    eoc7_maildict = dict()
    eoc7_userdict = dict()
    eoc7_mailcnt = 0
    for course_id in course_list:
        syslog(course_id)
        eoc = vmedxlib.edx_endofcourse(client_name, course_id)
        soc = vmedxlib.edx_course_started(client_name, course_id)
        eoc_gap = vmedxlib.edx_eocgap(client_name, course_id, 7)
        qry = f"select cohort_id from playbooks where client_name = '{client_name}' and course_id = '{course_id}';"
        cohort_id = rds_param(qry)
        if cohort_id=="":
            cohort_id = getcohort(course_id)
        qry = f"select module_code from playbooks where client_name = '{client_name}' and course_id = '{course_id}';"
        module_code = rds_param(qry)
        if module_code=="":
            module_code = piece(cohort_id,'-',0)
        qry = f"select enquiry_email from course_module where client_name = '{client_name}' and module_code = '{module_code}';"
        enquiry_email = rds_param(qry)
        if (soc == 1) and ((eoc == 0) or (eoc_gap == 1)):
            query = f"select * from stages where client_name='{client_name}' and courseid='{course_id}';"
            sdf = rds_df(query)
            if sdf is None:
                continue
            sdf.columns = s_cols
            query = f"select * from userdata where client_name='{client_name}' and courseid='{course_id}' and deferred=0;"
            udf = rds_df(query)
            if udf is None:
                continue
            udf.columns = u_cols
            err_list = []
            sent_list = []
            ulist = [x for x in udf.studentid]
            uname_list = [x for x in udf.username]
            udict = dict(zip(ulist,uname_list))
            for sid in ulist:
                uname = udict[sid]
                df1 = udf[ udf.studentid == sid ].copy()
                df2 = mdf[ mdf.studentid == sid ].copy()
                email = df2.email.values[0]
                if email.lower().split('@')[1] in efilter:
                    del df1, df2
                    continue
                usrtyp = df2.usertype.values[0]
                if usrtyp != 1:
                    del df1, df2
                    continue
                tid = df2.chat_id.values[0]
                binded = df2.binded.values[0]
                tid = 0 if binded==0 else tid
                (tt, vars) = verify_student(client_name, df1, sid, course_id, sdf)
                (txt1, txt2, vars) = load_progress(df1, sid, vars, client_name, resp_dict, pass_rate, att_pass_rate, sdf)
                if txt1 in ["_soc_","_eoc_"]:
                    syslog( txt1 + "\t" + course_id + "\t" + str(sid) )
                    continue
                stagecode = vars['stagecode']
                f2f_stage = vars['f2f_stage']
                f2f_error = 0 if f2f_stage=="" else 1
                stage = vars['stage']
                amt = vars['amt']
                mcq_zero = vars['mcq_zero']
                mcq_failed = vars['mcq_failed']
                as_zero = vars['as_zero']
                as_failed = vars['as_failed']
                asdate =  vars['asdate']
                risk_level = vars['risk_level']
                att_rate = vars['att_rate']
                pm_stage = vars['pm_stage']
                if (pm_stage == 1) and (att_rate < att_pass_rate):
                    f2f_error = 1
                df = sdf[ sdf.stage == stagecode ].copy()
                m = len(df)
                if m == 0:
                    syslog(f"no matching record in stages table for {stagecode} {course_id} {sid}")
                    continue
                else:
                    stageid = df.id.values[0]
                    stagecode = df.stage.values[0]
                    stagename = df.name.values[0]
                    stagedesc = df.desc.values[0]
                    mcqvars = df.mcq.values[0]
                    asvars = df.assignment.values[0]
                    f2fvars = df.f2f.values[0]
                due_date = vars['duedate']
                stagedesc = stagedesc.replace('?','')
                iu_list = mcqvars
                iu_list = [int(x) for x in str(iu_list).split(',') if x!='0' and x.isnumeric() ]
                mcq_zero_iu = [x for x in iu_list if x in mcq_zero]
                mcq_failed_iu = [x for x in iu_list if x in mcq_failed]
                eoc7 = 0

                if eoc_gap == 1:
                    try:
                        eoc7 = vmedxlib.edx_eocend(client_name, course_id, 7)
                    except:
                        pass
                    if eoc7==1:
                        if enquiry_email not in list(eoc7_maildict):
                            eoc7_maildict[enquiry_email]=''
                            eoc7_userdict[enquiry_email]=''
                        txt = notif12
                    else:
                        txt = notif10
                else:
                    txt = notif3
                    if (len(mcq_zero) + len(as_zero) + len(mcq_failed) + len(as_failed) == 0 ) and (f2f_error==0):
                        txt += resp8
                        continue
                    if len(mcq_zero) > 0:
                        txt += notif4
                    if len(mcq_failed) > 0:
                        txt += notif5
                    if len(as_zero) > 0:
                        txt += notif6
                    if len(as_failed) > 0:
                        txt += notif7
                    if f2f_error == 1:
                        if pm_stage==0:
                            resp11 = resp_dict['resp11']
                            txt += notif8
                        else:
                            if att_rate >= att_pass_rate:
                                txt += notif8
                            else:
                                txt += notif9
                txt += "\n"
                if '{uname}' in txt:
                    txt = txt.replace('{uname}', uname)
                if '{course_id}' in txt:
                    txt = txt.replace('{course_id}', course_id)
                if '{email}' in txt:
                    txt = txt.replace('{email}', email)
                if '{sid}' in txt:
                    txt = txt.replace('{sid}', str(sid))
                if '{stagename}' in txt:
                    txt = txt.replace('{stagename}', stagename)
                if '{stagedesc}' in txt:
                    txt = txt.replace('{stagedesc}', stagedesc)
                if '{mcq_zero}' in txt:
                    txt = txt.replace('{mcq_zero}', str(mcq_zero))
                if '{mcq_failed}' in txt:
                    txt = txt.replace('{mcq_failed}', str(mcq_failed))
                if '{as_zero}' in txt:
                    txt = txt.replace('{as_zero}', str(as_zero))
                if '{as_failed}' in txt:
                    txt = txt.replace('{as_failed}', str(as_failed))
                if '{iu_list}' in txt:
                    txt = txt.replace('{iu_list}', str(iu_list))
                if '{mcq_zero_iu}' in txt:
                    txt = txt.replace('{mcq_zero_iu}', str(mcq_zero_iu))
                if '{mcq_failed_iu}' in txt:
                    txt = txt.replace('{mcq_failed_iu}', str(mcq_failed_iu))
                if '{due_date}' in txt:
                    txt = txt.replace('{due_date}', str(due_date))
                if '{asdate}' in txt:
                    txt = txt.replace('{asdate}' ,  str(asdate) )
                if '{f2f_stage}' in txt:
                    txt = txt.replace('{f2f_stage}' , f2f_stage)
                if '{enquiry_email}' in txt:
                    txt = txt.replace('{enquiry_email}' , enquiry_email)
                if '{lf}' in txt:
                    txt = txt.replace('{lf}', "\n")

                tlist = txt.split('\n')
                txt1 = "Hi" + ", " + uname + " !\n"
                txt2 = '\n'.join(tlist[1:])
                txt = txt1 + txt2

                if eoc7==1:
                    eoc7_maildict[enquiry_email] += txt + "\n"
                    eoc7_userdict[enquiry_email] += ',' + uname
                    eoc7_mailcnt += 1
                else:
                    if tid <= 0:
                        err_list.append(sid)
                    else:
                        try:
                            await bot_intance.bot.sendMessage(tid, txt)
                            sent_list.append(sid)
                        except:
                            err_list.append(sid)
                del df1, df2
            del udf
            msg = ""
            if len(sent_list) > 0:
                msg += f"Succesfully sent intervention to learners from {course_id} :\n{str(sent_list)} \n"
            if len(err_list) > 0:
                msg += f"Unable to send intervention to learners from {course_id} :\n{str(err_list)} \n"
            if (len(msg) > 0) and (adm_chatid > 0):
                try:
                    await bot_intance.bot.sendMessage(adm_chatid, msg)
                except:
                    syslog(msg)
                    pass
            syslog(msg)
        await asyncio.sleep(1)
    del mdf
    syslog("auto_intervent completed")
    if adm_chatid > 0:
        txt = "auto_intervent completed"
        await bot_intance.bot.sendMessage(adm_chatid, txt)
    return

#------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    version = sys.version_info
    if version.major == 3 and version.minor >= 6:
        #do_main()
        with open("vmbot.json") as json_file:
            bot_info = json.load(json_file)
        client_name = bot_info['client_name']
        vmedxlib.edx_api_url = "https://omnimentor.sambaash.com/edx/v1"
        vmedxlib.edx_api_header = {'Authorization': 'Basic ZWR4YXBpOlVzM3VhRUxJVXZENUU4azNXdG9E', 'Content-Type': 'text/plain'}
        vmsvclib.rds_connstr = bot_info['omdb']
        vmsvclib.rdsdb = None
        vmsvclib.rdscon = None
        vmsvclib.rds_pool = 0
        vmsvclib.rds_schema = bot_info['schema']
        vmedxlib.max_iu = 20
        vmsvclib.rdscon = vmsvclib.rds_connector()
        course_id = ""
        if len(sys.argv)>=2:
            course_id = str(sys.argv[1])
        if course_id != "":
            qry = f"select * from stages where client_name = '{client_name}' and courseid = '{course_id}';"
            df = rds_df(qry)
            if df is not None:
                df.columns = get_columns("stages")
                msg = stage_calendar(df)
                print(msg)
    else:
        syslog("Unable to use this version of python\n", version)
