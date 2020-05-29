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
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore")

import pandas as pd
import pandas.io.formats.style
import os, re, sys, time, datetime, string, random 
import subprocess 
import matplotlib.pyplot as plt
import matplotlib.ticker as mtk

import telepot
from telepot.loop import MessageLoop
from telepot.delegate import pave_event_space, per_chat_id, create_open, per_callback_query_chat_id
from telepot.namedtuple import ReplyKeyboardMarkup
from telepot.helper import IdleEventCoordinator

import sqlite3
import wget
import json

import vmedxlib
import vmnlplib
import vmaiglib
import vmmcqdlib
import vmsvclib
from vmsvclib import *

global vmbot, connect_string, match_score, max_duration, ft_model, dt_model, resp_dict, use_regexpr, mcq_analysis 

# following will be replaced by generic config without hardcoded.
adminchatid = 1064466049
#adminchatid = 71354936
max_rows = 20
sysconfig = "sysconf.db"
nlpconfig = "nlp-conf.db"
syslogdb  = "syslog.db"
pbconfig = "pbconfig.db"
btn_hellobot = "Hello OmniMentor 👩‍🎓 👨‍🎓🤖"
option_back = "◀️"
option_import = "Import 📦"
option_export = "Export 💾"
option_mainmenu = "mainmenu"
option_learners = "Learners 👩"
option_faculty = "Faculty"
option_admin = "Admin"
option_2fa = "2FA"
option_demo = "Demo"
mainmenu = [[option_learners, option_faculty , option_admin, option_back]]
option_mycourse = "My course"
option_myprogress = "My progress"
option_faq = "FAQ"
option_mychat = "LiveChat"
option_mychart = "Chart"
option_binduser = "Auto Sign-in"
option_bind = "Binding"
option_gethelp = "Contact me"
option_info = "Info"
lrn_start = "Learner Started"
lrn_student = "Learner Verified"
#learners_menu = []         # use this if demobot / testbot
learners_menu = [[option_mycourse, option_myprogress, option_mychart],\
    [option_gethelp, option_mychat, option_faq], [option_binduser, option_info, option_back]]
option_fct = "Faculty Admin 📚"
option_pb = "Playbooks 📗📘📙"
option_analysis = "Analysis 📊"
option_chat = "Chat 💬"
option_sos = "Help 🆘"
option_chart = "Chart 📊"
option_chatlist = "Chat List"
option_chatempty = "Chat Empty"
mentor_menu = [[option_fct, option_pb], [option_analysis, option_chat, option_back]]
fc_student = "Student Update"
fc_cohlist = "Cohort Listing"
fc_edx = "EdX Import"
fc_assignment = "Update Assignment"
fc_mcqtest = "Update MCQs"
fc_schedule = "Schedule Update"
faculty_menu = [[fc_student,fc_cohlist, fc_edx], [fc_assignment, fc_mcqtest, fc_schedule, option_back]]
fc_updstage = "Stage Update"
fc_resetstage = "Stage Reset"
fc_recupd = "Record Update"
fc_studentsub = "Student Submenu"
opt_stage = "Edit Stage Cohorts"
opt_recupd = "Record Update Cohorts"
opt_updstage = "Stage Update Cohorts"
opt_resetstage = "Reset Stage Cohorts"
faculty_submenu = [[fc_updstage, fc_resetstage, fc_recupd, option_back]]
pb_config = "Configurator Playbook 📗"
pb_userdata = "Persona Playbook 📙"
playbook_menu= [[pb_config, pb_userdata, option_back]]
pbk_config = "Configuration 💻"
pbk_updcfg = "Update Configurator 💻"
pbconfig_menu = [[pbk_config, pbk_updcfg],[option_import,option_export, option_back]]
ps_userdata = "Userdata"
ps_schedule = "Schedule 📅"
ps_stage = "Edit Stage"
course_menu = [[ps_userdata, ps_schedule, ps_stage ],[option_import, option_export, option_back]]
an_mcq = "MCQ Analysis"
an_chart = "Graph"
an_mcqd = "MCQ Diff. Analysis"
ml_grading = "AI Grading"
analysis_menu = [[ml_grading, an_mcq, an_mcqd, an_chart, option_back]]  
an_mcqavg = "By MCQ Average"
an_avgatt = "By MCQ Attempts"
an_avgscore = "By MCQ Scores"
opt_mcqd = "MCQ Diff Cohorts"
opt_pbusr = "Playbook Cohorts"
opt_mcqavg = "MCQ Avg Cohorts"
mcqdiff_menu = [[an_avgatt,an_avgscore,an_mcqavg,option_back]]
option_syscfg = "System 🖥️"
option_nlp = "NLP"
option_ml = "Machine Learning"
admin_menu = [[option_nlp, option_ml, option_syscfg, option_back]]
cfg_syslog = "System Log"
cfg_reload = "Load Config"
cfg_bind = "Binded Users"
cfg_map = "Mapping"
cfg_progress = "Responses"
option_cmd = "Cmd Mode"
option_edx = "SQL Mode"
option_py = "Script Mode"
#syscfg_menu = [[cfg_syslog, cfg_bind, cfg_progress, cfg_reload, cfg_map],[option_cmd, option_edx, option_py, option_import, option_export,option_back]]
syscfg_menu = [[cfg_syslog, cfg_bind, cfg_progress],[cfg_reload, cfg_map, option_edx],[option_cmd, option_py, option_back]]
nlp_prompts = "Bot Prompts"
nlp_dict = "Dictionary 📖"
nlp_corpus = "Corpus"
nlp_stopwords = "Stopwords"
nlp_faq = "FAQ List"
nlp_train = "Train NLP"
nlp_menu = [[nlp_dict, nlp_prompts, nlp_corpus], [nlp_train, nlp_stopwords, nlp_faq],[option_import, option_export,option_back]]
ml_data = "Model Data"
ml_pipeline = "ML Pipeline"
ml_report = "ML EDA"
ml_train = "Train Model" 
ml_graph = "ML Graph"
opt_aig = "AI Grad Cohorts"
ml_menu = [[ml_data,ml_pipeline,ml_report],[ml_graph,ml_train, option_back]]


gen2fa = lambda : (''.join(random.choice( "ABCDEFGHJKLMNPQRTUVWXY0123456789" ) for i in range(32))).upper()
string2date = lambda x,y : datetime.datetime.strptime(x,y).date()
piece = lambda txtstr,seperator,pos : txtstr.split(seperator)[pos]

def do_main():
    global vmbot, max_duration, dt_model
    loadconfig()
    gmt = sql2var(sysconfig,"select value from params where key='GMT';", 0)
    edx_time = sql2var(sysconfig,"select value from params where key='edx_import';", 0)
    if os.name == 'nt':
        Token = "812577272:AAEgRcGYOGzkN9AoJQKLusspiowlUuGrtj0"  # @OmniMentor
    else:
        Token = sql2var(sysconfig, "select value from params where key = 'BotToken';", '')
    edx_cnt = 0
    err = 0    
    vmbot = BotInstance(Token)
    botname = vmbot.bot_name
    vmbot.load_courselist()
    if dt_model.model_name=="":
        txt = "Ai grading model data file dt_model.bin is missing"
        syslog('system', txt)
        vmbot.bot.sendMessage(adminchatid, txt)
        vmbot.bot_running = False
    else:
        print("running " + botname)
        if os.name != 'nt':
            try:
                vmbot.bot.sendMessage(adminchatid, "Click /admin to access the admin mode\n/sys for system mode\n/demo for demo")
            except:
                pass
    vmbot.client_name = sql2var(sysconfig, "select value from params where key = 'client_name';", "Sambaash")        
    syslog('system',f"Bot {botname} started for client {vmbot.client_name} .")
    while vmbot.bot_running :
        try:
            checkjoblist(vmbot.bot)
            timenow = time_hhmm(gmt)
            if (edx_time > 0) and (timenow==edx_time) and (edx_cnt==0):
                edx_cnt = 1
                syslog('system', 'edx_mass_import started')
                vmedxlib.edx_mass_import()
                syslog('system', 'edx_mass_import ended')
            if (edx_time > 0) and (timenow > edx_time) and (edx_cnt==1):
                edx_cnt = 0
            time.sleep(3)
        except:
            print("Error running the bot,please check")
            vmbot.bot_running = False
            break

    msg = botname + ' shutdown.'
    syslog('system',msg)
    print(msg)

    try:
        os.kill(os.getpid(), 9)
    except:
        err = 1
        txt='Thank you for using OmniMentor bot. Goodbye!'
        print(txt)
    return (err==0)

def loadconfig():
    global match_score, max_duration, use_regexpr, resp_dict, ft_model, dt_model, mcq_analysis
    ok = True
    try:
        ft = sql2var(sysconfig, "select value from params where key = 'ft_model';", "ft_model.bin")
        ft_model = vmnlplib.NLP_Parser()
        ft_model.load_model(ft, nlpconfig)
        dt_model = vmaiglib.MLGrader()
        dt_model.load_model("dt_model.bin")
        if dt_model.model_name == "":
            ok = False
        mcq_analysis = vmmcqdlib.MCQ_Diff()
        max_duration = int(sql2var(sysconfig, "select value from params where key = 'max_duration';", 300))
        match_score = sql2var(sysconfig, "select value from params where key = 'match_score';", 0.95)        
        use_regexpr = int(sql2var(sysconfig, "select value from params where key = 'regexpr';", 0))
        resp_dict=load_respdict()
    except:  
        ok = False  
        pass
    return ok

def checkjoblist(bot):
    global vmbot
    result = ""
    if len(vmbot.job_list) > 0:
        job_item = vmbot.job_list[0]        
        result = runbotjob(bot, job_item)
        vmbot.job_list = vmbot.job_list[1:]
    return result

def binduser(tid, sid=0, cohort_id=""):
    try:
        telegram_users = load_data(sysconfig, 'telegram', 'telegram_id', False)
        if tid in telegram_users:
            query = """update telegram set chat_id = _y_ where telegram_id = _x_,courseid = '_z_';"""
        else:
            query = """insert into telegram(telegram_id,chat_id,courseid) values(_x_,_y_,'_z_')"""
        query = query.replace("_x_",str(tid))
        query = query.replace("_y_",str(sid)) # set sid to 0 if unbind
        query = query.replace("_z_",cohort_id)
        updatesql(sysconfig, query)
        query = """update telegram set chat_id = _y_ where courseid = '_x_';"""
    except:
        pass
    return

def load_respdict():
    keys = load_data(sysconfig, 'progress', 'key', False)
    resp = load_data(sysconfig, 'progress', 'response', False)
    mydict=dict(zip(keys,resp))
    return mydict

class BotInstance():
    def __init__(self, Token):
        self.Token = ""
        self.bot_name = ""
        self.bot_id = ""
        self.client_name = ""
        self.bot_running = False
        self.bot = None
        self.user_list = {}
        self.job_list = []
        self.chat_list = {}
        self.code2fa_list = {}
        self.list_courseid = []
        self.list_coursename = []
        self.list_datafile = []
        self.list_students = []
        self.vars = dict()
        self.cmd_dict = dict()
        self.keys_dict = dict()
        self.keys_dict[option_mainmenu] = 1
        # /home=learner+admin+sys
        self.define_keys( mainmenu, self.keys_dict[ option_mainmenu ])
        # /Hello OmniMentor
        self.define_keys( learners_menu, self.keys_dict[ option_learners ])
        #
        self.keys_dict[ lrn_start ] = (self.keys_dict[ option_learners ]*10) + 1
        self.keys_dict[ lrn_student ] = (self.keys_dict[ option_learners ]*10) + 2
        self.keys_dict[ option_bind ] = (self.keys_dict[ option_learners ]*10) + 3
        self.keys_dict[ option_demo ] = (self.keys_dict[ option_learners ]*10) + 4
        # /admin
        self.define_keys( mentor_menu, self.keys_dict[ option_faculty ])
        self.define_keys( faculty_menu, self.keys_dict[ option_fct ])
        self.define_keys( faculty_submenu , self.keys_dict[ fc_student ])
        self.define_keys( playbook_menu, self.keys_dict[ option_pb ])
        self.define_keys( pbconfig_menu, self.keys_dict[ pb_config ])
        self.define_keys( course_menu, self.keys_dict[ pb_userdata ])
        self.define_keys( analysis_menu, self.keys_dict[ option_analysis ])
        self.define_keys( mcqdiff_menu, self.keys_dict[ an_mcqd ])
        self.keys_dict[ option_chatlist ] = (self.keys_dict[ option_chat ]*10) + 1
        self.keys_dict[ option_chatempty ] = (self.keys_dict[ option_chat ]*10) + 2
        self.keys_dict[ fc_studentsub ] = (self.keys_dict[ fc_student ]*10) + 1
        self.keys_dict[ opt_pbusr ] = (self.keys_dict[ pb_userdata ]*10) + 1
        self.keys_dict[ opt_stage ] = (self.keys_dict[ ps_stage ]*10) + 1
        self.keys_dict[ opt_recupd ] = (self.keys_dict[ fc_recupd ]*10) + 1
        self.keys_dict[ opt_updstage ] = (self.keys_dict[ fc_updstage ]*10) + 1
        self.keys_dict[ opt_resetstage ] = (self.keys_dict[ fc_resetstage ]*10) + 1        
        self.keys_dict[ opt_mcqd ] = (self.keys_dict[ an_mcqd ]*10) + 1
        self.keys_dict[ opt_mcqavg ] = (self.keys_dict[ an_mcqavg ]*10) + 1
        self.keys_dict[ opt_aig ] = (self.keys_dict[ ml_grading ]*10) + 1
        
        # /sys
        self.define_keys( admin_menu, self.keys_dict[option_admin])
        self.define_keys( nlp_menu  , self.keys_dict[ option_nlp ])
        self.define_keys( ml_menu  , self.keys_dict[ option_ml ])
        self.define_keys( syscfg_menu  , self.keys_dict[ option_syscfg ])
        self.keys_dict[option_2fa] = (self.keys_dict[ option_admin ] *10) + 1

        # print(*(self.keys_dict).items(), sep = '\n')
        # print(*(self.cmd_dict).items(), sep = '\n')
        #
        try:
            self.bot = telepot.DelegatorBot(Token, [
                pave_event_space()( [per_chat_id(), per_callback_query_chat_id()],
                create_open, MessageCounter, timeout=max_duration, include_callback_query=True),
            ])
            info = self.bot.getMe()
            self.bot_name = info['username']
            self.bot_id = info.get('id')
            self.Token = Token
            print('Frontend bot : ', self.bot_id)
            msg = self.bot_name + ' started running. URL is https://t.me/' + self.bot_name
            print(msg)
            try:
                MessageLoop(self.bot).run_as_thread()
                self.bot_running = True
            except:
                print("Error running this bot instance")
            return
        except:
            pass
        return

    def __str__(self):
        return "Telegram chatbot service class"

    def __repr__(self):
        return 'BotInstance()'

    def broadcast(self, msg):
        for d in self.user_list:
            self.bot.sendMessage(d, msg)
        return

    def define_keys(self, telegram_menu, start_key):
        button_list = lambda x : str(x).replace('[','').replace(']','').replace(", ",",").replace("'","").split(',')
        menu_keys = start_key*100 + 1
        self.cmd_dict[start_key] = button_list(telegram_menu)
        for menu_item in self.cmd_dict[start_key] :
            if (menu_item != option_back) and (menu_item != ''):
                self.keys_dict[menu_item] = menu_keys 
                menu_keys += 1
        return 

    def load_courselist(self):
        df = querydf(pbconfig, "select * from playbooks;")
        self.list_courseid = [ x for x in df.course_id]
        self.list_coursename = [ "" for x in df.course_id]
        self.list_datafile = [ x for x in df.userdata]
        self.list_students = [ [] for x in df.course_id]
        for n in range(len(self.list_courseid)):
            fn = self.list_datafile[n] 
            course_name = sql2var(fn, "select value from params where key = 'course_name';", " ")
            if course_name.strip() != "":
                self.list_coursename[n] = course_name
            self.list_students[n] = load_data(fn, 'userdata', 'studentid', False)        
        return

    def find_course(self, stud):
        return [self.list_courseid[n] for n in range(len(self.list_courseid)) if stud in self.list_students[n]]

class MessageCounter(telepot.helper.ChatHandler):    
    def __init__(self, *args, **kwargs):
        super(MessageCounter, self).__init__(*args, **kwargs)
        self.new_session = True
        self.is_admin = False
        self.chatid = 0
        self.student_id = 0
        self.username = ""
        self.userdata = ''
        self.courseid = ''
        self.coursename = ''
        self.stage_name = ''
        self.stagedate = ''
        self.tablefields = ''
        self.edited = 0
        self.tableindex = 0
        self.list_courseids = []
        self.mcq_list = []
        self.as_list  = []        
        self.tablerows = []
        self.stage_list = []
        self.stage_days = []
        self.records = dict()
        self.menu_id = 0
        self.menu_home = []
        self.parentbot = None

    def reset(self):
        self.__init__()
        return

    def logoff(self, txt = "Have a great day!"):
        global vmbot        
        try:
            if self.chatid in [d for d in vmbot.user_list]:
                vmbot.user_list.pop(self.chatid)
        except:
            pass
        bot_prompt(self.bot, self.chatid, txt, [[btn_hellobot]])
        syslog('system',f"telegram user {self.chatid} logged out.")
        self.new_session = True
        self.chatid = 0
        self.student_id = 0
        self.reset  
        self.menu_id = 0
        return

    def on_close(self, exception):
        self.logoff()
        self.sender.sendMessage('session time out, goodbye.')
        return 

    def student_progress(self):
        self.tablerows = []
        prev_stage = self.records['stage']
        stage_list = load_data(self.userdata, 'stages', 'name', False)
        stage_days = load_data(self.userdata, 'stages', 'days', False)
        self.stage_list = stage_list
        self.stage_days = stage_days
        self.records['username'] = self.username
        txt = 'Select the day # to check your progress :'
        btn_list = build_menu(["Day #" + str( stage_days[n] ) for n in range(len(stage_list))], 4, option_back)
        bot_prompt(self.bot, self.chatid, txt, btn_list)
        for stg in stage_list:                    
            result = "stage:'_x_'".replace('_x_', stg)
            edit_fields(self.userdata, "userdata", "studentid", self.student_id, result)
            self.stage_name = stg
            (tt, self.mcq_list, self.as_list, self.records ) = verify_student(self.userdata, self.student_id)
            tt = display_progress(self.userdata, stg, self.records)
            self.tablerows.append(tt)
        self.stage_name = prev_stage
        self.records['stage'] = prev_stage
        result = "stage:'_x_'".replace('_x_', prev_stage)
        edit_fields(self.userdata, "userdata", "studentid", self.student_id, result)
        self.parentbot.keys_dict[option_myprogress]
        return txt

    def mcqas_chart(self, groupcht = False ):
        try:
            userdata  = load_data(self.userdata, 'userdata','studentid', True)
            if groupcht:
                cohort_id = piece(self.userdata, '.', 0)
                fn = 'chart_' + cohort_id + '.png'
                df1 = dict(userdata.mean(axis = 0, skipna = True))
                df2 = pd.DataFrame({
                    'Test/IU' : [ '#' + str(n) for n in range(1,14) ],    
                    'mcq test' : [ df1['mcq_avg' + str(n)]*100 for n in range(1,14) ],
                    'assignment test' : [ df1['as_avg' + str(n)]*100 for n in range(1,14) ]
                })
                title = f"MCQ and Assignment scores for cohort {cohort_id}"
            else:
                sid = self.student_id
                fn = 'chart_' + str(sid) + '.png'
                df1 = userdata[userdata.studentid==sid]                
                df2 = pd.DataFrame({
                    'Test/IU' : [ '#' + str(n) for n in range(1,14) ],    
                    'mcq test' : [ list(df1['mcq_avg' + str(n)])[0]*100 for n in range(1,14) ],
                    'assignment test' : [ list(df1['as_avg' + str(n)])[0]*100 for n in range(1,14) ]
                })
                title = f"MCQ and Assignment scores for student #{sid}"                
            
            df2.plot(kind='bar',figsize=(10,4), rot = 90)            
            plt.title(title)
            ax = plt.gca()
            cols = [c for c in  df2.columns]
            label_col = cols[0]
            xcol = cols[1]
            label_list = [ x for x in df2[label_col] ]
            width = 0.8
            plt.xlim([-width, len(df2[xcol])-width])
            ax.set_xticklabels((label_list))
            ax.yaxis.set_major_formatter(mtk.PercentFormatter())
            plt.draw()
            plt.savefig(fn, dpi=100)
            plt.clf()
            f = open(fn, 'rb')
            self.bot.sendPhoto(self.chatid, f)
            if groupcht:
                df3 = pd.DataFrame({
                    'Test/IU' : [ '#' + str(n) for n in range(1,14) ],    
                    'mcq test' : [ "{:.2%}".format(df1['mcq_avg' + str(n)]) for n in range(1,14) ],                    
                    'assignment test' : [ "{:.2%}".format(df1['as_avg' + str(n)]) for n in range(1,14) ]
                })
                if render_table(df3, header_columns=0, col_width=6, title_name=title) is not None:
                    plt.savefig(fn, dpi=100)
                    plt.clf()
                    f = open(fn, 'rb')
                    self.bot.sendPhoto(self.chatid, f)
            del df0, df1, df2, df3
        except:
            pass
        return 

    def session_info(self):
        global vmbot          
        txt = "Summary:\n"
        if self.is_admin :
            txt += '\nYou are the faculty adm\n\n'
            if len(vmbot.user_list)==0:
                txt += 'No students online\n'
            else:
                txt += 'List of students online:\n'
                txt += '\n'.join(['     '.join([str(d) for d in vmbot.user_list[r]]) for r in vmbot.user_list])
        else:
            if self.new_session:
                txt += '\nsession already logged out.'
            else:
                #tt = 'Who is online:\n'
                #tt += '\n'.join(['     '.join([str(d) for d in vmbot.user_list[r]]) for r in vmbot.user_list if vmbot.user_list[r][0]==self.courseid])
                #txt = tt + "\n" + txt
                txt += '\nStudent id is ' + str(self.records['studentid'])
                txt += '\nCohort ID : ' + self.courseid
                txt += '\nCourse Name : ' + self.coursename
                cc = [x for x in self.list_courseids if self.courseid != x]
                if len(cc)>0:
                    txt += "\nOther courses (cohort-id):\n"
                    for x in cc:
                        txt += "\t" + x + "\n"
                txt += '\nLearning Stage : ' + self.stage_name
                txt += '\nOutstanding Amount : ' + str(self.records['amt']) + "\n"
        txt += f"\nYour telegram username is {self.username}"
        txt += f"\nYour telegram chat_id is {self.chatid}"
        return txt

    def livechat(self, sid=0, telegram_id=0):
        global vmbot 
        list_learners = []                
        if len(vmbot.user_list)==0:
            txt = "There is no online learners at the moment"
            self.sender.sendMessage(txt)
        elif sid>0:
            tlist = [x for x in list(vmbot.user_list) if vmbot.user_list[x][1] == sid]
            if tlist == []:
                txt = f"student #{sid} is not online at the moment" 
                self.sender.sendMessage(txt)
            else:
                if self.chatid in vmbot.user_list:
                    user_from = vmbot.user_list[self.chatid][2]
                else:
                    user_from = self.username
                user_from += "(" 
                user_from += "faculty admin" if self.is_admin else str(self.student_id)
                user_from += ") "
                if telegram_id==0:
                    tid = tlist[0]
                else:
                    tid = telegram_id
                tname = vmbot.user_list[tid][2]
                txt = banner_msg("Live Chat","Hi " + tname + ", you are in the live chat with " + user_from)
                self.bot.sendMessage(tid,txt)
                vmbot.chat_list[tid] = self.chatid
                vmbot.chat_list[self.chatid] = tid
                txt = banner_msg("Live Chat","Hi, you are in the live chat with " + tname)
                bot_prompt(self.bot, self.chatid, txt, [['bye']])
                self.menu_id = vmbot.keys_dict[option_chat]                
        else:
            if self.is_admin:
                list_learners = [ ['     '.join([str(d) for d in vmbot.user_list[r]])] for r in vmbot.user_list ]            
            else:
                list_learners = [ ['     '.join([str(d) for d in vmbot.user_list[r]])] for r in vmbot.user_list \
                    if (vmbot.user_list[r][0] == self.courseid) and (vmbot.user_list[r][1] != self.student_id) ]
            if len(list_learners) > 0:
                txt = 'Chat with online learners 🗣'
                list_learners = list_learners + [ [option_back] ] 
                bot_prompt(self.bot, self.chatid, txt, list_learners)  
                self.menu_id = vmbot.keys_dict[option_chatlist]
            else:
                txt = "There is no online learners at the moment"
                self.sender.sendMessage(txt)
        return

    def endchat(self):
        global vmbot 
        chat_id = self.chatid
        chat_found = False
        if chat_id in vmbot.chat_list:
            chat_found = True
            tid = vmbot.chat_list[chat_id]
            txt = "Live chat session disconnected. 👋"
            self.bot.sendMessage(chat_id, txt)
            self.bot.sendMessage(tid, txt)
            try:
                vmbot.chat_list.pop(chat_id)
                if tid != chat_id:
                    vmbot.chat_list.pop(tid)
            except:
                pass
            for c in [chat_id , tid] :
                if c in list(vmbot.user_list) :
                    vmbot.user_list[ c ][4] = ""
        return chat_found

    def runfaq(self, resp):
        global match_score, use_regexpr, ft_model, vmbot
        accuracy = 0
        user_resp = resp.lower()
        txt = ""
        if use_regexpr == 1 :  # sysconf.db params table regexpr field
            txt = ft_model.match_resp(resp)
            syslog( "REG" , txt )

        if txt == '':
            (result, accuracy) = ft_model.get_response(resp)
            if accuracy >= match_score:
                txt = result
                syslog( "NLP" , txt )
        
        if txt == '':            
            ( resp, accuracy ) = ft_model.find_matching( resp )
            if accuracy > 0:
                if accuracy < match_score:
                    txt = 'do you mean this ? =>\n' + resp                    
                    user_resp = resp.lower()
                    self.sender.sendMessage(txt)
                txt = ft_model.match_resp(user_resp)
                syslog( "REG" , txt )

        ## customized fullfillment with {variable} inside the response        
        if (txt != '') and re.search('.*\{.*', txt):
            stagedate = str(self.stagedate)
            aslist = str(self.as_list)
            mcqlist = str(self.mcq_list)
            amt = str( self.records['amt'] )
            mcqas_chart = ""
            mcqdate = self.records['mcqdate']
            asdate = self.records['asdate']
            eldate = self.records['eldate']
            fcdate = self.records['fcdate']
            if '{lf}' in txt:
                txt = txt.replace('{lf}' , '\n')
            if '{sos}' in txt:
                vmbot.user_list[ self.chatid ][4] = "👋"
                txt = txt.replace('{sos}' , '')
            if '{mcqas_chart}' in txt:
                self.mcqas_chart()
                txt = txt.replace('{mcqas_chart}' , '')
            if '{mcq_att_balance}' in txt:
                result = self.track_attempts()
                txt = txt.replace('{mcq_att_balance}' , result)
            if '{student_progress}' in txt:
                self.student_progress()
                txt = txt.replace('{student_progress}' , '')
                return ""
            try:
                txt = eval("f'@'".replace('@', txt.replace('\n','~~~'))).replace('~~~','\n')
            except:
                # formatted string does not work due to old python versions 2.7
                tt = txt.replace('\n','~~~')
                arr_parts = [t.split('{')[1] for t in tt.split('}') if '{' in t]
                for w in arr_parts:
                    p = '{' + w + '}'
                    v = eval(w)                    
                    tt = tt.replace( p, str(v) )
                txt = tt.replace('~~~','\n')
                
        if txt == '':
            txt = "I'm sorry, I do not understand you."

        recommendation = ft_model.recommend_list(resp)
        if len(recommendation) > 0:
            if txt != '':
                self.sender.sendMessage(txt)
                rec_menu = build_menu(recommendation,1,option_back,[])
                bot_prompt(self.bot, self.chatid, "You might want to ask :", rec_menu)
                txt = ""
                self.menu_id = vmbot.keys_dict[option_faq]
        return txt

    def load_courseinfo(self, resp):
        global vmbot
        resptxt = resp.lower()
        menu_list = [ x.lower() for x in vmbot.list_courseid ]
        n = -1
        if resptxt in menu_list:
            n = menu_list.index(resptxt)            
            self.courseid = vmbot.list_courseid[n]
            self.userdata = vmbot.list_datafile[n]
            self.coursename = vmbot.list_coursename[n]
            ok = 1
        else:
            self.courseid = ""
            self.userdata = ""
            self.coursename = ""
            ok = 0
        return ok

    def check_student(self, sid, chat_id):
        self.student_id = 0
        txt = ''
        if self.new_session == False :
            return 
        (txt, self.mcq_list, self.as_list, self.records ) = verify_student(self.userdata, sid)
        err = 0        
        try:
            self.stage_name = self.records['stage']
            self.stagedate = self.find_stage_date("stagedate")
            self.records['asdate'] = self.find_stage_date("asdate")
            self.records['eldate'] = self.find_stage_date("eldate")
            self.records['fcdate'] = self.find_stage_date("fcdate")
            self.records['mcqdate'] = self.find_stage_date("mcqdate")
        except:
            self.records=={}
        if (self.records=={}) or (self.stage_name==""):
            txt = "Hi, there is incomplete information at the moment, the session is not ready yet.\n\n"
            txt += "Please select the course from below list."
            self.student_id = sid
            stud_courselist = vmbot.find_course(sid)
            self.list_courseids = stud_courselist
            btn_course_list = build_menu(stud_courselist,1, option_back)
            bot_prompt(self.bot, self.chatid, txt, btn_course_list)
            self.menu_id = self.parentbot.keys_dict[option_learners]
            return
        else:
            self.is_admin = False
            self.student_id = sid
            self.new_session = False
            vmbot.user_list[chat_id]=[self.courseid, self.student_id, self.username, chat_id, ""]
            txt = display_progress(self.userdata, self.stage_name, self.records)
            if txt == "":
                txt = "Welcome back."
            bot_prompt(self.bot, self.chatid, txt, self.menu_home)
            self.menu_id = self.parentbot.keys_dict[lrn_student]
        return 

    def find_stage_date(self, date_type):
        stage_table = load_data(self.userdata, 'stages', 'name', True)
        if stage_table is None:
            return ''
        row_match = ( stage_table['name'] == self.stage_name ) 
        if len(stage_table[row_match])==0:
            return ''
        df=stage_table[row_match]        
        result = list(df[date_type])[0]
        n = self.records['stage_names'].index(self.stage_name)
        #stg_datestr = self.records['stage_dates'][n+1]
        stg_datestr = self.records['stage_dates'][n]
        if result is None:
            datestring = stg_datestr
        else:
            try:
                datestring = result.strftime('%d/%m/%Y') #'%Y-%m-%d' 
            except:
                datestring = list(str(list(df[date_type])[0]).split(' '))[0]
        soc_datestr = [x for x in stage_table.stagedate][0]
        soc_date = string2date([x for x in stage_table.stagedate][0],"%d/%m/%Y")
        if datestring.strip() == "":
            datestring = stg_datestr
        else:
            stg_date = string2date(datestring,"%d/%m/%Y")
            diff_days = (stg_date - soc_date).days
            if diff_days <= 0:
                datestring = stg_datestr
        return datestring    

    def getdoc(self, bot, msg):
        fname = msg['document']['file_name']
        fid = msg['document']['file_id']
        fpath = bot.getFile(fid)['file_path']
        fn = "https://api.telegram.org/file/bot" + bot._token + "/"  + fpath
        return (fname, fn)

    def track_attempts(self):        
        vars = load_vars(self.userdata, self.student_id)
        (t0, t1, vars) = load_progress(self.userdata, vars['stage'], vars)
        if vars['mcq_att_balance'] == "":
            txt = "There is no outstand MCQs for futher attempts."
        else:
            txt = vars['mcq_att_balance']
        return txt

    def update_stage(self, sid):
        if sid <= 0:
            return ""
        df = querydf(self.userdata,'select * from userdata where studentid=_x_;'.replace('_x_',str(sid)))
        vars = dict()
        vars = df.iloc[0].to_dict()
        df = querydf(self.userdata,'select * from stages;')
        tblsize = len(df)
        if tblsize==0:
            return ""
        stage_dates = [x for x in df.stagedate]
        stage_names = [x for x in df.name]
        current_stage = ""
        userdata = None
        mcq_str = sql2var(self.userdata, "select value from params where key = 'mcq';", "")
        as_str = sql2var(self.userdata, "select value from params where key = 'assignment';", "")
        vars['mcq_due_dates'] = {} if mcq_str==' ' else eval(mcq_str)    
        vars['as_due_dates'] = {} if as_str==' ' else eval(as_str)
        try:
            pass_stage = 0
            txt = ''
            for n in range(tblsize):
                try:
                    current_stage = stage_names[n]
                    vars['stage'] = current_stage
                    (t1, t2, vars) = load_progress(self.userdata, current_stage, vars)
                    txt = t1 + t2
                    if vars['pass_stage'] == 0:
                        pass_stage = 1
                        break
                except:
                    pass
        except:
            current_stage = ""
        if current_stage == "":
            return ""
        else:
            updqry = """update userdata set stage = '_x_' WHERE studentid==_y_;"""
            updqry = updqry.replace("_x_", current_stage)
            updqry = updqry.replace("_y_", str(sid))
            updatesql(self.userdata, updqry)            
        return current_stage

    def grad_prediction(self):
        if dt_model.model_name == "":
            return []
        txt = ""        
        df = load_data(self.userdata, 'userdata', 'studentid', True)
        list_sid = [str(x) for x in df.studentid]
        if list_sid == []:
            return []
        progress_df = dict()
        progress_tt = dict()
        tbl = []
        try:
            for n in range(len(list_sid)):
                vv = list_sid[n]
                sid = int(vv)
                vars = load_vars(self.userdata, sid)
                uu = vars['username']
                fw = lambda u,v : f"\nTest Results for Student #{v} {u}\n    MCQ Tests\t\tAssignment Tests\n"
                fx = lambda n : vars["mcq_avg"+str(n)]
                fy = lambda n : vars["as_avg"+str(n)]
                gx = lambda n : vars["mcq_attempts"+str(n)]
                gy = lambda n : vars["as_attempts"+str(n)]
                gz = lambda n : gx(n) + gy(n) if gx(n) is not None and  gy(n) is not None else 0
                fz = lambda n : [ "#" + str(n) , "{:.2%}".format(fx(n)), str(gx(n)), "{:.2%}".format(fy(n)) , str(gy(n)) ]
                mscores = [fx(n) for n in range(1,14) if gx(n) > 0]
                ascores = [fy(n) for n in range(1,14) if gy(n) > 0]
                mcnt = len(mscores)
                acnt = len(ascores)
                mavg = 0 if mcnt == 0 else sum(mscores) / mcnt
                aavg = 0 if acnt == 0 else sum(ascores) / acnt                    
                grad_pred = dt_model.predict(mavg , aavg, 13)
                tbl.append( [vv , uu , "{:.2%}".format(grad_pred[0])] )
                progress_list = [ fz(n) for n in range(1,14) if gz(n) > 0]
                progress_list.append(['Avg', "{:.2%}".format(mavg) , " ",  "{:.2%}".format(aavg) , " "])
                df =  pd.DataFrame( progress_list )
                df.columns = ['Test #', 'MCQ', '#Attempts', 'Assignment', '# Attempts']                
                progress_df[sid] = df
                progress_tt[sid] = f"\nTest Results for Student #{vv} {uu}"
            self.records['progress_df'] = progress_df
            self.records['progress_tt'] = progress_tt
            df1 =  pd.DataFrame( tbl )
            df1.columns = ['Student ID#','Name', 'Prediction']
            tt = "AI Grading for " + self.courseid
            df1= df1.sort_values(by ='Prediction') 
            if render_table(df1, header_columns=0, col_width=3, title_name=tt) is not None:
                fn='gradpred_' + str(self.chatid) + '.png'
                plt.savefig(fn, dpi=100)
                plt.clf()
                f = open(fn, 'rb')
                self.bot.sendPhoto(self.chatid, f)
        except:
            pass
        return list_sid

    def on_chat_message(self, msg):
        global pbconfig, ft_model, dt_model, mcq_analysis, vmbot
        try:
            content_type, chat_type, chat_id = telepot.glance(msg)
            bot = self.bot
            self.chatid = chat_id
            keys_dict = vmbot.keys_dict
        except:
            return

        resptxt = ""
        resp = ""
        txt = ""
        retmsg = ""
        if content_type == 'text':
            resp = msg['text'].strip()
            resptxt = resp.lower()
            if 'from' in list(msg):
                username = msg['from']['first_name']                
                self.records['username'] = self.username = username
            if 'reply_to_message' in list(msg) and 'For your approval with 2FA code :' in str(msg):
                err=0
                try:
                    msglist = str(msg).replace('"',"'").replace("'message_id':",chr(4644)).split(chr(4644))[3].split(',')
                    reply_id = int([x for x in msglist if 'from' in x ][0].split(':')[2].strip())
                    req_user = [x for x in msglist if 'first_name' in x ][0].replace("'",'').split(':')[1].strip()
                    code2fa = [x for x in msglist if 'bot_command' in x ][0].split(' ')[-1].replace("'",'')
                    txt = "Hi " + req_user + ", your 2FA code is : " + code2fa
                    bot.sendMessage(reply_id,txt)
                except:
                    pass
        elif content_type=="document":
            (fname, fn) = self.getdoc(bot, msg)
            syslog( str(chat_id) , fname )
            retmsg = "Reading file " + fname + " ....\n"
            retmsg += "file url is " + fn + "\n\n"
            if self.menu_id == keys_dict[opt_pbusr]:
                if (fn.lower())[-4:]==".csv":  
                    statusok = map_and_update(self.userdata, fn)
                elif (fn.lower())[-4:]==".xls":  
                    statusok = xls2sqldb(fn, self.userdata)
                elif (fn.lower())[-5:]==".xlsx": 
                    statusok = xls2sqldb(fn, self.userdata)
                elif (fn.lower())[-3:]==".db":
                    fname = wget.download(fn)
                    for tbl in ["userdata", "stages", "params"] : 
                        statusok = copydbtbl(fname, self.userdata, tbl)
            elif self.menu_id == keys_dict[pb_config]:
                if (fn.lower())[-4:]==".csv":  # import only playbooks table
                    statusok=csv2sqldb(fn, pbconfig, "playbooks")
                elif (fn.lower())[-4:]==".xls":
                    statusok = xls2sqldb(fn, pbconfig)
                elif (fn.lower())[-5:]==".xlsx":
                    statusok = xls2sqldb(fn, pbconfig)
                elif (fn.lower())[-3:]==".db":
                    fname = wget.download(fn)
                    statusok = copydbtbl(fname, pbconfig, "playbooks")
                else:
                    statusok = False
            elif self.menu_id == keys_dict[option_syscfg]:
                if (fn.lower())[-4:]==".csv": 
                    statusok = import_keydict(fn, sysconfig)
                elif (fn.lower())[-4:]==".xls":
                    statusok = xls2sqldb(fn, sysconfig)
                elif (fn.lower())[-5:]==".xlsx":
                    statusok = xls2sqldb(fn, sysconfig)
                elif (fn.lower())[-3:]==".db":
                    fname = wget.download(fn)
                    statusok = copydbtbl(fname, sysconfig, "dictionary")
            elif self.menu_id == keys_dict[option_nlp]:
                if (fn.lower())[-4:]==".csv": 
                    statusok = import_keydict(fn, nlpconfig)
                elif (fn.lower())[-4:]==".xls":
                    statusok = xls2sqldb(fn, nlpconfig)
                elif (fn.lower())[-5:]==".xlsx":
                    statusok = xls2sqldb(fn, nlpconfig)
                elif (fn.lower())[-3:]==".db":
                    fname = wget.download(fn)
                    statusok = copydbtbl(fname, nlpconfig, "ft_corpus")
            else:                    
                txt = "Thanks for the document but I do not know what to do with it."
                self.sender.sendMessage(txt)
                return

            if statusok:
                txt += "file has been imported."
            else:
                txt += "Unable to import this file correctly. Please check the file."
            sent = bot.sendMessage(self.chatid, txt)                
            resp = ""
        elif content_type != "text":
            txt = "Thanks for the " + content_type + " but I do not need it for now."
            self.sender.sendMessage(txt)
            return
        else:
            syslog( content_type , str(msg) )

        # for demo/testbot only
        if resp in ["/bind","/unbind"]:
            resp = option_binduser
        elif resp == "/chat":
            resp = option_mychat
        elif resp == "/faq":
            resp = option_faq
        elif resp == "/plot":
            resp = option_mychart
        elif resp == "/progress":
            resp = option_myprogress

        syslog(str(self.chatid),f"response = {resp}")
        if resp=='/?':
            retmsg = self.session_info()
            retmsg += "\nmenu id = " + str(self.menu_id)

        elif resp=='/demo':
            sqldb = "demo.db"
            try:
                df = querydf(sqldb,'select distinct steps from user_stories order by id;')
                demo_list = [] if df is None else [x for x in df.steps]
                self.tablerows = demo_list
                demo_menu = build_menu(demo_list, 3, 'exit', ['/start','/end'])
                txt = 'Select from the following journey events:\nType exit to get out from demo mode'
                txt = bot_prompt(self.bot, self.chatid, txt, demo_menu )
                self.new_session = True
                self.chatid = 0
                self.student_id = 0
                self.reset  
                self.menu_id = keys_dict[option_demo]
            except:
                pass
    
        elif resp=='/end':
            self.endchat()
            self.is_admin = (chat_id == adminchatid)
            syslog("system","telegram user " + str(chat_id) + " offine.")
            self.logoff()
            
        elif resp=='/stop' and (chat_id==adminchatid):
            vmbot.broadcast('System shutting down.')
            vmbot.bot_running = False
            txt = 'System already shutdown.'
            self.sender.sendMessage(txt)
            syslog('system',txt)
                
        elif resp == '/start' or resp == '/hellobot' or resp == btn_hellobot:
            self.reset
            self.new_session = True
            self.is_admin = (chat_id == adminchatid)
            self.parentbot = vmbot
            self.menu_home = learners_menu
            if chat_id > 0 :
                self.endchat()
                syslog("system","telegram user " + str(chat_id) + " online.")
                if len(vmbot.list_courseid) == 0:
                    txt = 'There is no playbooks found at the moment. Please contact the admin.'
                    self.logoff(txt)
                else:
                    query = "select chat_id from telegram where telegram_id =" + str(chat_id)
                    result = sql2var(sysconfig, query , 0)
                    sid = int(result)
                    self.student_id = sid
                    if sid > 0:
                        query = "select courseid from telegram where telegram_id =" + str(chat_id)
                        cohort_id = sql2var(sysconfig, query , "")
                        if cohort_id=="":
                            stud_courselist = vmbot.find_course(sid)
                        else:
                            stud_courselist = [cohort_id]
                        self.list_courseids = stud_courselist
                        n = len(stud_courselist)
                        if n > 1:                            
                            btn_course_list = build_menu(stud_courselist,1, option_back)
                            txt = "Please select the course id from below:"
                            bot_prompt(self.bot, self.chatid, txt, btn_course_list)
                            self.menu_id = keys_dict[option_learners]
                        elif len(stud_courselist) == 1:
                            resp = stud_courselist[0]
                            msg = "You are in course:\n" + resp
                            self.courseid = resp
                            idx = vmbot.list_courseid.index(resp)
                            self.userdata = vmbot.list_datafile[idx]
                            bot.sendMessage(chat_id, msg)
                            self.check_student(self.student_id, self.chatid)
                    else:
                        self.list_courseids = []
                        txt = 'My name is OmniMentor, I am your friendly learning facilitator at ' + vmbot.client_name + '.\n'
                        txt += "\nplease enter your student id or email address :"                        
                        bot_prompt(self.bot, self.chatid, txt, [])
                        self.menu_id = keys_dict[option_learners]
                
        elif resp.startswith('/admin') or resp.startswith('/sys'):
            if chat_id in [adminchatid , 71354936, 56381493, 263090563]:
                self.is_admin = True
                self.menu_id = 1
                if resp.startswith('/admin'):
                    txt = banner_msg("Welcome","You are now connected to Mentor mode.")                    
                    self.menu_home = mentor_menu
                else:
                    txt = banner_msg("Welcome","You are now connected to Admin mode.")                    
                    self.menu_home = admin_menu
                bot_prompt(self.bot, self.chatid, txt, self.menu_home)
            else:
                self.is_admin = False
                self.edited = 1 if resp.startswith('/admin') else 2
                txt = "Following user requesting for admin access :\n"
                txt += f"Command of request : {resp} \n\n"
                txt += json.dumps(msg)
                vmbot.code2fa_list[chat_id] = gen2fa()
                txt += "\n\nFor your approval with 2FA code : " + vmbot.code2fa_list[chat_id]
                bot.sendMessage(adminchatid, txt)  
                retmsg = "Please enter the 2FA code :"
                self.menu_id = keys_dict[option_2fa]

        elif self.menu_id == keys_dict[option_mainmenu]:
            if resp == option_fct :
                txt = 'You are in faculty admin mode.'
                bot_prompt(self.bot, self.chatid, txt, faculty_menu)
                self.menu_id = keys_dict[option_fct]
            elif resp == option_pb :
                txt = 'You are in playbooks maintainence mode.'
                bot_prompt(self.bot, self.chatid, txt, playbook_menu)
                self.menu_id = keys_dict[option_pb]
            elif resp == option_syscfg :
                txt = 'You are in system config mode.'
                bot_prompt(self.bot, self.chatid, txt, syscfg_menu)
                self.menu_id = keys_dict[option_syscfg]
            elif resp == option_nlp :
                txt = 'You are in NLP options.'
                bot_prompt(self.bot, self.chatid, txt, nlp_menu)
                self.menu_id = keys_dict[option_nlp]
            elif resp == option_ml :                
                txt = 'You are in ML options.'
                bot_prompt(self.bot, self.chatid, txt, ml_menu)
                self.menu_id = keys_dict[option_ml]
            elif resp == option_chat :
                self.livechat()
            elif resp == option_analysis :
                txt = 'You are in Analysis options.'
                bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                self.menu_id = keys_dict[option_analysis]
            elif resp == option_back :
                self.endchat()
                syslog("system","telegram user " + str(chat_id) + " offine.")
                self.logoff()

        elif self.menu_id == keys_dict[option_learners] :
            if resp == option_back:
                self.logoff
                bot_prompt(self.bot, self.chatid, "End of session.", [[btn_hellobot]])
                return
            if '@' in resp :
                for sqldb in vmbot.list_datafile:
                    result = email_lookup(sqldb, resptxt)
                    if result != "":
                        resp = result
                        break
            if resp.isnumeric():
                sid = int(resp)
                if sid in vmbot.user_list:
                    txt = "Sorry you can't logon using another telegram account\nPlease try again later."
                    self.logoff(txt)
                    return
                stud_courselist = vmbot.find_course(sid)
                slen = len(stud_courselist)
                self.student_id = sid
                if (slen == 0) :
                    txt = "Sorry, we are not unable to find your record.\nPlease contact the course admin."
                    self.logoff(txt)
                    return                
                self.list_courseids = stud_courselist
                if slen == 1:
                    self.chatid = chat_id
                    self.courseid = stud_courselist[0]
                    n = vmbot.list_courseid.index( self.courseid )
                    self.coursename = vmbot.list_coursename[n]
                    self.userdata = vmbot.list_datafile[n]
                    self.check_student(self.student_id, chat_id)
                else:
                    btn_course_list = build_menu(stud_courselist, 1)
                    txt = "Please select the course id from below:"
                    bot_prompt(self.bot, self.chatid, txt, btn_course_list)
                    self.menu_id = keys_dict[option_learners]
            else:
                if self.load_courseinfo(resp) == 0:
                    txt = 'Your selection is not available !\n'
                    txt += 'Please select the course from below list'
                    stud_courselist = vmbot.find_course(self.student_id)
                    btn_course_list  = build_menu(stud_courselist, 1)
                    bot_prompt(self.bot, self.chatid,  txt, btn_course_list )
                else:                    
                    self.chatid = chat_id
                    if self.student_id == 0 or self.chatid == 0 :
                        txt = 'My name is OmniMentor, I am your friendly learning facilitator at ' + vmbot.client_name + '.\n'
                        txt += "\nplease enter your student id or email address :"
                        bot_prompt(self.bot, self.chatid, txt, [])
                        txt = ''
                        self.menu_id = keys_dict[lrn_start]                        
                    else:
                        sid = self.student_id
                        ch_id = self.chatid
                        self.check_student(sid, ch_id)

        elif self.menu_id == keys_dict[lrn_start] :
            userdata = load_data(self.userdata, 'userdata', 'email', True)
            if userdata is None:        
                retmsg = 'Unable to load course data : ' + self.userdata                    
            else:
                if self.new_session and '@' in resp :
                    resptxt = email_lookup(self.userdata, resptxt)
                    resp = resp if resptxt=="" else resptxt
                if resp.isnumeric() and self.new_session :
                    sid = int(resp)
                    self.check_student(sid, chat_id)
                    self.menu_id = keys_dict[lrn_student]
                else:
                    retmsg = "please enter your student id or email address :"

        elif self.menu_id == keys_dict[lrn_student] and resp in [option_mycourse, option_myprogress, \
            option_faq, option_mychat, option_mychart, option_binduser, option_gethelp, option_info, option_back]:
            if resp == option_back:
                self.logoff()            
            elif resp == option_mycourse:
                #btn_course_list = build_menu(self.list_courseids, 1, option_back)
                btn_course_list = build_menu(self.list_courseids, 1)
                txt = "Please select the course id from below:"
                bot_prompt(self.bot, self.chatid, txt, btn_course_list)
                self.menu_id = keys_dict[option_mycourse]
            elif resp == option_myprogress:
                self.student_progress()
                self.menu_id = keys_dict[option_myprogress]
            elif resp == option_faq:
                txt = 'These are the FAQs :'
                faq_menu = build_menu(ft_model.faq_list.copy(),1,option_back,[])
                bot_prompt(self.bot, self.chatid, txt, faq_menu)
                self.menu_id = keys_dict[option_faq]
            elif resp == option_gethelp:
                vmbot.user_list[ self.chatid ][4] = "👋"
                retmsg = "Please wait, our faculty admin will connect with you on a live chat"
            elif resp == option_mychat:
                self.livechat()
            elif resp == option_mychart:
                self.menu_id = keys_dict[lrn_student]
                self.mcqas_chart()
                txt = 'Do you have any more questions?'
                bot_prompt(self.bot, self.chatid, txt, self.menu_home)
            elif resp == option_binduser:
                txt += "\nDo you want me to activate auto-login without entering student id each time ?"
                opt_yes = "Yes, enable auto-login"
                opt_no = "No, I would like to login manually each time"
                yesno_menu = build_menu([opt_yes,opt_no],1,option_back)
                bot_prompt(self.bot, self.chatid, txt, yesno_menu)
                txt = ""
                self.menu_id = self.parentbot.keys_dict[option_bind]
            elif resp == option_info:
                retmsg = self.session_info()

        elif self.menu_id == keys_dict[option_mycourse] :
            if resp == option_back:
                txt = "You are back to this cohort : " + self.courseid
                bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                self.menu_id = keys_dict[lrn_student]
            elif resp in self.list_courseids:
                self.courseid = resp
                n = vmbot.list_courseid.index( self.courseid )
                self.coursename = vmbot.list_coursename[n]
                self.userdata = vmbot.list_datafile[n]
                txt = "You are back to this cohort : " + self.courseid
                bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                self.menu_id = keys_dict[lrn_student]
                self.new_session = True
                self.check_student(self.student_id, chat_id)                
            else:
                btn_course_list = build_menu(self.list_courseids, 1, option_back)
                txt = "Please select the course id from below:"
                bot_prompt(self.bot, self.chatid, txt, btn_course_list)
                self.menu_id = keys_dict[option_mycourse]
                self.new_session = True
            
        elif self.menu_id == keys_dict[option_myprogress] :
            stage_list = self.stage_list
            stage_days = self.stage_days
            if resp.startswith("Day #"):
                numstr = int(resp[5:])
                n = stage_days.index(numstr) 
                retmsg = self.tablerows[n]
                self.stage_name = stage_list[n]
            elif resp == option_back:
                txt = 'Do you have any more questions?'
                bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                self.menu_id = keys_dict[lrn_student]

        elif self.menu_id == keys_dict[option_demo] :
            if resp in ['0', 'exit']:
                txt = 'End of demo mode.'
                txt = bot_prompt(self.bot, self.chatid, txt)
                self.menu_id = keys_dict[lrn_student]
            elif resp in self.tablerows:
                sid = 4477
                demodb  = "demo.db"
                query = "select sql from user_stories where steps = '_x_' order by id;"
                query = query.replace('_x_', resp)
                df = querydf(demodb,query)
                sql_list = [x for x in df.sql]
                for updqry in sql_list:
                    updatesql(demodb, updqry)
                    if 'studentid' in updqry:
                        sidstr = updqry.replace("studentid","$").split('$')[1].replace('=','').replace(';','')
                        sid = int(sidstr)
                        try:
                            (txt, self.mcq_list, self.as_list, self.records ) = verify_student(demodb, sid)
                            txt = display_progress( demodb, self.records['stage'], self.records)
                            [ bot.sendMessage(x, txt) for x in list(vmbot.user_list) if vmbot.user_list[x][1] == sid ]
                        except:
                            txt=''
            else:
                retmsg = "Your are now in demo mode, there will be no response to above statement."

        elif self.menu_id == keys_dict[option_bind] :
            #if resptxt=="yes":
            if "yes," in resptxt:
                binduser(chat_id, self.student_id,self.courseid)
                #txt = f"telegram account {chat_id} and student id has been binded to {self.student_id}."
                txt = "Auto-Login option enabled"
            elif "no," in resptxt:
            #    binduser(chat_id, 0)
            #    txt = f"telegram account {chat_id} and student id has been unbinded to {self.student_id}."
            #elif resptxt=="skip":
                updqry = """delete from telegram where telegram_id = _x_;"""
                updqry = updqry.replace("_x_", str(self.chatid) )
                updatesql(sysconfig, updqry)
                #txt = f"telegram account {chat_id} removed from our records."
                txt = "Auto-Login option disabled"
            elif resp == option_back:
                txt = "you are back to main menu"
            bot_prompt(self.bot, self.chatid, txt, self.menu_home)
            syslog(str(self.chatid),txt)
            self.menu_id = keys_dict[lrn_student]

        elif self.menu_id == keys_dict[option_fct]:
            btn_course_list = build_menu(vmbot.list_courseid.copy(), 1, option_back, [])
            if resp == fc_student:
                if len(vmbot.list_datafile)==0:
                    retmsg = 'There is no data being loaded.'
                else:
                    self.menu_id = keys_dict[fc_student]
                    txt = "This determine the learning stage of a learner\n"
                    txt += "test data to be used:\n\n"
                    bot_prompt(self.bot, self.chatid, txt,btn_course_list)
            elif resp == fc_assignment :
                if vmbot.list_courseid == []:
                    retmsg = 'There is no data being loaded.'
                else:
                    self.menu_id = keys_dict[fc_assignment]
                    txt = "This option imports assignments scores from Edx datasource.\nEnter * for mass update"
                    bot_prompt(self.bot, self.chatid, txt,btn_course_list)
            elif resp == fc_mcqtest :
                if vmbot.list_courseid == []:
                    retmsg = 'There is no data being loaded.'
                else:
                    self.menu_id = keys_dict[fc_mcqtest]
                    txt = "This option imports mcq tests scores from Edx datasource.\nEnter * for mass update"
                    bot_prompt(self.bot, self.chatid, txt,btn_course_list)
            elif resp == fc_edx :
                if vmbot.list_courseid == []:
                    retmsg = 'There is no data being loaded.'
                else:
                    self.menu_id = keys_dict[fc_edx]
                    txt = "This option imports userdata from Edx datasource.\nEnter * if mass import"
                    bot_prompt(self.bot, self.chatid, txt,btn_course_list)
            elif resp == fc_cohlist :
                txt = "Search course-id by keywords:\n"
                txt += "Example :  FOS\n"
                txt += "Enter 0 to exit"
                bot_prompt(self.bot, self.chatid, txt, [])
                self.menu_id = keys_dict[fc_cohlist]
            elif resp == fc_schedule :
                if vmbot.list_courseid == []:
                    retmsg = 'There is no data being loaded.'
                else:
                    self.menu_id = keys_dict[fc_schedule]
                    txt = "This option updates the course schedule and due-dates.\nEnter * for mass update"
                    bot_prompt(self.bot, self.chatid, txt,btn_course_list)
            elif resp == option_back :
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                self.menu_id = 1

        elif self.menu_id == keys_dict[fc_student]:            
            if resp in vmbot.list_courseid:
                df = load_data(pbconfig, 'playbooks', 'course_id', True)            
                tbl = df[df.course_id == resp].iloc[0].to_dict()
                self.courseid = tbl['course_id']
                self.userdata = tbl['userdata']
                query = "select studentid,username,amt,grade,stage,f2f from userdata"
                cohort_id = piece(self.userdata, '.', 0)
                if list_table(self.userdata, query, "List of learners from " + cohort_id) is not None:
                    fn='userdata' + str(self.chatid) + '.png'
                    plt.savefig(fn, dpi=100)
                    plt.clf()
                    f = open(fn, 'rb')
                    self.bot.sendPhoto(self.chatid, f)
                txt = "Select the following:"
                bot_prompt(self.bot, self.chatid, txt, faculty_submenu)
                self.menu_id = keys_dict[fc_studentsub]
            else:
                newlist = [[x] for x in vmbot.list_courseid if resptxt in x.lower()]
                if len(newlist)>0:
                    txt = "select the course id below:"
                    btn_list = build_menu([[x] for x in vmbot.list_courseid if resptxt in x.lower()],1,option_back,[])
                    bot_prompt(self.bot, self.chatid, txt,btn_list)
                    return
                else:
                    txt = "You are back to the faculty menu."
                    bot_prompt(self.bot, self.chatid, txt, faculty_menu)
                    self.menu_id = keys_dict[option_fct]

        elif self.menu_id == keys_dict[fc_studentsub]:
            if resp == fc_updstage:                
                txt = "Enter a list of student id for the update\n"
                txt += "Example :  123,456\n"
                txt += "You can enter * for entire class.\n"
                txt += "Enter 0 to exit"
                bot_prompt(self.bot, self.chatid, txt, [])
                self.menu_id = keys_dict[fc_updstage]
            elif resp == fc_resetstage :
                txt = "Enter a list of student id for the update\n"
                txt += "Example :  123,456\n"
                txt += "You can enter * for entire class.\n"
                txt += "Enter 0 to exit"
                bot_prompt(self.bot, self.chatid, txt, [])
                self.menu_id = keys_dict[fc_resetstage]
            elif resp == fc_recupd  :
                txt = "Enter student id for update:\n"
                txt += "(Enter 0 to exit)"
                bot_prompt(self.bot, self.chatid, txt, [])
                self.menu_id = keys_dict[opt_recupd]
            elif resp == option_back :
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, faculty_menu)
                self.menu_id = keys_dict[option_fct]

        elif self.menu_id == keys_dict[fc_updstage] :            
            if resp == "0":
                txt = "there is nothing to update"
            else:
                students_list = load_data(self.userdata, 'userdata', 'studentid', False)
                if resp == "*":
                    selection = students_list
                else:
                    try:
                        selection = [ int(x.strip()) for x in resp.split(',') if len(x.strip()) >0 ]
                    except:
                        selection = []
                if selection==[]:
                    txt = "There is nothing to update."
                else:
                    for sid in [x for x in students_list if x in selection]:
                        self.update_stage(sid)
                    txt = "the stages for the following has been updated:\n" + self.courseid 
            bot_prompt(self.bot, self.chatid, txt, faculty_submenu)
            self.menu_id = keys_dict[fc_studentsub]

        elif self.menu_id == keys_dict[fc_resetstage]:            
            if resp == '0':
                txt = "there is nothing to update"
                bot_prompt(self.bot, self.chatid, txt, faculty_submenu)
                self.menu_id = keys_dict[fc_studentsub]
                self.tablerows = []
            elif resp == '*':
                self.tablerows = load_data(self.userdata, 'userdata', 'studentid', False)
            else:
                if "," in resp:
                    try:
                        self.tablerows = [ int(x) for x in resp.split(',')]
                    except:
                        self.tablerows = []
                else:
                    self.tablerows = [ int(resp) ]
            if self.tablerows == []:
                txt = "there is nothing to update"
                bot_prompt(self.bot, self.chatid, txt, faculty_submenu)
                self.menu_id = keys_dict[fc_studentsub]
            else:                
                stage_list = load_data(self.userdata, 'stages', 'name', False)
                txt = "Please select the stage:\nType exit to get out from the list"            
                btn_list = build_menu(stage_list, 4, 'exit', [])
                bot_prompt(self.bot, self.chatid, txt, btn_list)
                self.menu_id = keys_dict[opt_resetstage]

        elif self.menu_id == keys_dict[opt_resetstage] :
            if resp == 'exit':
                txt = "There is nothing to update"
            else:
                result = "stage:'_x_'"
                result = result.replace('_x_', resp)
                try:
                    if len(self.tablerows) > 0:
                        for sid in self.tablerows:
                            txt = edit_fields(self.userdata, "userdata", "studentid", sid, result, self.tablerows)
                            self.tablerows = []
                    else:
                        txt = 'nothing to update'
                except:
                    txt = "Unable to update the new stage : " + resp
            bot_prompt(self.bot, self.chatid, txt, faculty_submenu)
            self.menu_id = keys_dict[fc_studentsub]

        elif self.menu_id == keys_dict[fc_recupd] :
            if resp == '0':
                txt = "there is nothing to update"
                bot_prompt(self.bot, self.chatid, txt, faculty_submenu)
                self.menu_id = keys_dict[fc_studentsub]
            elif resp.isnumeric():
                stage_id = int(resp)
                txt = ""
                for fld in ["amt","mcq_avg","as_avg","f2f"]:
                    txt += edit_records(self.userdata, 'userdata', 'studentid', stage_id, fld)
                if txt == "" :
                    txt = "there is nothing to update"
                    bot_prompt(self.bot, self.chatid, txt, faculty_submenu)
                    self.menu_id = keys_dict[fc_studentsub]
                else:
                    bot_prompt(self.bot, self.chatid, txt, [])

                    self.menu_id = keys_dict[opt_recupd]
            else:
                txt = "there is nothing to update"
                bot_prompt(self.bot, self.chatid, txt, faculty_submenu)
                self.menu_id = keys_dict[fc_studentsub]

        elif self.menu_id == keys_dict[opt_recupd] :
            if ':' in resp :
                txt = edit_fields(self.userdata, "userdata", "studentid", self.student_id, resp)
            else:
                txt = "there is nothing to update"
            bot_prompt(self.bot, self.chatid, txt, faculty_submenu)
            self.menu_id = keys_dict[fc_studentsub]

        elif self.menu_id in [ keys_dict[x] for x in [fc_assignment,fc_mcqtest,fc_edx] ]:
            if resp in ['0', 'exit']:
                txt = "You are back to the faculty menu."
            elif resp == "*":
                botname = (self.bot.getMe())['username']
                idx = [ keys_dict[x] for x in [fc_assignment,fc_mcqtest,fc_edx] ].index(self.menu_id)
                func_req = ["mass_update_assignment", "mass_update_mcq", "edx_mass_import"][idx]
                txt = "@" + botname + "\n" + str(chat_id) + "\n12\n" + func_req + "\n\n"
                job_item = txt.split("\n")
                vmbot.job_list.append(job_item)
                txt = 'Sending request edx_bot'
            elif resp in vmbot.list_courseid:
                self.courseid = resp
                n = vmbot.list_courseid.index(resp)
                self.userdata = vmbot.list_datafile[n]
                idx = [ keys_dict[x] for x in [fc_assignment,fc_mcqtest,fc_edx] ].index(self.menu_id)
                func_req_list = ["assignment","mcq","update"]
                func_req = "Cohort_" + func_req_list[idx]
                botname = (self.bot.getMe())['username']
                txt = "@" + botname + "\n" + str(self.chatid) + "\n12\n" + func_req + "\n" + resp + "\n" + self.userdata
                job_item = txt.split("\n")
                vmbot.job_list.append(job_item)
                txt = 'Sending request edx_bot'            
            else:
                newlist = [[x] for x in vmbot.list_courseid if resptxt in x.lower()]
                if len(newlist)>0:
                    txt = "select the course id below:"
                    btn_list = build_menu([[x] for x in vmbot.list_courseid if resptxt in x.lower()],1,option_back,[])
                    bot_prompt(self.bot, self.chatid, txt,btn_list)
                    return
                else:
                    txt = "course id not found."
            bot_prompt(self.bot, self.chatid, txt, faculty_menu)
            self.edited = 0
            self.menu_id = keys_dict[option_fct]

        elif self.menu_id == keys_dict[fc_cohlist]:
            if os.name == "nt":
                resp = "0"
            if resp == "0":
                txt = "there is nothing to search"
                bot_prompt(self.bot, self.chatid, txt, faculty_menu)
                self.menu_id = keys_dict[option_fct]
            else:
                try:
                    if vmedxlib.edx_connect()==0:
                        course_list = []
                        txt = "there is nothing to show"
                    else:
                        course_list = vmedxlib.search_course_list(resp)
                        vmedxlib.edx_disconnect()
                        df = pd.DataFrame(course_list, columns=['course_id'])
                        fn = "course_list.html"
                        write2html(df, title='COURSE LIST', filename=fn)
                        bot.sendDocument(chat_id, document=open(fn, 'rb'))
                        txt = "You are now at faculty menu"
                    bot_prompt(self.bot, self.chatid, txt, faculty_menu)
                    self.menu_id = keys_dict[option_fct]
                except:
                    pass

        elif self.menu_id == keys_dict[fc_schedule]:
            if resp in ['0', 'exit']:
                txt = "You are back to the faculty menu."
            elif resp == "*":
                botname = (self.bot.getMe())['username']
                txt = "@" + botname + "\n" + str(chat_id) + "\n12\nmass_update_schedule\n\n"
                job_item = txt.split("\n")
                vmbot.job_list.append(job_item)
                txt = 'Sending request edx_bot'
            else:
                pbconfig = sql2var(sysconfig, "select value from params where key = 'pbconfig';", "pbconfig.db")
                self.userdata = sql2var(pbconfig, "select userdata from playbooks where course_id = '" + resp + "'", resp + ".db")
                course_id = sql2var(self.userdata, "select courseid from userdata limit 1;", resp)
                self.courseid = resp
                vmedxlib.update_schedule(self.userdata, course_id)
                txt = "course schedule and due dates has been updated."
            bot_prompt(self.bot, self.chatid, txt, faculty_menu)
            self.menu_id = keys_dict[option_fct]

        elif self.menu_id == keys_dict[option_pb]:
            if resp==pb_config:
                txt = "show the list of playbooks"
                bot_prompt(self.bot, self.chatid, txt, pbconfig_menu)
                self.menu_id = keys_dict[pb_config]
            elif resp == pb_userdata:
                txt = "Let's take a look on the persona playbooks."
                playbooklist_menu = [[x] for x in vmbot.list_courseid]
                playbooklist_menu.append([option_back])
                bot_prompt(self.bot, self.chatid, txt, playbooklist_menu)
                self.menu_id = keys_dict[pb_userdata]
            elif resp == option_back:
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                self.menu_id = 1
                
        elif self.menu_id == keys_dict[pb_config]:
            if resp == pbk_config :
                fn = "pbconfig.html"
                df = sql2var(pbconfig, "select * from playbooks;", "", True)
                write2html(df, title='PLAYBOOKS LIST', filename=fn)
                bot.sendDocument(chat_id, document=open(fn, 'rb'))
            elif resp == pbk_updcfg :
                mass_update_playbooklist()
                self.sender.sendMessage('Playbook configurator has been updated.')
            elif resp == option_import :
                txt = "Please upload the configuration (existing data will be overwritten):"
                bot_prompt(self.bot, self.chatid, txt, [[option_back]])
            elif resp == option_export :
                fn = "export_pbconfig.xlsx"
                sqldb2xls(fn, pbconfig, ["playbooks"], ["course_id"])
                retmsg = "configuration has been saved as : " + fn
                bot.sendDocument(chat_id=self.chatid, document=open(fn, 'rb'))
            elif resp == option_back:
                txt = 'You are in playbooks maintainence mode.'
                bot_prompt(self.bot, self.chatid, txt, playbook_menu)
                self.menu_id = keys_dict[option_pb]

        elif self.menu_id == keys_dict[pb_userdata]:
            if self.load_courseinfo(resp) == 1:                
                resp = self.courseid 
                query = "select studentid,username,amt,grade,stage,f2f from userdata"
                cohort_id = piece(self.userdata, '.', 0)
                if list_table(self.userdata, query, "List of learners from " + cohort_id) is not None:
                    fn='userdata' + str(self.chatid) + '.png'
                    plt.savefig(fn, dpi=100)
                    plt.clf()
                    f = open(fn, 'rb')
                    self.bot.sendPhoto(self.chatid, f)
                txt = "Which tables are you looking at for " + resp + " ?"
                bot_prompt(self.bot, self.chatid, txt, course_menu)                
                self.menu_id = keys_dict[opt_pbusr]
            elif resp == option_back:
                txt = 'You are in playbook maintainence mode.'
                bot_prompt(self.bot, self.chatid, txt, playbook_menu)
                self.menu_id = keys_dict[option_pb]

        elif self.menu_id == keys_dict[opt_pbusr]:
            if resp == ps_userdata :
                fn = "userdata.html"
                df = sql2var(self.userdata, "select * from userdata;", "", True)
                write2html(df, title='LEARNERS INFO', filename=fn)
                bot.sendDocument(chat_id, document=open(fn, 'rb'))
            elif resp == ps_schedule :
                fn = "schedule.html"
                df = sql2var(self.userdata, "select * from stages;", "", True)
                write2html(df, title='COURSE SCHEDULE', filename=fn)
                bot.sendDocument(chat_id, document=open(fn, 'rb'))
            elif resp == ps_stage  :
                txt = "Enter Stage-id for update:\n"
                txt += "(Enter 0 to exit)"
                bot_prompt(self.bot, self.chatid, txt, [])
                self.menu_id = keys_dict[ps_stage]
            elif resp == option_import :
                txt = "Please upload the userdata in the .csv format for importing:"
                bot_prompt(self.bot, self.chatid, txt, [[option_back]])
            elif resp == option_export :
                fn = "export_" + self.courseid + ".xlsx"
                sqldb2xls(fn, self.userdata, ["userdata","mcq_data","mcq_score","stages","params"], ["studentid","course_id","courseid","id","key"])
                retmsg = "Existing playbooks already exported, filename is : "+fn 
                bot.sendDocument(chat_id=self.chatid, document=open(fn, 'rb'))
            elif resp == option_back :
                txt = 'You are in playbooks maintainence mode.'
                bot_prompt(self.bot, self.chatid, txt, playbook_menu)
                self.menu_id = keys_dict[option_pb]

        elif self.menu_id == keys_dict[ps_stage]:
            if resp == '0':
                txt = "there is nothing to update"
                bot_prompt(self.bot, self.chatid, txt, course_menu)
                self.menu_id = keys_dict[opt_pbusr]
            elif resp.isnumeric():
                stage_id = int(resp)
                txt = edit_records(self.userdata, 'stages', 'id', stage_id, "f2f")
                txt += edit_records(self.userdata, 'stages', 'id', stage_id, "mcq")
                txt += edit_records(self.userdata, 'stages', 'id', stage_id, "assignment")
                txt += edit_records(self.userdata, 'stages', 'id', stage_id, "flipclass")
                txt += edit_records(self.userdata, 'stages', 'id', stage_id, "IU")
                if txt == "" :
                    txt = "there is nothing to update"
                    bot_prompt(self.bot, self.chatid, txt, course_menu)
                    self.menu_id = keys_dict[opt_pbusr]
                else:
                    bot_prompt(self.bot, self.chatid, txt, [])
                    self.menu_id = keys_dict[opt_stage]
            else:
                txt = "there is nothing to update"
                bot_prompt(self.bot, self.chatid, txt, course_menu)
                self.menu_id = keys_dict[opt_pbusr]

        elif self.menu_id == keys_dict[opt_stage] :
            txt = edit_fields(self.userdata, "stages", "stage", self.student_id, resp)
            bot_prompt(self.bot, self.chatid, txt, course_menu)
            self.menu_id = keys_dict[opt_pbusr]

        elif self.menu_id == keys_dict[option_syscfg]:
            if resp == cfg_syslog :
                fn = "syslog.html"
                df = sql2var(syslogdb, "select * from syslog;", "", True)
                write2html(df, title='SYSLOG', filename=fn)
                bot.sendDocument(chat_id, document=open(fn, 'rb'))
            elif resp == cfg_progress  :
                fn = "progress.html"
                df = sql2var(sysconfig, "select * from progress;", "", True)
                write2html(df, title='RESPONSE TEXT', filename=fn)
                bot.sendDocument(chat_id, document=open(fn, 'rb'))
            elif resp == cfg_bind  :
                fn = "bindusers.html"
                df = sql2var(sysconfig, "select * from telegram;", "", True)
                write2html(df, title='BINDED USERS', filename=fn)
                bot.sendDocument(chat_id, document=open(fn, 'rb'))
            elif resp == cfg_reload :
                loadconfig()
                vmbot.load_courselist()
                txt = "System configuration reloaded."
            elif resp == cfg_map :
                fn = "mapping.html"
                df = sql2var(sysconfig, "select * from mapping;", "", True)
                write2html(df, title='FIELDS MAPPING', filename=fn)
                bot.sendDocument(chat_id, document=open(fn, 'rb'))
            elif resp == option_import :
                txt = "Please upload the system configuration in the .csv format (are you sure?):"
                bot_prompt(self.bot, self.chatid, txt, [[option_back]])
            elif resp == option_export :
                fn = "export_sysconf.xlsx"
                sqldb2xls(fn, sysconfig, ["dictionary","faq","prompts","stopwords"], ["keywords","questions","questions","keywords"])
                retmsg = "System configuration has been saved as : " + fn                
                bot.sendDocument(chat_id=self.chatid, document=open(fn, 'rb'))
            elif resp == option_cmd :
                txt = "You are now connected to Cmd mode.\nType cmd to list out the commands."
                txt = banner_msg("Service Console", txt)
                bot_prompt(self.bot, self.chatid, txt, [[option_back]])
                self.menu_id = keys_dict[option_cmd]
            elif resp == option_py :
                txt = "You are now connected to Script mode.\nDo not use double quote \" for string quotation."
                txt = banner_msg("Python Shell", txt)
                bot_prompt(self.bot, self.chatid, txt, [[option_back]])
                self.menu_id = keys_dict[option_py]
            elif resp == option_edx :
                if os.name == "nt":
                    txt = "You are now connected to Sqlite database via SQL."
                else:
                    txt = "You are now connected to EdX database via SQL."
                txt = banner_msg("SQL Console for EdX", txt)
                bot_prompt(self.bot, self.chatid, txt, [[option_back]])
                self.menu_id = keys_dict[option_edx]
            elif resp == option_back:
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                self.menu_id = 1

        elif self.menu_id == keys_dict[option_cmd] :
            if resp == option_back:
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, syscfg_menu)
                self.menu_id = keys_dict[option_syscfg]
            else:
                retmsg = shellcmd(resp)

        elif self.menu_id == keys_dict[option_py] :
            if resp == option_back:
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, syscfg_menu)
                self.menu_id = keys_dict[option_syscfg]
            else:
                retmsg = pycmd(resp, vmbot)

        elif self.menu_id == keys_dict[option_edx] :
            if resp == option_back:
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, syscfg_menu)
                self.menu_id = keys_dict[option_syscfg]
            else:
                fn = "sql_output" + str(chat_id) + ".html"
                if edxsql(resp,fn)==0:
                    retmsg =  "Unable to execute the query."
                else:
                    bot.sendDocument(chat_id, document=open(fn, 'rb'))

        elif self.menu_id == keys_dict[option_nlp]:
            retmsg = "The section handle all the natural language processing matters."
            if resp == nlp_dict :
                fn = "dictionary.html"
                df = sql2var(nlpconfig, "select * from dictionary;", "", True)
                write2html(df, title='DICTIONARY TABLE', filename=fn)
                bot.sendDocument(chat_id, document=open(fn, 'rb'))
            elif resp == nlp_prompts :
                fn = "prompts.html"
                df = sql2var(nlpconfig, "select * from prompts;", "", True)
                ft_model.qn_resp
                df["resp"] = df["resp"].apply(lambda x: x.replace(chr(157), "_"))
                write2html(df, title='RESPONSES TABLE', filename=fn)
                bot.sendDocument(chat_id, document=open(fn, 'rb'))
            elif resp == nlp_corpus  :
                fn = "ft_corpus.html"
                df = sql2var(nlpconfig, "select * from ft_corpus;", "", True)
                write2html(df, title='FASTTEXT CORPUS', filename=fn)
                bot.sendDocument(chat_id, document=open(fn, 'rb'))
            elif resp == nlp_stopwords :
                fn = "stopwords.html"
                df = sql2var(nlpconfig, "select * from stopwords;", "", True)
                write2html(df, title='STOPWORDS TABLE', filename=fn)
                bot.sendDocument(chat_id, document=open(fn, 'rb'))
            elif resp == nlp_faq:
                fn = "faq.html"
                df = sql2var(nlpconfig, "select * from faq;", "", True)
                write2html(df, title='FAQ TABLE', filename=fn)
                bot.sendDocument(chat_id, document=open(fn, 'rb'))
            elif resp == nlp_train :
                if ft_model.train_model() :
                    retmsg = "NLP model using the corpus table has been trained with model file saved as ft_model.bin"
                else:
                    retmsg = "NLP model using the corpus table was not trained properly"
            elif resp == option_import :
                txt = "Please upload the NLP corpus data file:"
                bot_prompt(self.bot, self.chatid, txt, [[option_back]])
            elif resp == option_export :
                fn = "export_nlpconf.xlsx"
                sqldb2xls(fn, nlpconfig, ["ft_corpus"], ["label"])
                retmsg = "System configuration has been saved as : " + fn                
                bot.sendDocument(chat_id=self.chatid, document=open(fn, 'rb'))
            elif resp == option_back:
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                self.menu_id = 1

        elif self.menu_id == keys_dict[option_ml]:
            txt = "The section handle all the machine learning related processes."
            self.sender.sendMessage(txt)
            if resp == ml_data :
                fn = "mcqas_info.html"
                df = sql2var("mcqinfo.db", "select * from mcqas_info limit " + str(max_rows), "", True)
                cols = "studentid,grade,mcq_avgscore,mcq_avgattempts,mcq_maxattempts,mcq_cnt".split(',')
                for c in cols:
                    df[c] = df[c].apply(lambda x: str(x))
                write2html(df, title='ML Model Data', filename=fn)
                retmsg = "Due to data too large, only " + str(max_rows) + " records being displayed."
                bot.sendDocument(chat_id, document=open(fn, 'rb'))
            elif resp == ml_pipeline :
                botname = (self.bot.getMe())['username']
                txt = "@" + botname + "\n" + str(chat_id) + "\n16\ngenerate_mcq_as\nFOS\nmcqinfo.db"
                job_item = txt.split("\n")
                vmbot.job_list.append(job_item)
                retmsg = 'Sending request edx_bot'
            elif resp == ml_report :
                retmsg = "Generating profiler report for quick data analysis."
                fn="mcqas_info.html"
                if dt_model.profiler_report("mcqinfo.db", fn)==1:
                    bot.sendDocument(chat_id=self.chatid, document=open(fn, 'rb'))
            elif resp == ml_graph  :
                retmsg = "Generating decision tree graph to explain the model."
                fn = 'mcqas_info.jpg'
                if dt_model.tree_graph(fn)==1:
                    f = open(fn, 'rb')
                    bot.sendPhoto(self.chatid, f)
            elif resp == ml_train :                
                retmsg = dt_model.train_model("mcqinfo.db", "dt_model.bin")
            elif resp == option_back:
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                self.menu_id = 1

        elif self.menu_id == keys_dict[option_analysis]:
            if resp == option_back:
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                self.menu_id = 1
            elif resp in [ an_mcqd , an_chart , an_mcq , ml_grading]:
                idx = [ an_mcqd , an_chart , an_mcq , ml_grading ].index(resp)
                txt = "Let's take a look on the following courses."
                courseid_menu = [[x] for x in vmbot.list_courseid]
                courseid_menu.append([option_back])
                bot_prompt(self.bot, self.chatid, txt, courseid_menu)
                self.menu_id = keys_dict[[ an_mcqd , an_chart , an_mcq , ml_grading ][idx]]

        elif self.menu_id == keys_dict[an_mcqd]:
            if resp == option_back:
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                self.menu_id = keys_dict[option_analysis]
            elif self.load_courseinfo(resp) == 1:                
                self.menu_id = keys_dict[opt_mcqd]
                txt = "MCQ Difficulty Analysis by:"
                bot_prompt(self.bot, self.chatid, txt, mcqdiff_menu)
            else:  
                newlist = [[x] for x in vmbot.list_courseid if resptxt in x.lower()]
                if len(newlist)>0:
                    txt = "Please select from the list of course id below:"
                    btn_list = build_menu([[x] for x in vmbot.list_courseid if resptxt in x.lower()],1,option_back,[])
                    bot_prompt(self.bot, self.chatid, txt,btn_list)
                    return
                else:
                    txt = 'Please select the following mode:'
                    bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                    self.menu_id = keys_dict[option_analysis]

        elif self.menu_id == keys_dict[opt_mcqd]:
            if resp == option_back:
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                self.menu_id = keys_dict[option_analysis]
            elif resp == an_avgatt:
                mcq_analysis.load_mcqdata(self.userdata)                
                if mcq_analysis.top10attempts(self.userdata) is None:
                    retmsg = 'There is no data for this course.'                    
                else:                    
                    plt.draw()
                    fn = "attempts_" + str(chat_id) +".png"
                    plt.savefig(fn, dpi=100)
                    plt.clf()
                    f = open(fn, 'rb')
                    self.bot.sendPhoto(chat_id, f)
            elif resp == an_avgscore:
                mcq_analysis.load_mcqdata(self.userdata)
                if mcq_analysis.top10score(self.userdata) is None:
                    retmsg = 'There is no data for this course.'
                else:
                    plt.draw()
                    fn = "score_" + str(chat_id) +".png"
                    plt.savefig(fn, dpi=100)
                    plt.clf()
                    f = open(fn, 'rb')
                    self.bot.sendPhoto(chat_id, f)
            elif resp == an_mcqavg:
                mcq_list = load_data(self.userdata, 'mcq_data', 'mcq', False)                
                sorted_list = sorted(list(set([int(x) for x in mcq_list]))) 
                mcq_list = [str(x) for x in sorted_list ] + [option_back]
                m = int((len(mcq_list)+4)/5)
                mcq_menu = [  mcq_list[n*5:][:5] for n in range(m) ]
                txt = "Please select from the list of MCQs below:"
                bot_prompt(self.bot, self.chatid, txt, mcq_menu)
                self.menu_id = keys_dict[opt_mcqavg]

        elif self.menu_id == keys_dict[opt_mcqavg]:            
            if resp.isnumeric() :
                mcq_analysis.load_mcqdata(self.userdata)
                if mcq_analysis.mcq_summary(int(resp), self.userdata) is None:
                    retmsg = 'There is no data for this course.'
                else:
                    plt.draw()
                    fn = "attempts_" + str(chat_id) +".png"
                    plt.savefig(fn, dpi=100)
                    plt.clf()
                    f = open(fn, 'rb')
                    self.bot.sendPhoto(chat_id, f)
            elif resp == option_back:                    
                self.menu_id = keys_dict[opt_mcqd]
                txt = "MCQ Difficulty Analysis by:"
                bot_prompt(self.bot, self.chatid, txt, mcqdiff_menu)

        elif self.menu_id == keys_dict[an_chart]:
            if self.load_courseinfo(resp) == 1:
                self.mcqas_chart(True)
                #bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                self.menu_id = keys_dict[option_analysis]
            elif resp == option_back:
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                self.menu_id = keys_dict[option_analysis]
            else:  
                newlist = [[x] for x in vmbot.list_courseid if resptxt in x.lower()]
                if len(newlist)>0:
                    txt = "select the course id below:"
                    btn_list = build_menu([[x] for x in vmbot.list_courseid if resptxt in x.lower()],1,option_back,[])
                    bot_prompt(self.bot, self.chatid, txt,btn_list)
                    return
                else:
                    txt = 'Please select the following mode:'
                    bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                    self.menu_id = keys_dict[option_analysis]

        elif self.menu_id == keys_dict[an_mcq]:
            txt = ""
            if self.load_courseinfo(resp) == 1:                
                if vmedxlib.analyze_cohort(self.userdata, resp) is not None:
                    fn='analyze_cohort_' + str(self.chatid) + '.png'
                    plt.savefig(fn, dpi=100)
                    plt.clf()
                    f = open(fn, 'rb')
                    self.bot.sendPhoto(self.chatid, f)
            elif resp == option_back:
                txt = 'You are back to the analysis menu:'
                bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                self.menu_id = keys_dict[option_analysis]
            else:
                newlist = [[x] for x in vmbot.list_courseid if resptxt in x.lower()]
                if len(newlist)>0:
                    txt = "select the course id below:"
                    bot_prompt(self.bot, self.chatid, txt,[[x] for x in vmbot.list_courseid if resptxt in x.lower()])
                    return
            txt = 'You are back to the analysis menu:' if txt=="" else txt
            bot_prompt(self.bot, self.chatid, txt, analysis_menu)
            self.menu_id = keys_dict[option_analysis]

        elif self.menu_id == keys_dict[ml_grading]:
            txt = ""
            if self.load_courseinfo(resp) == 1:
                sid_list = self.grad_prediction()
                btn_list = build_menu( sid_list, 4, option_back, [])
                self.records['progress_sid'] = sid_list
                txt = "Select the student id to see the progress :"
                bot_prompt(self.bot, self.chatid, txt, btn_list)
                self.menu_id = keys_dict[opt_aig]
                return
            elif resp == option_back:
                txt = 'Select your option:'
                bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                self.menu_id = keys_dict[option_analysis]
                return
            else:
                newlist = [[x] for x in vmbot.list_courseid if resptxt in x.lower()]
                if len(newlist)>0:
                    txt = "select the course id below:"
                    btn_list = build_menu([[x] for x in vmbot.list_courseid if resptxt in x.lower()],1,option_back,[])
                    bot_prompt(self.bot, self.chatid, txt,btn_list)
                    return
            txt = txt = 'Select your option:' if txt=="" else txt
            bot_prompt(self.bot, self.chatid, txt, analysis_menu)
            self.menu_id = keys_dict[option_analysis]

        elif self.menu_id == keys_dict[opt_aig]:            
            if resp.isnumeric() :
                sid = int(resp)
                df = self.records['progress_df'][sid]
                tt = self.records['progress_tt'][sid]
                tbl=render_table(df, header_columns=0, col_width=3, title_name=tt)
                if tbl is not None:
                    fn='test_result_' + resp + '.png'
                    plt.savefig(fn, dpi=100)
                    plt.clf()
                    f = open(fn, 'rb')
                    self.bot.sendPhoto(self.chatid, f)
            elif resp == option_back:
                txt = 'Select your option:'
                bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                self.menu_id = keys_dict[option_analysis]
                return

        elif self.menu_id == keys_dict[option_chatlist]:
            if chat_id in vmbot.chat_list:
                tid = vmbot.chat_list[ chat_id ]
                bot.sendMessage(tid , resp)
                bot_prompt(self.bot, self.chatid, "(type bye when you want to end the conversation)", [['bye']])
                self.menu_id = self.parentbot.keys_dict[option_chat]
            else:
                rlist = resp.replace('     ','*').split('*')
                if len(rlist)>=2 :
                    sid = rlist[1]
                    tid = rlist[3]
                    if sid.isnumeric():                    
                        tid = int(tid)
                        if tid in vmbot.chat_list:
                            retmsg = "User " + sid + " is on another conversation."
                        else:
                            sid = int(sid)
                            self.livechat(sid,tid)
                elif resp == option_back:
                    if self.is_admin :
                        bot_prompt(self.bot, self.chatid, "You are back in the main menu", mentor_menu)
                        self.menu_id = 1
                    else:
                        bot_prompt(self.bot, self.chatid, 'bye', self.menu_home)
                        self.menu_id = keys_dict[lrn_student]

        elif self.menu_id == keys_dict[option_chat]:
            if self.chatid in [d for d in vmbot.chat_list]:
                tid = vmbot.chat_list[self.chatid]
                if resp.lower() == 'bye':
                    self.endchat()
                    if self.is_admin :
                        txt = "You are back in the main menu"
                        vmbot.user_list[ tid ][4] = ""         
                        self.menu_id = 1
                    else:
                        txt = "bye"                        
                        self.menu_id = keys_dict[lrn_student]
                    bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                else:
                    bot.sendMessage(tid, resp)
                    txt = ''
            else:
                if self.is_admin :
                    txt = "Welcome back to main menu"
                    bot_prompt(self.bot, self.chatid, txt, self.menu_home)                
                    self.menu_id = 1
                else:
                    bot_prompt(self.bot, self.chatid, 'Live chat has been ended.', self.menu_home)
                    self.menu_id = keys_dict[lrn_student]

        elif self.menu_id == keys_dict[option_2fa]:
            code2FA = vmbot.code2fa_list[chat_id]
            if code2FA == resp:
                self.is_admin = True
                if self.edited == 1:
                    txt = banner_msg("Welcome","You are now connected to Mentor mode.")
                    self.menu_home = mentor_menu
                else:
                    txt = banner_msg("Welcome","You are now connected to Admin mode.")
                    self.menu_home = admin_menu
                bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                self.menu_id = 1
                vmbot.code2fa_list.pop(chat_id)
            else:
                txt  = "Sorry the 2FA code is invalid, please try again."
                self.is_admin = False
                bot_prompt(self.bot, self.chatid, txt, [['/end']])
                self.edited = 0
                self.menu_id = keys_dict[option_learners]

        elif chat_id in vmbot.chat_list and (resp.strip() != "") :            
            if resp.lower() == 'bye':
                self.endchat()
            else:
                if self.menu_id != keys_dict[option_chat]:
                    bot_prompt(self.bot, self.chatid, "(type bye when you want to end the conversation)", [['bye']])
                    self.menu_id = self.parentbot.keys_dict[option_chat]
                tid = vmbot.chat_list[chat_id]
                bot.sendMessage(tid, resp)

        elif self.menu_id == keys_dict[option_faq] and resp in ['0', 'exit', option_back]:
            bot_prompt(self.bot, self.chatid, "FAQ option is closed.", self.menu_home)
            self.menu_id = keys_dict[lrn_student]

        elif (self.menu_id != keys_dict[option_chat]) and (self.chatid in [d for d in vmbot.chat_list]):
            tid = vmbot.chat_list[ chat_id ]
            bot.sendMessage(tid , resp)
            bot_prompt(self.bot, self.chatid, "you said : " + resp, [['bye']])
            self.menu_id = keys_dict[option_chat]

        elif self.menu_id > 0:
            syslog( str(self.student_id) , "Q:" + resp )
            txt = self.runfaq(resp)
            if (txt != ""):
                bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                self.menu_id = keys_dict[lrn_student]
                
        while retmsg != "":
            txt = retmsg[:4000]
            retmsg = retmsg[4000:]
            self.sender.sendMessage(txt)
        return

def syslog(msgtype, message):
    if message == "":
        return
    date_now = time.strftime('%Y%m%d', time.localtime() )
    time_now = time.strftime('%H%M%S', time.localtime() )
    try:
        conn = sqlite3.connect(syslogdb)
        cursor = conn.cursor()
        query = """insert into syslog(date,time,type,message) values(_d,"_t","_x","_y");"""
        query = query.replace('_d',date_now)
        query = query.replace('_t',str(time_now))
        query = query.replace('_x',msgtype)
        query = query.replace('_y',message)
        cursor.execute(query)
        cursor.close()
        conn.commit()
        conn.close()
    except:
        print("Error for ", msgtype, message)
    return

def import_keydict(fn, sqldb):
    try:
        conn = sqlite3.connect(sqldb)
        df = pd.read_csv(fn)        
        df.to_sql("dictionary", conn , index=False, if_exists="replace")
        conn.commit()
        conn.close()
        return True
    except:
        return False

def getmapping():
    mapping_table = dict()
    try:
        df = load_data(sysconfig, 'mapping', 'key', True)
        result = df.to_dict()    
        for x in result['key']:
            key = result['key'][x]
            fld = result['value'][x]
            mapping_table[key.lower()] = fld
    except:
        pass
    return mapping_table
   
def map_and_update(sqldb, csv_data):
    tbl = "userdata"
    try:
        conn = sqlite3.connect(sqldb)
        cursor = conn.cursor()
    except:
        return False
    df1 = load_data(sqldb, tbl, 'studentid', True)
    col_list=list(df1.columns)
    df1 = None
    try:
        mapping = getmapping()
        df2 = pd.read_csv(csv_data)
    except:
        cursor.close()
        return False
    csv_dict = df2.to_dict()
    csv_map = list(csv_dict)
    df2index = list(df2.index)
    sid = 0
    for idx2 in df2index:        
        sql = ""
        for fld in csv_map:
            fld2 = fld.lower()
            if re.search("^mcq.*:",fld2):
                fld2=(fld2.split(":"))[0] + ":"
            x = csv_dict[fld][idx2]
            if fld2 in list(mapping):
                y = mapping[fld2]
            elif fld2 in col_list:
                y = fld2
            else:
                y = ''
            if y != '':
                if y == "studentid":
                    sid = x
                else:
                    if type(x)==str:
                        sql = sql + "," + y + " = '" + x + "'"
                    else:
                        sql = sql + "," + y + " = " + str(x)
        sql = "update " + tbl + " set " + sql[1:] + " where studentid = " + str(sid)    
        try:
            cursor.execute(sql)
        except:
            sql = ''
        
    conn.commit()
    cursor.close()
    return True

def runbotjob(bot, svr_req):
    global connect_string
    connect_string = sql2var(sysconfig, "select value from params where key = 'edxapp';", "")
    n = len(svr_req)
    if n >= 6:        
        bot_req = svr_req[0]
        sysid = int( svr_req[1] )
        reply_menu_id = int( svr_req[2] )
        func_req = svr_req[3]
        func_param = svr_req[4]
        sqldb = svr_req[5]
        txt = "@edxbot\n" + func_req + "\n" + func_param + "\nTask " + sqldb
        syslog('system',txt)
        func_req_list = ["Cohort_assignment", "Cohort_mcq", "Cohort_update"]        
        if func_req == "generate_mcq_as":
            try:
                syslog('system', f"calling generate_mcq_as({func_param})" )
                vmedxlib.generate_mcq_as(func_param)
                txt += " completed successfully."
                syslog('system', 'job completed')
            except:
                txt += " failed."               
        elif func_req in ["edx_mass_import", "mass_update_assignment", "mass_update_mcq", "mass_update_schedule"]:
            try:
                syslog('system', "calling " + func_req )
                func_svc = "vmedxlib." + func_req + "()"
                status = eval(func_svc)
                txt += " completed successfully."
                syslog('system', 'job completed')
            except:
                txt += " failed."

        elif func_req in func_req_list :
            func_svc_list = ["update_assignment" , "update_mcq" , "edx_import"]
            idx = func_req_list.index(func_req)
            func_svc = func_svc_list[idx] 
            course_id = func_param              
            try:
                if vmedxlib.edx_connect() == 1:
                    syslog('system', "calling " + func_svc + " with course id = " + course_id)
                    func_svc = "vmedxlib." + func_svc + "(sqldb, course_id)"
                    status = eval(func_svc)
                    if status:
                        txt += " completed successfully."
                    else:
                        txt += " failed."
                    vmedxlib.edx_disconnect()
                    syslog('system', 'job completed')
                else:
                    txt += " failed."
            except:
                txt += " failed."
        else:
            return ""
        try:
            bot.sendMessage(sysid, txt)            
        except:
            print(f"Unable to reply message due to this bot was blocked by the user {sysid}")            
    return txt

def verify_student(sqldb, student_id):
    student_name = ''
    stage = ''
    stype = -1
    mcqlist = []
    aslist = []    
    sid = int(student_id)
    vars = dict()
    amt = 0
    userdata = load_data(sqldb, 'userdata', 'studentid', True)
    if userdata is None:        
        msg = 'Unable to load userdata : ' + sqldb
        return (msg, mcqlist, aslist, vars)
    learners = [int(x) for x in userdata.studentid]
    if sid not in learners:
        msg = 'Student id not found. Please try again'        
        return (msg, mcqlist, aslist, vars)

    student_match = ( userdata['studentid'] == sid ) 
    
    if len(userdata[student_match])==0:
        msg = 'Sorry, we do not have your records. Please come back again some time.'
        return (msg, mcqlist, aslist, vars)
    
    student_record = userdata[student_match]
    for fld in list(student_record):
        fldname = fld.lower()
        try:
            if fldname=='amt':
                vars[fldname] = float(list(student_record[fld])[0])+0
            else:
                vars[fldname] = list(student_record[fld])[0]
        except:
            vars[fldname] = 0
    amt = vars['amt']
    student_name = vars['username']
    stage = vars['stage']
    grade = vars['grade']    

    mcq_str = sql2var(sqldb, "select value from params where key = 'mcq';", " ")
    as_str = sql2var(sqldb, "select value from params where key = 'assignment';", " ")
    vars['mcq_due_dates'] = {} if mcq_str==' ' else eval(mcq_str)    
    vars['as_due_dates'] = {} if as_str==' ' else eval(as_str)

    bptype = stype = 0
    stage_table = load_data(sqldb, 'stages', 'name', True)
    if stage_table is None:        
        msg = 'Unable to load userdata : ' + sqldb
        return (msg, mcqlist, aslist, vars)

    stage_list = [x for x in stage_table.name]
    date_list = [x for x in stage_table.stagedate]    
    vars['stage_dates'] = date_list
    vars['stage_names'] = stage_list
    msg = '\n\nHi ' + student_name + " !\n"               
    if stage is None:
        stage = stage_list[0]
        vars['stage'] = stage
    msg += 'You are currently on stage ' + stage + "\n\n"
    return (msg, mcqlist, aslist, vars)

def get_stage_name(sqldb):
    today_date = time.strftime('%Y-%m-%d', time.localtime() )
    stage_dates = load_data(sqldb, 'stages', 'stagedate', False)
    stage_names = load_data(sqldb, 'stages', 'name', False)
    stage_descriptions = load_data(sqldb, 'stages', 'name', False)
    stage_daysnum = load_data(sqldb, 'stages', 'days', False)
    cnt = len( stage_names )
    if cnt==0:
        return ""
    stage_name = ""
    stage_desc = ""
    stage_days = ""
    if cnt > 0:
        k = 0
        txt = ''
        stage_found = 0
        for m in range(1,cnt):
            if stage_found==0:
                n = m - 1
                stage_name = stage_names[n]
                stage_date = stage_dates[n]
                stage_desc = stage_descriptions[n]
                stage_days = str(stage_daysnum[n])
                if stage_found==0 and today_date >= stage_date:
                    next_stage_date = stage_dates[m]
                    if next_stage_date >= today_date:
                        k = n
        if stage_found==0:
            if today_date > stage_dates[-1]:
                k = cnt - 1
            stage_name = stage_names[k]
            stage_desc = stage_descriptions[k]
            stage_days = str(stage_daysnum[k])
        result = stage_name + " ( " +stage_desc+" ) on days #" + stage_days
    return result

def evaluate_progress(vars,iu_list,duedate_list,passingrate,var_prefix,var_title):
    avg_prefix = var_prefix + "_avg"
    att_prefix = var_prefix + "_attempts"
    scoredate = ' '*10 ; score_zero = [] ; iu_score = [] ; iu_attempts = []
    score_avg = 0 ; tt = '' ; iu_cnt = 0 ; attempts_balance = ""
    if iu_list != '0' and len(duedate_list) > 0:
        iu_vars = [int(x) for x in iu_list.split(',')]
        scoredate = max([ duedate_list[x] for x in iu_vars])
        score_pass = [ x for x in iu_vars if vars[avg_prefix + str(x)]>=passingrate]
        score_failed = [ x for x in iu_vars if vars[avg_prefix + str(x)] < passingrate and vars[avg_prefix + str(x)] > 0]
        score_zero = [ x for x in iu_vars if vars[avg_prefix + str(x)] == 0]
        iu_score = [ vars[x] for x in [ avg_prefix + x for x in iu_list.split(',') ]]        
        score_avg = sum(iu_score)/len(iu_score) if iu_score != [] else 0
        iu_attempts = [ vars[x] for x in [ att_prefix + x for x in iu_list.split(',') ]]

        tt += "\n" + var_title + " average test score : " + "{:.2%}".format(score_avg)
        if len(score_pass) > 0:
            tt += "\n☑ " + var_title + " test passed : " + str(score_pass)
        if len(score_failed) > 0:                
            tt += "\n‼ " + var_title + " test failed : " + str(score_failed)
        if len(score_zero) > 0:                
            tt += "\n☐ " + var_title + " test pending : " + str(score_zero)
        iu_cnt = len(score_pass) + len(score_failed)
        m = 4 if var_prefix=="mcq" else 1
        attempts_balance = "".join([ ("\n" + var_title + ' #'+str(x) + " has " + str(m-vars[att_prefix + str(x)])+" attempts left"  ) for x in iu_vars \
            if vars[avg_prefix + str(x)] < passingrate ])
        tt += attempts_balance 
    return (tt, score_avg, scoredate, score_zero, iu_score , iu_attempts, iu_cnt, attempts_balance)

def load_progress(sqldb, stg, vars):    
    global resp_dict
    if len(stg)==0:
        return ("", "", vars)
    seperator = re.compile('[a-zA-Z0-9\ ]').sub('',stg)    
    if 'soc' in stg.lower() and seperator=='':
        stage = 'SOC'
    else:
        stg_list = [x.strip() for x in stg.split(seperator)]    
        stage = stg_list[1] if len(stg_list)>=2 else stg_list[0]
    query = "select _y_ from stages s where s.stage = '_x_' ;"    
    mcq_qry = query.replace("_x_", stage).replace("_y_", "s.mcq")
    assignment_qry = query.replace("_x_", stage).replace("_y_", "s.assignment")
    f2f_qry = query.replace("_x_", stage).replace("_y_", "s.f2f")
    mcqvars = sql2var(sqldb, mcq_qry, "0")
    asvars = sql2var(sqldb, assignment_qry, "0")
    f2f_qry = "select s.f2f from stages s where s.stage= '_x_' ;"
    f2f_qry = f2f_qry.replace("_x_", stage)
    f2fvars = sql2var(sqldb, f2f_qry, "0")
    desc_qry = query.replace("_x_", stage).replace("_y_", "s.desc")
    stage_desc = sql2var(sqldb, desc_qry, "")
    pass_rate = sql2var(sysconfig, "select value from params where key = 'pass_rate';", 0.7)
    stagebyschedule = get_stage_name(sqldb)
    #soc_date = sql2var(sqldb, "select s.stagedate from stages s where s.id=1;", " "*10)
    stg_date = sql2var(sqldb, "select s.stagedate from stages s where s.stage= '_x_' ;".replace("_x_", stage), " "*10)
    
    try:  # if undefined, then defined it
        resp_dict 
    except: 
        resp_dict = load_respdict()

    txt_hdr = resp_dict['stg0']
    if "eoc" not in stagebyschedule.lower():        
        txt_hdr += resp_dict['stg1']
    if '{stage_desc}' in txt_hdr:
        txt_hdr = txt_hdr.replace('{stage_desc}' , stage_desc)
    if '{username}' in txt_hdr:
        txt_hdr = txt_hdr.replace('{username}' , vars['username'])
    if '{stage}' in txt_hdr:
        txt_hdr = txt_hdr.replace('{stage}' , stg)
    if '{stagebyschedule}' in txt_hdr:
        txt_hdr = txt_hdr.replace('{stagebyschedule}' , stagebyschedule)
    if '{lf}' in txt_hdr:
        txt_hdr = txt_hdr.replace('{lf}' , '\n')

    mcqas_list = [] ; list_attempts = [] ; f2f_limit = int(f2fvars) ; txt = "" ; t1 = '' ; t2 = ''
    mcnt = 0 ; mcqdate = '' ; mcq_avg = 0 ; mcq_zero = [] ; mcq_att_balance = ""
    acnt = 0 ; asdate = '' ; as_avg = 0 ; as_zero = [] ; as_att_balance = ""
    mcq_duedate_list = vars['mcq_due_dates']    
    (t1, mcq_avg, mcqdate, mcq_zero, mcqas_list1 , mcq_attempts, mcnt, mcq_att_balance) = \
        evaluate_progress(vars, mcqvars, mcq_duedate_list, pass_rate, 'mcq','MCQ')
    txt += t1
    mcqas_list += mcqas_list1
    list_attempts += mcq_attempts
    as_duedate_list = vars['as_due_dates']    
    (t2, as_avg, asdate, as_zero, mcqas_list2 , as_attempts, acnt, as_att_balance) = \
        evaluate_progress(vars, asvars, as_duedate_list, pass_rate, 'as','Assignment')
    txt += t2
    mcqas_list += mcqas_list2
    list_attempts += as_attempts
    mm = len(mcqas_list)
    has_score = 1 if mm > 0 else 0

    txt += "\n\n"
    avg_score  = sum(mcqas_list)/len(mcqas_list) if mcqas_list != [] else 0
    mcqas_complete = 1 if len([n+1 for n in range(len(list_attempts)) if list_attempts[n]==0])==0 else 0
    max_attempts = 0 if len(list_attempts)==0 else max(list_attempts)
    if mcqdate.strip() == "":
        mcqdate = stg_date
    else:
        if string2date(stg_date,"%d/%m/%Y") >= string2date(mcqdate,"%d/%m/%Y") :
            mcqdate = stg_date
    if asdate.strip() == "":
        asdate = stg_date
    else:
        if string2date(stg_date,"%d/%m/%Y") >= string2date(asdate,"%d/%m/%Y") :
            asdate = stg_date
    eldate = mcqdate
    fcdate = max( [mcqdate, asdate] )
    f2f = vars['f2f']
    amt = vars['amt']

    pass_stage = 0
    if stage.lower()=="soc" and amt == 0:
        pass_stage = 1
    if mcqas_complete==1 and avg_score>= pass_rate :
        pass_stage = 1
    if f2f_limit >= 5 and f2f >= f2f_limit :
        pass_stage = 1
    if f2f_limit >= 6 and f2f >= f2f_limit :
        pass_stage = 1
    stage = stg
    for vv in ['stage', 'mcqdate', 'asdate', 'eldate', 'fcdate', 'avg_score', 'mcqas_complete', \
            'has_score', 'pass_stage', 'max_attempts', 'mcqas_list', 'mcq_zero', 'mcq_avg', \
            'mcnt', 'acnt', 'as_avg', 'as_zero', 'f2f_limit', 'pass_rate', 'stage_desc', 'mcq_attempts', \
                'mcq_att_balance', 'as_att_balance'] :
        vars[vv] = eval(vv)
    return (txt_hdr, txt, vars)

def display_progress(sqldb, stg, vars):
    global vmbot, dt_model, resp_dict
    (txt1, txt2, vars) = load_progress(sqldb, stg, vars)
    txt = txt1 + txt2
    if txt == "":
        return "Your information is incomplete, please do not proceed and inform you faculty admin."
    telegram_ids = [x for x in list(vmbot.user_list) if vmbot.user_list[x][1] == vars['studentid']]
    if len(telegram_ids)>0:
        chatid = telegram_ids[0]
    else:
        chatid = 0

    try:
        if vars['has_score'] == 1:
            txt += resp_dict['avg_score']
    except:
        pass

    if stg.lower() == "soc days":
        if vars['amt'] == 0 :
            resp_amt0 = resp_dict['amt0']
            txt += resp_amt0 + "\n\n"
        elif vars['amt'] > 0:
            resp_amt1 = resp_dict['amt1']
            txt += resp_amt1 + "\n\n"
    else:
        if vars['amt'] > 0 :
            resp0 = resp_dict['resp0']
            txt += resp0 + "\n\n"

    if vars['mcqas_complete']==1 and vars['avg_score']>= vars['pass_rate'] :
        resp1 = resp_dict['resp1']
        txt += resp1 + "\n\n"
    if len(vars['mcq_zero']) > 0 and vars['avg_score'] == 0 :
        resp2 = resp_dict['resp2']
        txt += resp2 + "\n\n"
    if len(vars['mcqas_list'])>0 and vars['avg_score'] < vars['pass_rate'] and vars['max_attempts'] < 4 :
        resp3 = resp_dict['resp3']
        txt += resp3 + "\n\n"
    if len(vars['mcqas_list'])>0 and vars['avg_score'] > 0 and vars['avg_score'] < vars['pass_rate'] \
        and vars['max_attempts'] >= 4 :
        resp4 = resp_dict['resp4']
        txt += resp4 + "\n\n"
    if len(vars['mcq_zero']) > 0 :
        resp5 = resp_dict['resp5']
        txt += resp5.replace("{mcqlist}", str(vars['mcq_zero'])) + "\n\n"
    if len(vars['as_zero']) > 0 :
        resp6 = resp_dict['resp6']
        txt += resp6.replace("{aslist}" , str(vars['as_zero'])) + "\n\n"
    if vars['f2f_limit'] > 0 and vars['f2f'] < vars['f2f_limit'] : 
        resp7 = resp_dict['resp7']
        txt += resp7 + "\n\n"
    if vars['f2f_limit'] >= 5 and vars['f2f'] >= vars['f2f_limit'] :
        resp8 = resp_dict['resp8']
        txt += resp8 + "\n\n"
    if vars['f2f_limit'] >= 6 and vars['f2f'] >= vars['f2f_limit'] :
        resp9 = resp_dict['resp9']
        txt += resp9 + "\n\n"

    if '{lf}' in txt:
        txt = txt.replace('{lf}' , '\n')
    if '{stage}' in txt:
        txt = txt.replace('{stage}' , vars['stage'])
    if '{avg_score}' in txt:
        txt = txt.replace('{avg_score}' , "{:.2%}".format(vars['avg_score']))
    if '{amt}' in txt:
        txt = txt.replace('{amt}' ,  "{:8.2f}".format(vars['amt']).strip() )
    if '{mcqdate}' in txt:
        txt = txt.replace('{mcqdate}' , vars['mcqdate'])
    if '{asdate}' in txt:
        txt = txt.replace('{asdate}' ,  vars['asdate'] )
    if '{eldate}' in txt:
        txt = txt.replace('{eldate}' ,  vars['eldate'] )
    if '{fcdate}' in txt:
        txt = txt.replace('{fcdate}' ,  vars['fcdate'] )
    if '{sos}' in txt and chatid != 0:
        vmbot.user_list[ chatid ][4] = "👋"
        txt = txt.replace('{sos}' , '')
    if vars['pass_stage'] == 1:
        txt += "You have passed this stage."
    else:
        txt += "You have not passed this stage yet."

    if (dt_model.model_name != "") and ((vars['mcnt'] + vars['acnt']) >0):
        grad_pred = dt_model.predict(vars['mcq_avg'] , vars['as_avg'], 13)
        txt += "\n\nAI grading prediction : " +  "{:.2%}".format(grad_pred[0]) + "\n\n"        
    return txt

def load_vars(sqldb, sid):
    vars = dict()
    def vars_addrec(r):
        if r !='':
            vars[ r.split(':')[0] ] = eval( r.split(':')[1] )
        return    
    process_rec = lambda t : [ vars_addrec(r) for r in t.split('\n') if r != '']
    process_rec( edit_records(sqldb,  'userdata', 'studentid', sid, "mcq_avg") )    
    process_rec( edit_records(sqldb,  'userdata', 'studentid', sid, "mcq_attempts") )    
    process_rec( edit_records(sqldb,  'userdata', 'studentid', sid, "as_avg") )    
    process_rec( edit_records(sqldb,  'userdata', 'studentid', sid, "as_attempts") )    
    process_rec( edit_records(sqldb,  'userdata', 'studentid', sid, "f2f") )    
    process_rec( edit_records(sqldb,  'userdata', 'studentid', sid, "amt") )    

    courseid = sql2var(sqldb, "select courseid from userdata where studentid = " + str(sid), "")    
    vars['courseid'] = courseid
    username = sql2var(sqldb, "select username from userdata where studentid = " + str(sid), "learner")
    vars['username'] = username
    vars['studentid'] = sid
    mcq_str = sql2var(sqldb, "select value from params where key = 'mcq';", " ")
    as_str = sql2var(sqldb, "select value from params where key = 'assignment';", " ")    
    vars['mcq_due_dates'] = {} if mcq_str==' ' else eval(mcq_str)    
    vars['as_due_dates'] = {} if as_str==' ' else eval(as_str)
    stg = sql2var(sqldb, "select stage from userdata where studentid = " + str(sid), "SOC Days")
    vars['stage'] = stg
    vars['f2f'] = 1
    vars['amt'] = 0
    return vars

def test_display_progress(sqldb, sid):
    # test_display_progress("FOS-1219A.db",4477)
    global vmbot, resp_dict, dt_model
    dt_model = vmaiglib.MLGrader()
    dt_model.load_model("dt_model.bin")
    resp_dict = load_respdict()
    vmbot.user_list = []
    vars = load_vars(sqldb, sid)
    stg = vars['stage']
    txt = display_progress(sqldb, stg, vars)
    print(txt)
    return

def mass_update_playbooklist():
    for fn in os.listdir("."):
        if (len(fn)>3) and (fn[-3:]==".db"):
            try:
                df = querydf(fn, "SELECT name FROM sqlite_master WHERE type ='table';")
                tbl_names = [x for x in df.name if x=='stages' or x=='userdata']
                if len(tbl_names)==2:                    
                    course_id = sql2var(fn, "select value from params where key = 'course_id';", '')
                    course_id = str(course_id).strip()
                    if course_id != "":
                        update_playbooklist(fn, course_id)
            except:
                pass
    return 

#------------------------------------------------------------------------------------------------------
if __name__ == "__main__":        
    version = sys.version_info
    if version.major == 3 and version.minor >= 7:        
        #do_main()
        #test_display_progress("FOS-1219A.db",4477)
        #encrypt_email("demo.db")
        #mass_encrypt_email()        
        #if list_table(sqldb = 'FOS-1219A.db', "select studentid,username,amt,grade,stage,f2f from userdata", "List of learners from FOS-1219A") is not None:
        #    plt.savefig('userdata.png', dpi=100)
        print("this is vmbotlib")
    else:
        print("Unable to use this version of python\n", version)
