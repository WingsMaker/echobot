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
from pandas_profiling import ProfileReport

import os, re, sys, time, datetime, string, random 
import subprocess 
import matplotlib.pyplot as plt
import matplotlib.ticker as mtk
import requests

import telepot
from telepot.loop import MessageLoop
from telepot.delegate import pave_event_space, per_chat_id, create_open, per_callback_query_chat_id
from telepot.namedtuple import ReplyKeyboardMarkup
from telepot.helper import IdleEventCoordinator

import wget
import json

import vmnlplib
import vmaiglib
#import vmffnnlib
import vmmcqdlib
import vmsvclib
import vmedxlib
from vmsvclib import *

global vmbot, ft_model, dt_model, nn_model, mcq_analysis 
global rdscon, rds_connstr, edx_api_header, edx_api_url

# following will be replaced by generic config without hardcoded.
btn_hellobot = "Hello OmniMentor 👩‍🎓 👨‍🎓🤖"
option_back = "◀️"
option_import = "Import 📦"
option_export = "Export 💾"
option_mainmenu = "mainmenu"
option_learners = "Learners 👩"
option_faculty = "Faculty"
option_demo = "Demo"
mainmenu = [[option_learners, option_faculty , option_back]]
option_mycourse = "My course"
option_updateprogress = "Update progress"
option_faq = "FAQ"
option_mychat = "LiveChat"
option_mychart = "Chart"
option_binduser = "Auto Sign-in"
option_bind = "Binding"
option_gethelp = "Contact me"
option_info = "Info"
lrn_start = "Learner Started"
lrn_student = "Learner Verified"
learners_menu = [[option_mycourse, option_updateprogress, option_mychart],\
    [option_gethelp, option_mychat, option_faq], [option_binduser, option_info, option_back]]
option_fct = "Faculty Admin 📚"
option_pb = "Playbooks 📗📘📙"
option_analysis = "Analysis 📊"
option_chat = "Chat 💬"
option_sos = "Help 🆘"
option_chart = "Chart 📊"
option_chatlist = "Chat List"
option_chatempty = "Chat Empty"
option_bindadm = "Auto Sign-in 🔐"
mentor_menu = [[option_fct, option_pb, option_analysis], [option_chat, option_bindadm, option_back]]
fc_student = "Student Update"
fc_cohlist = "Cohort Listing"
fc_userimport = "User Import"
fc_edxupdate = "LMS Import"
#fc_edx = "EdX Import"
#fc_assignment = "Update Assignment"
#fc_mcqtest = "Update MCQs"
#fc_schedule = "Schedule Update"
#faculty_menu = [[fc_student,fc_cohlist, fc_edx], [fc_assignment, fc_mcqtest, fc_schedule, option_back]]
faculty_menu = [[fc_student, fc_cohlist, fc_userimport, fc_edxupdate, option_back]]
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
ps_userdata = "Userdata"
ps_schedule = "Schedule 📅"
#ps_stage = "Edit Stage"
ps_stage = "Module Stage"
course_menu = [[ps_userdata, ps_schedule, ps_stage, option_back]]
an_mcq = "MCQ Analysis"
an_chart = "Graph"
an_mcqd = "MCQ Diff. Analysis"
ml_grading = "AI Grading"
opt_aig = "AI Grad Cohorts"
analysis_menu = [[ml_grading, an_mcq, an_mcqd, an_chart, option_back]]  
an_mcqavg = "By MCQ Average"
an_avgatt = "By MCQ Attempts"
an_avgscore = "By MCQ Scores"
opt_mcqd = "MCQ Diff Cohorts"
opt_pbusr = "Playbook Cohorts"
opt_mcqavg = "MCQ Avg Cohorts"
mcqdiff_menu = [[an_avgatt,an_avgscore,an_mcqavg,option_back]]

gen2fa = lambda : (''.join(random.choice( "ABCDEFGHJKLMNPQRTUVWXY0123456789" ) for i in range(32))).upper()
string2date = lambda x,y : datetime.datetime.strptime(x,y).date()
piece = lambda txtstr,seperator,pos : txtstr.split(seperator)[pos]
module_code = lambda x : piece(piece(piece(x,':',1),'+',1),'-',0)
list_avg = lambda x : 0 if len(x)==0 else sum(x)/len(x)
#callgraph = lambda x : vmsvclib.callgraph(x)
#debug = lambda x : vmsvclib.debug(x)

def do_main():
    global vmbot, dt_model, nn_model
    err = 0
    vmsvclib.rds_connstr = ""
    vmsvclib.rdscon = None
    if not loadconfig():
        print("error loading config")
        return
    vmbot = BotInstance()
    botname = vmbot.bot_name
    if dt_model.model_name=="" :
        txt = "AI grading model data file dt_model.bin is missing"
        syslog('system', txt)
        vmbot.bot.sendMessage(vmbot.adminchatid, txt)
        vmbot.bot_running = False
    #elif nn_model.model_name=="" :
    #    txt = "AI grading model data file ffnn_model.hdf5 is missing"
    #    syslog('system', txt)
    #    vmbot.bot.sendMessage(vmbot.adminchatid, txt)
    #    vmbot.bot_running = False
    else:
        print("running " + botname)
        #vmbot.bot.sendMessage(vmbot.adminchatid, "welcome to the bot")
    syslog('system',f"Bot {botname} started.")
    edx_cnt = 0
    edx_time = vmbot.edx_time
    gmt = vmbot.gmt    
    while vmbot.bot_running :
        try:
            checkjoblist(vmbot)
            timenow = time_hhmm(gmt)            
            if (edx_time > 0) and (timenow==edx_time) and (edx_cnt==0) :
                edx_cnt = 1
                job_request("ServiceBot",vmbot.adminchatid,vmbot.client_name,"edx_mass_import","")
                time.sleep(60)
            if (edx_time > 0) and (timenow > edx_time) and (edx_cnt==1):
                edx_cnt = 0
            time.sleep(3)
        except:
            pass

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

def load_edxdata(client_name):
    global vmbot                
    #client_name = vmbot.client_name
    date_today = datetime.datetime.now().date()
    yrnow = str(date_today.strftime('%Y'))    
    query = f"SELECT DISTINCT courseid FROM userdata WHERE client_name = '{client_name}' " 
    query += f" AND SUBSTRING(courseid,-4)='{yrnow}' ORDER BY courseid;"    
    df = rds_df(query)
    if df is None:
        course_list = []
    else:
        df.columns = ['courseid']
        course_list= [x for x in df.courseid]                
        
    #qry = f"SELECT DISTINCT module_code FROM course_module WHERE client_name='{client_name}';"    
    #df = rds_df(qry)
    #df.columns = ['module_code']
    #if df is None:
        #mc_list = []
    #else:
        #mc_list = [x for x in df.module_code]
        #mc_list = list( set( mc_list))
        #course_list = [ x for x in course_list if module_code(x) in mc_list ]
        #print(course_list)
    #course_list = [ x for x in course_list if 'v1:lithan' not in x.lower() ]    
    
    # update existing courses on mcq,attempts,stage schedule
    vmbot.updated_courses = []
    #print(course_list)
    for course_id in course_list:        
        #if course_id == 'course-v1:Lithan+FOS-0620A+17Jun2020':
        eoc = vmedxlib.edx_endofcourse(client_name, course_id)            
        if eoc == 0:
            #print(course_id)
            #print("update_mcq")
            vmedxlib.update_mcq(course_id, client_name)
            #print("update_assignment")
            vmedxlib.update_assignment(course_id, client_name)
            #print("update_schedule")
            vmedxlib.update_schedule(course_id, client_name)
            #print("updated_courses")
            vmbot.updated_courses.append(course_id)        
    
    course_list = vmedxlib.search_course_list(yrnow)
    # new courses created recently , not found RDS
    #print("edx_import")
    for course_id in [ x for x in course_list if x not in vmbot.updated_courses]:
        #print(course_id)
        vmedxlib.edx_import(course_id, client_name)
        
    #print("mass_update_usermaster")
    vmedxlib.mass_update_usermaster(client_name)    
    print("load_edxdata completed")
    return
    
def loadconfig():
    global ft_model, dt_model, nn_model, mcq_analysis, vmbot
    ok = True
    try:
        dt = "dt_model.bin"
        ft = "ft_model.bin"
        ffnn = "ffnn_model.hdf5"
        with open("vmbot.json") as json_file:  
            bot_info = json.load(json_file)
        #print(*bot_info.items(), sep = '\n')
        client_name = bot_info['client_name']
        ft_model = vmnlplib.NLP_Parser()
        ft_model.load_modelfile(ft, client_name)        
        dt_model = vmaiglib.MLGrader()
        dt_model.load_model(dt)
        if dt_model.model_name == "":
            print("Error loading dt_model")
            ok = False
        #nn_model = vmffnnlib.NNGrader()
        #nn_model.model_loader(ffnn)
        #if nn_model.model_name == "":
        #    print("Error loading nn_model")
        #    ok = False
        mcq_analysis = vmmcqdlib.MCQ_Diff()
    except:  
        ok = False  
    return ok

def load_respdict():
    mydict = dict()
    df = rds_df("select * from progress;")
    if df is not None:
        df.columns = ['key','response']  
        keys = [ x for x in df.key]
        resp = [ x for x in df.response]
        mydict=dict(zip(keys,resp))
    return mydict

class BotInstance():
    def __init__(self):
        self.Token = ""
        self.bot_name = ""
        self.bot_id = ""
        self.client_name = ""
        self.bot_running = False
        self.bot = None
        self.user_list = {}
        self.chat_list = {}
        self.job_items = {}
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

        # print(*(self.keys_dict).items(), sep = '\n')
        # print(*(self.cmd_dict).items(), sep = '\n')
        #
        with open("vmbot.json") as json_file:  
            bot_info = json.load(json_file)
        #print(*bot_info.items(), sep = '\n')
        Token = bot_info['BotToken']
        client_name = bot_info['client_name']                
        
        df = rds_df("select * from params where client_name = 'System';")
        if df is None:                        
            print("Unable to access params table from RDS")
            return 
        df.columns = ['client_name','key', 'value']
        par_val = ['' + str(x) for x in df.value]
        par_key = [x for x in df.key]
        par_dict = dict(zip(par_key, par_val))            
        self.adminchatid = int(par_dict['adminchatid'])
        max_duration = int(par_dict['max_duration'])
        self.match_score = eval(par_dict['match_score'])
        self.use_regexpr = int(par_dict['regexpr'])
        self.pass_rate = float(par_dict['pass_rate'])
        self.gmt = int(par_dict['GMT'])
        self.resp_dict = load_respdict()
        if client_name=="":
            client_name = par_dict['client_name']
        self.client_name = client_name        
        df = rds_df(f"select * from params where client_name = '{self.client_name}';")
        if df is None:
            print("Unable to access params table from RDS")
            return 
        df.columns = ['client_name','key', 'value']               
        par_val = ['' + str(x) for x in df.value]
        par_key = [x for x in df.key]
        par_dict = dict(zip(par_key, par_val))
        email_filter = par_dict['email_filter']
        self.efilter = email_filter.split(',')
        self.updated_courses = []
        self.edx_time = int(par_dict['edx_import'])    
        hdr = par_dict['edx_api_header']        
        self.edx_api_header = eval(hdr)
        self.edx_api_url = par_dict['edx_api_url']
        vmedxlib.edx_api_url = self.edx_api_url
        vmedxlib.edx_api_header = self.edx_api_header
        if Token == "":
            Token = par_dict['BotToken']
        try:
            self.bot = telepot.DelegatorBot(Token, [
                pave_event_space()( [per_chat_id(), per_callback_query_chat_id()],
                create_open, MessageCounter, timeout=max_duration, include_callback_query=True),
            ])
            info = self.bot.getMe()
            print(info)
            self.bot_name = info['username']
            self.bot_id = info.get('id')
            self.Token = Token
            print(self.bot_name)            
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
        del par_dict, df, client_name 
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
        
    def get_menukey(self, val): 
        for key, value in (self.keys_dict).items(): 
             if val == value: 
                 return key       
        return "key doesn't exist"        

class MessageCounter(telepot.helper.ChatHandler):    
    def __init__(self, *args, **kwargs):
        super(MessageCounter, self).__init__(*args, **kwargs)
        self.new_session = True
        self.is_admin = False
        self.client_name = ""
        self.chatid = 0
        self.student_id = 0
        self.username = ""
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
        stage_list = list(self.stagetable['name'])
        stage_days = list(self.stagetable['days'])
        self.stage_list = stage_list
        self.stage_days = stage_days
        self.records['username'] = self.username
        self.courseid = self.records['courseid']        
        txt = 'Select the day # to check your progress :'
        btn_list = build_menu(["Day #" + str( stage_days[n] ) for n in range(len(stage_list))], 4, option_back)
        bot_prompt(self.bot, self.chatid, txt, btn_list)
        for stg in stage_list:                    
            result = "stage:'_x_'".replace('_x_', stg)
            edit_fields(self.client_name, self.courseid, "userdata", "studentid", self.student_id, result)
            self.stage_name = stg
            (tt, self.records ) = verify_student(self.userdata, self.student_id, self.courseid)
            tt = display_progress(self.userdata, stg, self.records, self.client_name)
            self.tablerows.append(tt)
        self.stage_name = prev_stage
        self.records['stage'] = prev_stage
        result = "stage:'_x_'".replace('_x_', prev_stage)
        edit_fields(self.client_name, self.courseid, "userdata", "studentid", self.student_id, result)
        #self.parentbot.keys_dict[option_myprogress]
        self.parentbot.keys_dict[lrn_student]        
        return txt

    def mcqas_chart(self, groupcht = False ):
        if self.userdata is None:            
            return            
        avg_list = dict()
        if groupcht:
            condqry = f"client_name = '{self.client_name}' AND courseid = '{self.courseid}' "
            cohort_id = piece(piece(self.courseid,':',1),'+',1)
            fn = 'chart_' + cohort_id + '.png'
            title = f"MCQ and Assignment scores for cohort {cohort_id}"
        else:            
            sid = self.student_id
            if sid == 0:
                return 
            courseid = self.courseid
            cname = self.client_name
            query = f"select * from userdata where client_name = '{cname}' and studentid={sid} and courseid='{courseid}';"
            df = rds_df(query)
            if df is not None:
                df.columns = get_columns("userdata")
                self.userdata = df                            
            condqry = f"client_name = '{self.client_name}' AND courseid = '{self.courseid}' AND studentid = {sid} "
            fn = 'chart_' + str(sid) + '.png'
            title = f"MCQ and Assignment scores for student #{sid}"                
        avg_list = dict()
        for avgopt in ['mcq_avg','as_avg']:
            qry =",".join([ f" avg({avgopt}{x}) as avg{x} " for x in range(1,14) ])
            query = f"select {qry} from userdata where " + condqry
            df = rds_df(query)
            if df is None:                
                return
            df.columns = qry.split(',')
            avg_list[avgopt] = [eval(str(x)) for x in df.values.tolist()[0]]
        df = pd.DataFrame({
            'Test/IU' : [ '#' + str(n) for n in range(1,14) ],    
            'mcq test' : avg_list['mcq_avg'],
            'assignment test' : avg_list['as_avg']
        })
        df.plot(kind='bar',figsize=(10,4), rot = 90)            
        plt.title(title)
        ax = plt.gca()
        cols = [c for c in  df.columns]
        label_col = cols[0]
        xcol = cols[1]
        label_list = [ x for x in df[label_col] ]
        width = 0.8
        plt.xlim([-width, len(df[xcol])-width])
        ax.set_xticklabels((label_list))
        ax.yaxis.set_major_formatter(mtk.PercentFormatter())
        plt.draw()
        plt.savefig(fn, dpi=100)
        plt.clf()
        f = open(fn, 'rb')
        self.bot.sendPhoto(self.chatid, f)
        if groupcht:
            df = pd.DataFrame({
                'Test/IU' : [ '#' + str(n) for n in range(1,14) ],    
                'mcq test' : [ "{:.2%}".format(x) for x in avg_list['mcq_avg'] ],
                'assignment test' : [ "{:.2%}".format(x) for x in avg_list['as_avg'] ]
            })
            if render_table(df, header_columns=0, col_width=6, title_name=title) is not None:
                plt.savefig(fn, dpi=100)
                plt.clf()
                f = open(fn, 'rb')
                self.bot.sendPhoto(self.chatid, f)
        del df
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
                txt += '\nStudent id is ' + str(self.records['studentid'])                
                txt += '\nCourse ID : ' + self.courseid
                txt += '\nCourse Name : ' + self.coursename
                cc = [x for x in self.list_courseids if self.courseid != x]
                if len(cc)>0:
                    txt += "\nOther courses (cohort-id):\n"
                    for x in cc:
                        txt += "\t" + x + "\n"
                txt += '\nLearning Stage : ' + self.stage_name
                txt += '\nOutstanding Amount : ' + str(self.records['amt']) + "\n"
        txt += f"\nYour username is {self.username}"
        txt += f"\nYour telegram chat_id is {self.chatid}"
        txt += f"\nSystem client name is {self.client_name}"
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
        global ft_model, vmbot
        accuracy = 0
        match_score = vmbot.match_score
        use_regexpr = vmbot.use_regexpr
        user_resp = resp.lower()
        txt = ""
        if use_regexpr == 1 :
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
            mcqlist = ""
            aslist = ""
            amt = str( self.records['amt'] )
            mcqas_chart = ""
            mcqdate = stagedate
            asdate = stagedate
            eldate = stagedate
            fcdate = stagedate
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

    def load_tables(self):
        global vmbot
        client_name = vmbot.client_name
        #if (vmbot.edx_time > 0) and (self.courseid not in vmbot.updated_courses):
        #if (self.client_name != "Demo") and (self.courseid not in vmbot.updated_courses):        
        if (client_name != "Demo") :
            self.sender.sendMessage("Please wait for a while.")
            try:            
                #vmedxlib.update_mcq(self.courseid, client_name)
                vmedxlib.update_mcq(self.courseid, client_name, self.student_id)
                #vmedxlib.update_assignment(self.courseid,  client_name)
                vmedxlib.update_assignment(self.courseid, client_name, self.student_id)
                #vmedxlib.update_schedule(self.courseid,  client_name)
                #vmbot.updated_courses.append(self.courseid)        
            except:
                pass
        qry = "select * from userdata where client_name = '_c_' and courseid = '_x_';"
        qry = qry.replace('_c_', self.client_name)
        qry = qry.replace('_x_', self.courseid)
        df = rds_df( qry )
        if df is None:            
            self.userdata = None
            if self.client_name != "Demo":
                vmedxlib.edx_import(self.courseid, self.client_name)
                df = rds_df( qry )
        if df is not None:
            df.columns = get_columns("userdata")
            self.userdata = df
            
        qry = "select * from stages where client_name = '_c_' and courseid = '_x_';"
        qry = qry.replace('_c_', self.client_name)
        qry = qry.replace('_x_', self.courseid)
        df = rds_df( qry)
        if df is None:            
            self.stagetable = None
        else:            
            df.columns = get_columns("stages")        
            self.stagetable = df                    
        return 

    def load_courseinfo(self, resp):        
        if len(self.list_courseids)==0:
            return 0
        query = f"SELECT COUNT(course_id) AS cnt FROM playbooks WHERE course_id='{resp}' and client_name = '{self.client_name}';"
        result = rds_param(query)
        cnt = int("0" + str(result))        
        if cnt == 0:
            self.courseid = ""
            self.userdata = ""
            self.coursename = ""
            ok = 0
            if resp in self.list_courseids:
                n = self.list_courseids.index(resp)
                del (self.list_courseids)[n]
                del (self.list_coursename)[n]
        else:
            n = self.list_courseids.index(resp)
            self.coursename = self.list_coursename[n]
            self.courseid = resp
            self.load_tables()
            ok = 1
        return ok

    def check_student(self, sid, chat_id):
        self.student_id = 0
        txt = ''
        if self.new_session == False :
            return 
        if self.userdata is None:            
            return
        (txt, self.records ) = verify_student(self.userdata, sid, self.courseid)        
        err = 0        
        try:
            self.stage_name = self.records['stage']
            self.stagedate = self.find_stage_date("stagedate")
            self.records['asdate'] = self.stagedate
            self.records['eldate'] = self.stagedate
            self.records['fcdate'] = self.stagedate
            self.records['mcqdate'] = self.stagedate
        except:
            self.records=={}
        if (self.records=={}) or (self.stage_name==""):
            txt = "Hi, there is incomplete information at the moment, the session is not ready yet.\n\n"
            txt += "Please select the course from below list."
            btn_course_list = build_menu(self.list_courseids,1)
            bot_prompt(self.bot, self.chatid, txt, btn_course_list)
            self.menu_id = self.parentbot.keys_dict[option_learners]
            return
        else:
            self.is_admin = False
            self.student_id = sid
            self.new_session = False
            #vmbot.user_list[chat_id]=[self.courseid, self.student_id, self.client_name, chat_id, ""]
            vmbot.user_list[chat_id]=[self.courseid, self.student_id, self.username, chat_id, ""]    
            txt = display_progress(self.userdata, self.stage_name, self.records, self.client_name)            
            if txt == "":
                txt = "Welcome back."
            bot_prompt(self.bot, self.chatid, txt, self.menu_home)
            self.menu_id = self.parentbot.keys_dict[lrn_student]
        return 

    def find_stage_date(self, date_type):
        stage_table = self.stagetable
        if stage_table is None:
            return ''
        row_match = ( stage_table['name'] == self.stage_name ) 
        if len(stage_table[row_match])==0:
            return ''
        df=stage_table[row_match]        
        result = list(df[date_type])[0]
        n = self.records['stage_names'].index(self.stage_name)
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
        (t0, t1, vars) = load_progress(self.userdata, vars['stage'], vars, self.client_name)
        if vars['mcq_att_balance'] == "":
            txt = "There is no outstand MCQs for futher attempts."
        else:
            txt = vars['mcq_att_balance']
        return txt

    def update_stage(self, sid):          
        if (sid <= 0) or (self.userdata is None):
            return ""
        vars = load_vars(self.userdata, sid)       
        client_name = vars['client_name']
        courseid = vars['courseid']
        if True:
            current_stage = get_stage_name(client_name, courseid)
            vars['stage'] = current_stage
            return current_stage
        condqry = f" client_name = '{client_name}' and courseid = '{courseid}';"
        df = rds_df( "select * from stages where " + condqry)
        if df is None:
            return ""
        df.columns = get_columns("stages")
        stage_dates = list(df['stagedate'])
        stage_names = list(df['name'])
        tblsize = len(stage_names)
        current_stage = ""
        vars['mcq_due_dates'] = []
        vars['as_due_dates'] = []
        try:
            pass_stage = 0
            txt = ''
            for n in range(tblsize):
                try:
                    current_stage = stage_names[n]            
                    vars['stage'] = current_stage
                    (t1, t2, vars) = load_progress(self.userdata, current_stage, vars, self.client_name)
                    if t1 == "_eoc_":        
                        pass_stage = 1
                        break
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
            updqry = f"update userdata set stage = '{current_stage}' where studentid = {str(sid)} and " 
            updqry += condqry            
            rds_update(updqry)
            self.userdata.loc[ self.userdata.studentid == sid, "stage" ] = current_stage
            self.stage_name = current_stage
        return current_stage

    def grad_prediction(self):
        global nn_model, dt_model
        if dt_model.model_name == "" :
            print("please load the model first")
            return []
        #if nn_model.model_name == "":
        #    print("please load the model first")
        #    return []
        txt = ""        
        df = self.userdata
        if self.userdata is None:
            print("there is no data")
        list_sid = [str(x) for x in df.studentid]
        if len(list_sid)==0:
            return []
        client_name = list(df['client_name'])[0]
        courseid = list(df['courseid'])[0]
        progress_df = dict()
        progress_tt = dict()
        tbl = []
        new_sidlist = []
        for vv in list_sid:
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
            if mcnt+acnt==0:
                continue
            mavg = 0 if mcnt == 0 else sum(mscores) / mcnt
            aavg = 0 if acnt == 0 else sum(ascores) / acnt
            # current not working                
            use_neural_network = False   # True or False
            if use_neural_network:
                #mavgatt = sum([gx(n) for n in range(1,14)]) / 13
                #mmaxatt = max([gx(n) for n in range(1,14)])
                #as_avgatt = sum([gy(n) for n in range(1,14)]) / 13
                #as_maxatt = max([gy(n) for n in range(1,14)])
                #grad_pred = nn_model.pred(client_name,mavg,mavgatt,mmaxatt,aavg,as_avgatt,as_maxatt,acnt)
                #if grad_pred is None:
                #    continue                    
                pass
            else:
                grad_pred = dt_model.predict(mavg , aavg, mcnt) 
            
            tbl.append( [vv , uu , "{:.2%}".format(grad_pred[0])] )
            progress_list = [ fz(n) for n in range(1,14) if gz(n) > 0]
            progress_list.append(['Avg', "{:.2%}".format(mavg) , " ",  "{:.2%}".format(aavg) , " "])
            
            df =  pd.DataFrame( progress_list )
            df.columns = ['Test #', 'MCQ', '#Attempts', 'Assignment', '# Attempts']                
            progress_df[sid] = df

            progress_tt[sid] = f"\nTest Results for Student #{vv} {uu}"
            new_sidlist.append(str(sid))
        self.records['progress_df'] = progress_df
        self.records['progress_tt'] = progress_tt
        df1 =  pd.DataFrame( tbl )
        if len(df1) == 0:
            self.sender.sendMessage("There is no data for this course id")
            return []
        df1.columns = ['Student ID#','Name', 'Prediction']
        tt = "AI Grading for " + self.courseid
        df1= df1.sort_values(by ='Prediction') 
        if render_table(df1, header_columns=0, col_width=3, title_name=tt) is not None:
            fn='gradpred_' + str(self.chatid) + '.png'
            plt.savefig(fn, dpi=100)
            plt.clf()
            f = open(fn, 'rb')
            self.bot.sendPhoto(self.chatid, f)
        return new_sidlist

    def find_course(self, stud):
        client_name =  self.client_name        
        course_id_list = []
        course_name_list = []
        if client_name == "":
            return (course_id_list, course_name_list)
        if stud==0:
            qry = f"SELECT DISTINCT course_id, course_name FROM playbooks WHERE client_name='{self.client_name}';"
            df = rds_df(qry)
            if df is not None:            
                df.columns = ['course_id','course_name']
                course_id_list = [x for x in df.course_id]
                course_name_list = [x for x in df.course_name]
            return (course_id_list, course_name_list)

        api_url = self.parentbot.edx_api_url
        if api_url == '':
            userinfo = {}
        else:
            url = f"{api_url}/user/fetch/{stud}"
            headers = self.parentbot.edx_api_header            
            userinfo = edxapi_getuser(headers, url)
        if userinfo == {} :            
            (course_id_list, course_name_list) = rds_loadcourse(client_name, stud)
        else:
            self.username = userinfo['username']
            course_id_list = [ x['course_id'] for x in userinfo['enrolments'] ]
            course_name_list = [ x['course_name'] for x in userinfo['enrolments'] ]                
            qry = f"SELECT DISTINCT module_code FROM course_module WHERE client_name='{self.client_name}';"    
            df = rds_df(qry)
            if df is not None:
                df.columns = ['module_code']
                mc_list = [x for x in df.module_code]
                m = len(course_id_list)
                for n in range(m):
                    k = m - 1 - n
                    course_id = course_id_list[k]
                    module_code = piece(piece(piece(course_id,':',1),'+',1),'-',0)
                    if module_code not in mc_list:
                        del course_id_list[k]
                        del course_name_list[k]
        return (course_id_list, course_name_list)

    def on_chat_message(self, msg):
        global ft_model, dt_model, mcq_analysis, vmbot
        try:
            content_type, chat_type, chat_id = telepot.glance(msg)
            bot = self.bot
            self.chatid = chat_id
            keys_dict = vmbot.keys_dict
            adminchatid = vmbot.adminchatid            
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
                self.records['username'] = username
                if self.username=="":
                    self.username = username
        elif content_type != "text":
            txt = "Thanks for the " + content_type + " but I do not need it for now."
            self.sender.sendMessage(txt)
            return
        else:
            syslog( content_type , str(msg) )

        if resp not in list(keys_dict):
            syslog(str(self.chatid),f"response = {resp}")
        if resp=='/?':
            retmsg = self.session_info()
            retmsg += "\nmenu id = " + str(self.menu_id) 
            retmsg += "\nmenu key : " + vmbot.get_menukey(self.menu_id) 

        elif resp=='/progress':
            if self.client_name == "":
                print("client_name is empty")
                return
            if self.client_name == "Demo" :
                try:
                    df = rds_df('select distinct steps from user_stories order by id;')
                    if df is None:
                        retmsg = "The demo system is not ready yet"
                    else:                    
                        #df.columns = get_columns("stages")
                        df.columns = ['steps']
                        demo_list = [x for x in df.steps]
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
            else:                
                retmsg = "Sorry /demo is only supported for client Demo."
        
        elif resp=='/end':
            self.endchat()
            self.is_admin = (chat_id == adminchatid)
            syslog("system","telegram user " + str(chat_id) + " offine.")
            self.logoff()
            
        #elif resp=='/stop' and (chat_id in [adminchatid, 71354936]):
        elif resp=='/stop' and (chat_id == adminchatid):
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
            self.edited = 0
            self.client_name = vmbot.client_name
            if chat_id > 0 :
                self.endchat()
                syslog("system","telegram user " + str(chat_id) + " online.")
                query = "select usertype from user_master where chat_id =" + str(chat_id) + \
                    " and client_name = '" + self.client_name + "';"
                result = rds_param(query)
                usertype = int("0" + str(result))                
                query = "select studentid from user_master where chat_id =" + str(chat_id) + \
                    " and client_name = '" + self.client_name + "';"
                result = rds_param(query)
                sid = int("0" + str(result))
                query = "select username from user_master where studentid =" + str(self.student_id) + \
                    " and client_name = '" + self.client_name + "';"
                self.username = rds_param(query)
                self.student_id = sid                
                if sid > 0:  # binded users found
                    self.sender.sendMessage(f"Welcome {self.username} !")
                    if usertype == 1:
                        query = "select courseid from user_master where chat_id =" + str(chat_id) + \
                            " and client_name = '" + self.client_name + "';"
                        courseid = rds_param(query)                        
                        (self.list_courseids, self.list_coursename) = self.find_course(sid)                        
                        if courseid=="":
                            stud_courselist = self.list_courseids                                
                        else:
                            stud_courselist = [courseid]
                        self.list_courseids = stud_courselist
                        n = len(stud_courselist)                                                
                        if n > 20:
                            date_today = datetime.datetime.now().date()
                            yrnow = str(date_today.strftime('%Y'))
                            course_list = [x for x in stud_courselist if x[-4:]==yrnow]                        
                            btn_course_list = build_menu(course_list,1)
                            txt = "Please select the course id from below:"
                            bot_prompt(self.bot, self.chatid, txt, btn_course_list)
                            self.menu_id = keys_dict[option_learners]
                        elif n > 1:
                            btn_course_list = build_menu(stud_courselist,1)
                            txt = "Please select the course id from below:"
                            bot_prompt(self.bot, self.chatid, txt, btn_course_list)
                            self.menu_id = keys_dict[option_learners]                        
                        elif len(stud_courselist) == 1:
                            query = f"select course_name from playbooks where course_id ='{courseid}' and client_name='{self.client_name}';"
                            coursename = rds_param(query)                            
                            msg = "You are in course:\n" + courseid
                            self.courseid = courseid
                            self.load_tables()                            
                            bot.sendMessage(chat_id, msg)                            
                            self.update_stage(sid)                           
                            self.check_student(self.student_id, self.chatid)
                        return
                    elif usertype == 11:
                        self.is_admin = True
                        self.menu_id = 1
                        txt = banner_msg("Welcome " + self.username,"You are now connected to Mentor mode.")
                        self.menu_home = mentor_menu                            
                        (self.list_courseids, self.list_coursename) = self.find_course(0)
                        bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                        return
                    else:
                        self.sender.sendMessage("Sorry your account is blocked, please contact the admin.")
                        self.logoff()    
                        return
                else:
                    self.list_courseids = []
                    txt = 'My name is OmniMentor, I am your friendly learning facilitator at ' + self.client_name + '.\n'
                    txt += "\nplease enter your student id or email address :"                        
                    bot_prompt(self.bot, self.chatid, txt, [])
                    self.menu_id = keys_dict[option_learners]

        elif self.menu_id == keys_dict[option_mainmenu]:
            if resp == option_fct :
                txt = 'You are in faculty admin mode.'
                bot_prompt(self.bot, self.chatid, txt, faculty_menu)
                self.menu_id = keys_dict[option_fct]
            elif resp == option_pb :
                txt = 'You are in playbooks maintainence mode.'
                bot_prompt(self.bot, self.chatid, txt, playbook_menu)
                self.menu_id = keys_dict[option_pb]
            elif resp == option_chat :
                self.livechat()
            elif resp == option_analysis :
                txt = 'You are in Analysis options.'
                bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                self.menu_id = keys_dict[option_analysis]
            elif resp == option_bindadm:
                txt += "\nDo you want me to activate auto-login without entering admin id each time ?"
                opt_yes = "Yes, enable auto-login"
                opt_no = "No, I would like to login manually each time"
                yesno_menu = build_menu([opt_yes,opt_no],1,option_back)
                bot_prompt(self.bot, self.chatid, txt, yesno_menu)
                txt = ""
                self.menu_id = self.parentbot.keys_dict[option_bind]
            elif (resp == option_back) or (resp == "0"):
                self.endchat()
                syslog("system","telegram user " + str(chat_id) + " offine.")
                self.logoff()

        elif self.menu_id == keys_dict[option_learners] :
            if (resp == option_back) or (resp == "0"):
                self.logoff
                bot_prompt(self.bot, self.chatid, "End of session.", [[btn_hellobot]])
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
                        self.sender.sendMessage("Sorry your account is not found, please contact the admin.")
                        self.logoff()
                        return
                    result = rds_param("SELECT courseid FROM user_master " + condqry)
                    self.courseid = result
                    qry = "select * from userdata where client_name = '_c_' and courseid = '_x_';"
                    qry = qry.replace('_c_', self.client_name)
                    qry = qry.replace('_x_', self.courseid)
                    df = rds_df( qry )
                    if df is None:
                        self.sender.sendMessage("Sorry the system has no complete data yet.")
                        self.logoff()
                        return
                    df.columns = get_columns('userdata')
                    self.userdata = df
                    self.courseids = [x for x in df.courseid]
                    self.coursename =  rds_param(f"SELECT course_name FROM playbooks where course_id = '{self.courseid}'")
                    self.load_tables()
                    self.check_student(sid, chat_id)
                    txt = "Welcome " + self.username
                    self.menu_id = keys_dict[lrn_student]
                    bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                    self.edited = 0
                    return
                else:
                    return
            if resp.isnumeric():
                sid = int(resp)
                query=f"SELECT COUNT(*) AS cnt FROM user_master WHERE studentid={resp} and client_name ='{self.client_name}';"
                result = rds_param(query)
                cnt = int("0" + str(result))
                if cnt == 0:
                    self.sender.sendMessage("Sorry your account is not found, please contact the admin.")
                    self.logoff()
                    return
                usertype = rds_param(f"SELECT usertype FROM user_master WHERE studentid={resp} and client_name ='{self.client_name}';")
                query = "select username from user_master where studentid =" + resp + \
                    " and client_name = '" + self.client_name + "';"
                self.username = rds_param(query)                
                self.sender.sendMessage(f"Welcome {self.username} !")
                if usertype == 11:
                    self.is_admin = True
                    self.menu_id = 1
                    self.student_id = sid
                    txt = banner_msg("Welcome " + self.username,"You are now connected to Mentor mode.")
                    self.menu_home = mentor_menu
                    (self.list_courseids, self.list_coursename) = self.find_course(0)
                    bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                    return
                elif usertype not in [1,2]:
                    self.sender.sendMessage("Sorry your account is blocked, please contact the admin.")
                    self.logoff()    
                    return                
                if sid in vmbot.user_list:
                    txt = "Sorry you can't logon using another telegram account\nPlease try again later."
                    self.logoff(txt)
                    return                
                (self.list_courseids, self.list_coursename) = self.find_course(sid)
                stud_courselist = self.list_courseids                
                slen = len(stud_courselist)
                self.student_id = sid                
                if (slen == 0) :
                    txt = "Sorry, we are not unable to find your record.\nPlease contact the course admin."
                    self.logoff(txt)
                    return                
                if slen == 1:
                    self.chatid = chat_id
                    self.courseid = self.list_courseids[0]
                    self.coursename = self.list_coursename[0]                    
                    self.load_tables()
                    if self.userdata is None:
                        self.sender.sendMessage(f"we do not have any data for course id\n{self.courseid}")
                        return
                    else:
                        self.update_stage(sid)
                        self.check_student(self.student_id, chat_id)
                elif slen < 20:
                    btn_course_list = build_menu(stud_courselist, 1) 
                    txt = "Please select the course id from below:"
                    bot_prompt(self.bot, self.chatid, txt, btn_course_list)
                    self.menu_id = keys_dict[option_learners]                    
                else:
                    #btn_course_list = build_menu(stud_courselist, 1)
                    date_today = datetime.datetime.now().date()
                    yrnow = str(date_today.strftime('%Y'))
                    course_list = [x for x in stud_courselist if x[-4:]==yrnow]
                    btn_course_list = build_menu(course_list, 1) 
                    txt = "Please select the course id from below:"
                    bot_prompt(self.bot, self.chatid, txt, btn_course_list)
                    self.menu_id = keys_dict[option_learners]
            else:                
                if self.load_courseinfo(resp) == 0:
                    txt = 'Your selection is not available !\n'
                    txt += 'Please select the course from below list'
                    date_today = datetime.datetime.now().date()
                    yrnow = str(date_today.strftime('%Y'))
                    #course_list = [x for x in self.list_courseids if x[-4:]==yrnow and resptxt in x.lower()]
                    course_list = [x for x in self.list_courseids if resptxt in x.lower()]
                    btn_course_list  = build_menu(course_list, 1)
                    bot_prompt(self.bot, self.chatid,  txt, btn_course_list )
                else:
                    self.chatid = chat_id                    
                    if self.student_id == 0 or self.chatid == 0 :                        
                        txt = 'My name is OmniMentor, I am your friendly learning facilitator at ' + self.client_name + '.\n'
                        txt += "\nplease enter your student id or email address :"
                        bot_prompt(self.bot, self.chatid, txt, [])
                        txt = ''
                        self.menu_id = keys_dict[lrn_start]                        
                    else:                        
                        sid = self.student_id
                        ch_id = self.chatid   
                        #self.load_tables()  
                        self.update_stage(sid)
                        self.check_student(sid, ch_id)

        elif self.menu_id == keys_dict[lrn_start] :
            userdata = self.userdata
            if self.new_session and '@' in resp :
                resptxt = email_lookup(self.userdata, resptxt)
                resp = resp if resptxt=="" else resptxt
            if resp.isnumeric() and self.new_session :                
                sid = int(resp)                
                usertype = rds_param(f"SELECT usertype FROM user_master WHERE studentid={resp} and client_name ='{self.client_name}';")
                if usertype==3:
                    self.sender.sendMessage("Sorry your account is blocked, please contact the admin.")
                    self.logoff()
                else:
                    self.check_student(sid, chat_id)
                    self.menu_id = keys_dict[lrn_student]
            else:
                retmsg = "please enter your student id or email address :"

        elif self.menu_id == keys_dict[lrn_student] and resp in [option_mycourse, option_updateprogress, \
            option_faq, option_mychat, option_mychart, option_binduser, option_gethelp, option_info, option_back]:
            if (resp == option_back) or (resp == "0"):
                self.logoff()            
            elif resp == option_mycourse:                
                date_today = datetime.datetime.now().date()
                yrnow = str(date_today.strftime('%Y'))
                course_list = [x for x in self.list_courseids if x[-4:]==yrnow]
                btn_course_list = build_menu(course_list, 1)
                txt = "Please select the course id from below:"
                bot_prompt(self.bot, self.chatid, txt, btn_course_list)
                self.menu_id = keys_dict[option_mycourse]
                
            elif resp == option_updateprogress:
                sid = self.student_id
                courseid = self.courseid
                cname = self.client_name
                query = f"select stage from userdata where client_name = '{cname}' and studentid={sid} and courseid='{courseid}';"
                stg = rds_param(query)
                query = f"select * from userdata where client_name = '{cname}' and studentid={sid} and courseid='{courseid}';"
                df = rds_df(query)
                if df is not None:
                    df.columns = get_columns("userdata")
                    self.userdata = df                
                self.stage_name = stg
                self.student_id = sid
                self.courseid = courseid
                self.load_tables()
                (txt, self.records ) = verify_student(self.userdata, sid, self.courseid)                
                retmsg = display_progress(self.userdata, self.stage_name, self.records, self.client_name)                
                self.menu_id = keys_dict[lrn_student]
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
                if self.student_id > 0:
                    self.mcqas_chart()                    
                    txt = 'You are at the main menu.'
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
            if (resp == option_back) or (resp == "0"):
                txt = "You are back to this cohort : " + self.courseid
                bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                self.menu_id = keys_dict[lrn_student]
            elif resp in self.list_courseids:
                n = self.list_courseids.index( resp )
                self.courseid = resp
                self.coursename = self.list_coursename[n]
                self.load_tables()                
                sid = self.student_id                
                self.check_student(self.student_id, chat_id)                
                txt = "You are now with this cohort : " + self.courseid
                bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                self.menu_id = keys_dict[lrn_student]
                #self.new_session = True
                self.student_id = sid               
            else:
                btn_course_list = build_menu(self.list_courseids, 1)
                txt = "Please select the course id from below:"
                bot_prompt(self.bot, self.chatid, txt, btn_course_list)
                self.menu_id = keys_dict[option_mycourse]
                self.new_session = True

        elif self.menu_id == keys_dict[option_demo] :
            if resp in ['0', 'exit']:
                txt = 'End of demo mode.'
                txt = bot_prompt(self.bot, self.chatid, txt)
                self.menu_id = keys_dict[lrn_student]
            elif resp in self.tablerows:
                sid = 1000
                df = rds_df("select * from userdata where client_name ='Demo';")
                if df is None:
                    retmsg =  "There is no demo data found"
                client_name = "Demo"
                df.columns = get_columns("userdata")
                courseid = list(df['courseid'])[0]
                condqry = f" courseid = '{courseid}' and client_name ='Demo' "
                self.userdata = df
                query = "select * from user_stories where steps = '_x_' order by id;"
                query = query.replace('_x_', resp)
                df = rds_df(query)
                if df is None:
                    print("user_stories table has incomplete data.")
                    return
                else:
                    df.columns = get_columns("user_stories")
                    sql_list = [x for x in df.sql]                    
                    for updqry in sql_list:
                        query = updqry.replace(";", '')
                        query += ' and ' if ('where' in updqry.lower()) else ' where '
                        query += condqry + ';'
                        rds_update(query)                                                
                    retmsg = f"you have select {resp}"
            else:
                retmsg = "Your are now in demo mode, there will be no response to above statement."

        elif self.menu_id == keys_dict[option_bind] :            
            if "yes," in resptxt:
                updqry = f"update user_master set chat_id = {str(self.chatid)}, courseid = '{self.courseid}' where client_name = '{self.client_name}' and studentid={str(self.student_id)};"                
                rds_update(updqry)
                txt = "Auto-Login option enabled"
            elif "no," in resptxt:                
                updqry = f"update user_master set chat_id = 0 where client_name = '{self.client_name}' and studentid={str(self.student_id)};"
                rds_update(updqry)
                txt = "Auto-Login option disabled"
            elif (resp == option_back) or (resp == "0"):
                txt = "you are back to main menu"
            bot_prompt(self.bot, self.chatid, txt, self.menu_home)
            syslog(str(self.chatid),txt)
            self.menu_id = keys_dict[lrn_student]

        elif self.menu_id == keys_dict[option_fct]:
            df = rds_df(f"SELECT DISTINCT courseid FROM userdata WHERE client_name = '{self.client_name}' ORDER BY courseid;")
            if df is None:
                course_list = self.list_courseids
            else:
                df.columns = ['courseid']
                userdata_courseids = [ x for x in df.courseid ]
                course_list = [ x for x in self.list_courseids if x in userdata_courseids ]
            btn_course_list = build_menu( course_list , 1)
            if resp == fc_student:
                self.menu_id = keys_dict[fc_student]
                txt = "This determine the learning stage of a learner\n"
                txt += "test data to be used:\n\n"
                bot_prompt(self.bot, self.chatid, txt, btn_course_list)
            elif resp == fc_cohlist :
                txt = "Search course-id by keywords:\n"
                txt += "Example :  FOS\n"
                txt += "Enter 0 to exit"
                bot_prompt(self.bot, self.chatid, txt, [])
                self.menu_id = keys_dict[fc_cohlist]
            elif resp == fc_userimport :                
                self.sender.sendMessage("System has scheduled a job to update user master.")                
                #vmedxlib.mass_update_usermaster(self.client_name)
                job_request("ServiceBot",adminchatid, self.client_name,"mass_update_usermaster","")
                #retmsg = "User master data updated."
            elif resp == fc_edxupdate :
                self.sender.sendMessage("System has scheduled a job to import from LMS.")                
                job_request("ServiceBot",adminchatid, self.client_name,"edx_mass_import","")
                #retmsg = "system updated."
            elif (resp == option_back) or (resp == "0"):
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                self.menu_id = 1

        elif self.menu_id == keys_dict[fc_student]:
            if (resp == option_back) or (resp == "0"):
                txt = "You are back to the faculty menu."
                bot_prompt(self.bot, self.chatid, txt, faculty_menu)
                self.menu_id = keys_dict[option_fct]                
                return
            if resp in self.list_courseids:
                #if vmbot.edx_time > 0 and resp not in vmbot.updated_courses:                
                    #self.sender.sendMessage("Please wait for a while.")
                    #client_name = self.client_name                    
                    #vmedxlib.update_assignment(resp, client_name)
                    #vmedxlib.update_mcq(resp, client_name)
                    #vmedxlib.update_schedule(resp, client_name)   
                    #vmbot.updated_courses.append(resp)            
                self.courseid = resp
                cohort_id = piece(piece(resp,':',1),'+',1)
                condqry = " where client_name = '" + self.client_name + "' and courseid = '" + resp + "';"
                query = "select * from userdata " + condqry                
                df = rds_df( query )
                if df is None:
                    self.userdata = None
                    retmsg = "There is no data for this course id"                    
                    self.sender.sendMessage(retmsg)
                    return
                else:
                    df.columns = get_columns('userdata')
                    self.userdata = df                    
                query = "select studentid,username,amt,grade,stage,f2f from userdata " + condqry
                df = rds_df( query)
                if df is None:
                    retmsg = "There is no data for this course id"                    
                    self.sender.sendMessage(retmsg)
                    returnclea
                else:
                    df.columns = "studentid,username,amt,grade,stage,f2f".split(",")                    
                title_name = "List of learners from " + cohort_id                
                if render_table(df, header_columns=0, col_width=3, title_name=title_name) is not None:
                    fn='userdata' + str(self.chatid) + '.png'
                    plt.savefig(fn, dpi=100)
                    plt.clf()
                    f = open(fn, 'rb')
                    self.bot.sendPhoto(self.chatid, f)
                txt = "Select the following:"
                bot_prompt(self.bot, self.chatid, txt, faculty_submenu)
                self.menu_id = keys_dict[fc_studentsub]
            else:
                if self.list_courseids != []:
                    txt = "select the course id below:"
                    btn_list = build_menu([x for x in self.list_courseids if resptxt in x.lower()],1)
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
                self.menu_id = keys_dict[fc_recupd]
            elif (resp == option_back) or (resp == "0"):
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, faculty_menu)
                self.menu_id = keys_dict[option_fct]

        elif self.menu_id == keys_dict[fc_updstage] :
            if resp == "0":
                txt = "there is nothing to update"            
            else:
                df = self.userdata
                students_list = [x for x in df.studentid]
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
                    cnt = 0
                    for sid in [x for x in students_list if x in selection]:
                        self.update_stage(sid)
                        cnt += 1
                    if cnt == 0:
                        txt = "There is nothing to update."
                    else:
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
                df = self.userdata
                self.tablerows = [x for x in df.studentid]
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
                #df = self.stagetable 
                query = f"select name from stages where client_name = '{self.client_name}' and courseid = '{self.courseid}';"
                df = rds_df(query)
                if df is None:
                    retmsg = "The learning stage table information is incomplete"
                else:
                    df.columns = ['name']
                    stage_list = list( df['name'] )
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
                            txt = edit_fields(self.client_name, self.courseid, "userdata", "studentid", sid, result, self.tablerows)
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
                    txt += edit_records(self.client_name, self.courseid, 'userdata', 'studentid', stage_id, fld)
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
                txt = edit_fields(self.client_name, self.courseid, "userdata", "studentid", self.student_id, resp)
            else:
                txt = "there is nothing to update"
            bot_prompt(self.bot, self.chatid, txt, faculty_submenu)
            self.menu_id = keys_dict[fc_studentsub]

        elif self.menu_id == keys_dict[fc_cohlist]:
            if resp == "0":
                txt = "there is nothing to search"
                bot_prompt(self.bot, self.chatid, txt, faculty_menu)
                self.menu_id = keys_dict[option_fct]
            else:
                result = '%%'.join([w for w in resp.split(' ')  if w != ''])
                result = result.upper()
                course_list = []
                course_list = vmedxlib.search_course_list(result)
                df = pd.DataFrame(course_list, columns=['course_id'])
                fn = "course_list.html"
                write2html(df, title='COURSE LIST', filename=fn)
                bot.sendDocument(chat_id, document=open(fn, 'rb'))
                txt = "You are now at faculty menu"
                bot_prompt(self.bot, self.chatid, txt, faculty_menu)
                self.menu_id = keys_dict[option_fct]

        elif self.menu_id == keys_dict[option_pb]:
            if resp==pb_config:
                fn = "pbconfig.html"
                qry = f"select * from playbooks where client_name = '{self.client_name}'"
                df = rds_df( qry)
                df.columns = get_columns('playbooks')
                write2html(df, title='PLAYBOOKS LIST', filename=fn)
                bot.sendDocument(chat_id, document=open(fn, 'rb'))
            elif resp == pb_userdata:
                txt = "Let's take a look on the persona playbooks."
                playbooklist_menu = [[x] for x in self.list_courseids]
                playbooklist_menu.append([option_back])
                bot_prompt(self.bot, self.chatid, txt, playbooklist_menu)
                self.menu_id = keys_dict[pb_userdata]
            elif (resp == option_back) or (resp == "0"):
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                self.menu_id = 1

        elif self.menu_id == keys_dict[pb_userdata]:
            if self.load_courseinfo(resp) == 1:
                #if vmbot.edx_time > 0 and resp not in vmbot.updated_courses:                
                    #self.sender.sendMessage("Please wait for a while.")
                    #client_name = self.client_name
                    #vmedxlib.update_assignment(resp, client_name)
                    #vmedxlib.update_mcq(resp, client_name)
                    #vmedxlib.update_schedule(resp, client_name)   
                    #vmbot.updated_courses.append(resp)
                self.courseid = resp
                cohort_id = piece(piece(resp,':',1),'+',1)
                self.courseid = resp
                query = "select studentid,username,amt,grade,stage,f2f from userdata"
                query += " where client_name = '" + self.client_name + "' and courseid = '" + resp + "';"
                df = rds_df( query)
                if df is None:
                    retmsg = "There is information for this course at the moment"
                else:
                    df.columns = "studentid,username,amt,grade,stage,f2f".split(",")
                    title_name = "List of learners from " + cohort_id
                    if render_table(df, header_columns=0, col_width=3, title_name=title_name) is not None:
                        fn='userdata' + str(self.chatid) + '.png'
                        plt.savefig(fn, dpi=100)
                        plt.clf()
                        f = open(fn, 'rb')
                        self.bot.sendPhoto(self.chatid, f)
                    txt = "Which tables are you looking at for :\n" + resp + " ?"
                    bot_prompt(self.bot, self.chatid, txt, course_menu)                
                    self.menu_id = keys_dict[opt_pbusr]
            elif (resp == option_back) or (resp == "0"):
                txt = 'You are in playbook maintainence mode.'
                bot_prompt(self.bot, self.chatid, txt, playbook_menu)
                self.menu_id = keys_dict[option_pb]

        elif self.menu_id == keys_dict[opt_pbusr]:
            if resp == ps_userdata :
                fn = "userdata.html"
                write2html(self.userdata, title='LEARNERS INFO', filename=fn)
                bot.sendDocument(chat_id, document=open(fn, 'rb'))
            elif resp == ps_schedule :
                fn = "schedule.html"
                if self.stagetable is None:
                    retmsg = "The schedule information is not available"
                else:
                    write2html(self.stagetable, title='COURSE SCHEDULE', filename=fn)
                    bot.sendDocument(chat_id, document=open(fn, 'rb'))
            elif resp == ps_stage  :
                fn = "stages_master.html"
                query = f"select module_code,id,stage,name as stagename,days,f2f,mcq,assignment,flipclass,IU from stages_master where client_name = '{self.client_name}';"
                df = rds_df(query)
                if df is None:
                    retmsg = "The module stage information is not available"
                else:
                    df.columns = "module_code,id,stage,stagename,days,f2f,mcq,assignment,flipclass,IU".split(",")
                    write2html(df, title='Module Stage Information', filename=fn)
                bot.sendDocument(chat_id, document=open(fn, 'rb'))
            elif (resp == option_back) or (resp == "0"):
                txt = 'You are in playbooks maintainence mode.'
                bot_prompt(self.bot, self.chatid, txt, playbook_menu)
                self.menu_id = keys_dict[option_pb]

        elif self.menu_id == keys_dict[opt_stage] :
            txt = edit_fields(self.client_name, self.courseid, "stages", "stage", self.student_id, resp)
            bot_prompt(self.bot, self.chatid, txt, course_menu)
            self.menu_id = keys_dict[opt_pbusr]

        elif self.menu_id == keys_dict[option_analysis]:
            if (resp == option_back) or (resp == "0"):
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                self.menu_id = 1
            elif resp in [ an_mcqd , an_chart , an_mcq , ml_grading]:
                idx = [ an_mcqd , an_chart , an_mcq , ml_grading ].index(resp)
                txt = "Let's take a look on the following courses."
                courseid_menu = build_menu([x for x in self.list_courseids],1)
                bot_prompt(self.bot, self.chatid, txt, courseid_menu)
                self.menu_id = keys_dict[[ an_mcqd , an_chart , an_mcq , ml_grading ][idx]]

        elif self.menu_id == keys_dict[an_mcqd]:
            if (resp == option_back) or (resp == "0"):
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                self.menu_id = keys_dict[option_analysis]
            elif self.load_courseinfo(resp) == 1:                
                self.menu_id = keys_dict[opt_mcqd]
                txt = "MCQ Difficulty Analysis by:"
                bot_prompt(self.bot, self.chatid, txt, mcqdiff_menu)
            else:  
                newlist = [[x] for x in self.list_courseids if resptxt in x.lower()]
                if len(newlist)>0:
                    txt = "Please select from the list of course id below:"
                    btn_list = build_menu([x for x in self.list_courseids if resptxt in x.lower()],1)
                    bot_prompt(self.bot, self.chatid, txt,btn_list)
                    return
                else:
                    txt = 'Please select the following mode:'
                    bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                    self.menu_id = keys_dict[option_analysis]

        elif self.menu_id == keys_dict[opt_mcqd]:
            if (resp == option_back) or (resp == "0"):
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                self.menu_id = keys_dict[option_analysis]
            elif resp == an_avgatt:            
                mcq_analysis.load_mcqdata(self.client_name, self.courseid)
                if mcq_analysis.top10attempts() is None:
                    retmsg = 'There is no data for this course.'                    
                else:                    
                    plt.draw()
                    fn = "attempts_" + str(chat_id) +".png"
                    plt.savefig(fn, dpi=100)
                    plt.clf()
                    f = open(fn, 'rb')
                    self.bot.sendPhoto(chat_id, f)
            elif resp == an_avgscore:
                mcq_analysis.load_mcqdata(self.client_name, self.courseid)
                if mcq_analysis.top10score() is None:
                    retmsg = 'There is no data for this course.'
                else:
                    plt.draw()
                    fn = "score_" + str(chat_id) +".png"
                    plt.savefig(fn, dpi=100)
                    plt.clf()
                    f = open(fn, 'rb')
                    self.bot.sendPhoto(chat_id, f)
            elif resp == an_mcqavg:
                df = self.userdata
                client_name = list(df['client_name'])[0]
                course_id = list(df['courseid'])[0]
                self.client_name = client_name
                self.courseid = course_id
                qry = f"select * from mcq_data where client_name = '{client_name}' and course_id = '{course_id}';"
                df = rds_df(qry)
                df.columns = get_columns("mcq_data")
                mcq_list = [x for x in df.mcq]
                sorted_list = sorted(list(set([int(x) for x in mcq_list]))) 
                mcq_list = [str(x) for x in sorted_list ] + [option_back]
                m = int((len(mcq_list)+4)/5)
                mcq_menu = [  mcq_list[n*5:][:5] for n in range(m) ]
                txt = "Please select from the list of MCQs below:"
                bot_prompt(self.bot, self.chatid, txt, mcq_menu)
                self.menu_id = keys_dict[opt_mcqavg]

        elif self.menu_id == keys_dict[opt_mcqavg]:            
            if resp.isnumeric() :
                mcq_analysis.load_mcqdata(self.client_name, self.courseid)
                if mcq_analysis.mcq_summary(int(resp)) is None:                
                    retmsg = 'There is no data for this course.'
                else:
                    plt.draw()
                    fn = "attempts_" + str(chat_id) +".png"
                    plt.savefig(fn, dpi=100)
                    plt.clf()
                    f = open(fn, 'rb')
                    self.bot.sendPhoto(chat_id, f)
            elif (resp == option_back) or (resp == "0"):
                self.menu_id = keys_dict[opt_mcqd]
                txt = "MCQ Difficulty Analysis by:"
                bot_prompt(self.bot, self.chatid, txt, mcqdiff_menu)

        elif self.menu_id == keys_dict[an_chart]:
            if self.load_courseinfo(resp) == 1:
                self.mcqas_chart(True)
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                self.menu_id = keys_dict[option_analysis]
            elif (resp == option_back) or (resp == "0"):
                txt = 'Please select the following mode:'
                bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                self.menu_id = keys_dict[option_analysis]
            else:  
                newlist = [[x] for x in self.list_courseids if resptxt in x.lower()]
                if len(newlist)>0:
                    txt = "select the course id below:"
                    btn_list = build_menu([x for x in self.list_courseids if resptxt in x.lower()],1)
                    bot_prompt(self.bot, self.chatid, txt,btn_list)
                    return
                else:
                    txt = 'Please select the following mode:'
                    bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                    self.menu_id = keys_dict[option_analysis]

        elif self.menu_id == keys_dict[an_mcq]:
            txt = ""
            if self.load_courseinfo(resp) == 1:                
                if analyze_cohort(self.client_name, resp) is not None:                    
                    fn='analyze_cohort_' + str(self.chatid) + '.png'
                    plt.savefig(fn, dpi=100)
                    plt.clf()
                    f = open(fn, 'rb')
                    self.bot.sendPhoto(self.chatid, f)
            elif (resp == option_back) or (resp == "0"):
                txt = 'You are back to the analysis menu:'
                bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                self.menu_id = keys_dict[option_analysis]
            else:
                newlist = [[x] for x in self.list_courseids if resptxt in x.lower()]
                if len(newlist)>0:
                    txt = "select the course id below:"                    
                    btn_list = build_menu([x for x in self.list_courseids if resptxt in x.lower()],1)
                    bot_prompt(self.bot, self.chatid, txt, btn_list)
                    return
            txt = 'You are back to the analysis menu:' if txt=="" else txt
            bot_prompt(self.bot, self.chatid, txt, analysis_menu)
            self.menu_id = keys_dict[option_analysis]

        elif self.menu_id == keys_dict[ml_grading]:
            txt = ""
            if self.load_courseinfo(resp) == 1:
                self.courseid = resp                
                sid_list = self.grad_prediction()
                btn_list = build_menu( sid_list, 4, option_back, [])
                self.records['progress_sid'] = sid_list
                txt = "Select the student id to see the progress :"
                bot_prompt(self.bot, self.chatid, txt, btn_list)
                self.menu_id = keys_dict[opt_aig]
                return
            elif (resp == option_back) or (resp == "0"):
                txt = 'Select your option:'
                bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                self.menu_id = keys_dict[option_analysis]
                return
            else:
                newlist = [[x] for x in self.list_courseids if resptxt in x.lower()]                
                if len(newlist)>0:
                    txt = "select the course id below:"
                    btn_list = build_menu([x for x in self.list_courseids if resptxt in x.lower()],1)
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
            elif (resp == option_back) or (resp == "0"):
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
                elif (resp == option_back) or (resp == "0"):
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
    global vmbot
    if message == "":
        return
    date_now = time.strftime('%Y%m%d', time.localtime() )
    time_now = time.strftime('%H%M%S', time.localtime() )
    try:
        query = """insert into syslog(date,time,type,message) values(_d,"_t","_x","_y");"""
        query = query.replace('_d',date_now)
        query = query.replace('_t',str(time_now))
        query = query.replace('_x',msgtype)
        query = query.replace('_y',message)
        rds_update(query)
    except:
        #print("Error for ", msgtype, message)
        pass
    return
   
def verify_student(userdata, student_id, courseid):
    global vmbot    
    cname = vmbot.client_name
    vars = dict()
    amt = 0    
    sid = int(student_id)
    query = f"select * from userdata where client_name = '{cname}' and studentid={sid} and courseid='{courseid}';"    
    df = rds_df(query)
    if df is not None:
        df.columns = get_columns("userdata")
        userdata = df         
    if userdata is None:
        return (msg, vars)
        
    vars = load_vars(userdata, student_id)    
    amt = vars['amt']
    student_name = vars['username']
    stage = vars['stage']
    grade = vars['grade']        
    condqry = f" client_name = '{cname}' and courseid = '{courseid}'"
    query = "select * from stages where " + condqry 
    stage_table = rds_df(query)    
    if stage_table is None:        
        msg = f'Unable to load stage table for course-id {courseid}' 
        return (msg, vars)
        
    stage_table.columns = get_columns("stages")
    stage_list = [x for x in stage_table.name]
    date_list = [x for x in stage_table.stagedate]    
    vars['stage_dates'] = date_list
    vars['stage_names'] = stage_list
    msg = '\n\nHi ' + student_name + " !\n"               
    if stage is None:
        stage = stage_list[0]
        vars['stage'] = stage
    msg += 'You are currently on stage ' + stage + "\n\n"    
    
    return (msg, vars)

def get_stage_name(clt, courseid):
    query = f"SELECT `name` FROM stages WHERE client_name = '{clt}' AND courseid='{courseid}' AND STR_TO_DATE(stagedate,'%d/%m/%Y') <= CURDATE() ORDER BY id DESC LIMIT 1;"
    result = rds_param(query)                
    return result

def evaluate_progress(vars,iu_list,passingrate,var_prefix,var_title):    
    avg_prefix = var_prefix + "_avg"
    att_prefix = var_prefix + "_attempts"    
    score_zero = [] ; iu_score = [] ; iu_attempts = []
    score_avg = 0 ; tt = '' ; iu_cnt = 0 ; attempts_balance = ""
    if iu_list != '0' :
        iu_attempts = [ vars[x] for x in [ att_prefix + x for x in iu_list.split(',') ]]
        iu_vars = [int(x) for x in iu_list.split(',')]
        iu_att = dict(zip(iu_vars, iu_attempts))
        score_pass = [ x for x in iu_vars if vars[avg_prefix + str(x)]>=passingrate]
        #score_failed = [ x for x in iu_vars if vars[avg_prefix + str(x)] < passingrate and vars[avg_prefix + str(x)] > 0]
        score_failed = [ x for x in iu_vars if (iu_att[x] > 0) and (vars[avg_prefix + str(x)] < passingrate) and (vars[avg_prefix + str(x)] >= 0)]
        #score_zero = [ x for x in iu_vars if vars[avg_prefix + str(x)] == 0]
        score_zero = [ x for x in iu_vars if (iu_att[x] == 0) and (vars[avg_prefix + str(x)] == 0)]
        iu_score = [ vars[x] for x in [ avg_prefix + x for x in iu_list.split(',') ]]  
        iu_score = [ eval(str(x)) for x in iu_score]
        score_avg = sum(iu_score)/len(iu_score) if iu_score != [] else 0
        
        tt += "\n" + var_title + " average test score : " + "{:.2%}".format(score_avg)
        if len(score_pass) > 0:
            tt += "\n☑ " + var_title + " test passed : " + str(score_pass)
        if len(score_failed) > 0:                
            tt += "\n‼ " + var_title + " test failed : " + str(score_failed)
        if len(score_zero) > 0:                
            tt += "\n☐ " + var_title + " test pending : " + str(score_zero)
        iu_cnt = len(score_pass) + len(score_failed)
        m = 4 if var_prefix=="mcq" else 1
        
        if var_prefix=="as" :
            attempts_balance = "".join([ ("\n" + var_title + ' #'+str(x) + " has " + str(m-vars[att_prefix + str(x)])+" attempts left"  ) for x in iu_vars \
                if vars[avg_prefix + str(x)] < passingrate ])
            tt += attempts_balance + "\n"
        elif (var_prefix=="mcq") and len(score_failed)>0:
            sid = int(vars["studentid"])
            client_name = vars['client_name']
            course_id = vars['courseid']
            df = rds_df(f"SELECT mcq, qn, (4 - attempts) as att from mcq_data WHERE client_name='{client_name}' and course_id='{course_id}' and student_id={sid};")
            if df is not None:
                df.columns = ['mcq', 'qn', 'att']    
                df1 = pd.pivot_table(df, values='att', index=['mcq'],columns='qn')
                df1 = df1.fillna(0)        
                cols = list(df1.columns)
                cnt = len(cols) 
                tt += "\nMCQ attempts left :"
                for index, row in df1.iterrows():
                    if index in score_failed:
                        list1 = str(list(row))                        
                        tt += f"\n#{index} : {list1}"                    
                tt += "\n"
    return (tt, score_avg, score_zero, iu_score , iu_attempts, iu_cnt, attempts_balance)

def load_progress(df, stg, vars, client_name):
    global vmbot        
    resp_dict = vmbot.resp_dict
    if ( len(stg) == 0) or (df is None) or (vars == {}):
        return ("", "", vars)
    courseid = list(df['courseid'])[0]
    condqry = f" client_name = '{client_name}' and courseid = '{courseid}';"
    pass_rate = vmbot.pass_rate

    stagebyschedule = get_stage_name(client_name, courseid)    
    
    #seperator = re.compile('[a-zA-Z0-9\ ]').sub('',stg)    
    #if 'soc' in stg.lower() and seperator=='':
    #    stage = 'SOC'
    #else:        
    #    stg_list = [x.strip() for x in stg.split(seperator)]    
    #    stage = stg_list[1] if len(stg_list)>=2 else stg_list[0]
    #query = "select * from stages where stage = '" + stage + "' and " + condqry 
    query = "select * from stages where `name` = '" + stagebyschedule + "' and " + condqry 
    stagedf = rds_df(query)    
    if stagedf is None:           
        return ("", "", vars)
    stagedf.columns = get_columns("stages")    
    #stagebyschedule = get_stage_name(stagedf)
    
    mcqvars = [x for x in stagedf.mcq][0]
    asvars = [x for x in stagedf.assignment][0]
    f2fvars = [x for x in stagedf.f2f][0]
    stage_desc = [x for x in stagedf.desc][0]
    stg_date = [x for x in stagedf.stagedate][0]    
    resp_dict = vmbot.resp_dict
    stage = stagebyschedule
    vars['stage'] = stagebyschedule        
    txt_hdr = resp_dict['stg0']
    
    #if "eoc" not in stagebyschedule.lower():        
        #txt_hdr += resp_dict['stg1']
        
    if '{stage_desc}' in txt_hdr:
        txt_hdr = txt_hdr.replace('{stage_desc}' , stage_desc)
    if '{username}' in txt_hdr:
        txt_hdr = txt_hdr.replace('{username}' , vars['username'])
    if '{stage}' in txt_hdr:
        #txt_hdr = txt_hdr.replace('{stage}' , stg)
        txt_hdr = txt_hdr.replace('{stage}' , stagebyschedule)
    if '{stagebyschedule}' in txt_hdr:
        txt_hdr = txt_hdr.replace('{stagebyschedule}' , stagebyschedule)
    if '{lf}' in txt_hdr:
        txt_hdr = txt_hdr.replace('{lf}' , '\n')

    mcqas_list = [] ; list_attempts = [] ; f2f_limit = int(f2fvars) ; txt = "" ; t1 = '' ; t2 = ''
    mcnt = 0 ; mcqdate = '' ; mcq_avg = 0 ; mcq_zero = [] ; mcq_att_balance = ""
    acnt = 0 ; asdate = '' ; as_avg = 0 ; as_zero = [] ; as_att_balance = ""    
    mcqdate = stg_date
    (t1, mcq_avg, mcq_zero, mcqas_list1 , mcq_attempts, mcnt, mcq_att_balance) = \
        evaluate_progress(vars, mcqvars, pass_rate, 'mcq','MCQ')

    txt += t1
    mcqas_list += mcqas_list1
    list_attempts += mcq_attempts
    asdate = stg_date
    (t2, as_avg, as_zero, mcqas_list2 , as_attempts, acnt, as_att_balance) = \
        evaluate_progress(vars, asvars, pass_rate, 'as','Assignment')
    txt += t2
    mcqas_list += mcqas_list2
    list_attempts += as_attempts
    mm = len(mcqas_list)
    has_score = 1 if mm > 0 else 0

    txt += "\n\n"
    avg_score  = sum(mcqas_list)/len(mcqas_list) if mcqas_list != [] else 0
    avg_score = round(avg_score*100)/100
    mcqas_complete = 1 if len([n+1 for n in range(len(list_attempts)) if list_attempts[n]==0])==0 else 0
    max_attempts = 0 if len(list_attempts)==0 else max(list_attempts)
    mcqdate = stg_date
    asdate  = stg_date
    eldate  = stg_date
    fcdate  = stg_date
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
    if "eoc" in stage.lower():
        pass_stage = 1
        txt = ""
        txt_hdr = "_eoc_"
        
    stage = stg
    for vv in ['stage', 'mcqdate', 'asdate', 'eldate', 'fcdate', 'avg_score', 'mcqas_complete', \
            'has_score', 'pass_stage', 'max_attempts', 'mcqas_list', 'mcq_zero', 'mcq_avg', \
            'mcnt', 'acnt', 'as_avg', 'as_zero', 'f2f_limit', 'stage_desc', 'mcq_attempts', \
                'mcq_att_balance', 'as_att_balance', 'as_attempts'] :
        vars[vv] = eval(vv)
    return (txt_hdr, txt, vars)

def display_progress(df, stg, vars, client_name):
    global vmbot, dt_model , nn_model        
    #if vars == {}:
    if len(list(vars)) == 0:        
        return "Your information is incomplete, please do not proceed and inform you faculty admin."
    resp_dict = vmbot.resp_dict
    pass_rate = vmbot.pass_rate
    (txt1, txt2, vars) = load_progress(df, stg, vars, client_name)
    txt = txt1 + txt2        
    if txt == "":
        return "Your information is incomplete, please do not proceed and inform you faculty admin."
    if txt == "_eoc_":        
        return "Congratulations, you hae reached the end of the course."
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

    if vars['mcqas_complete']==1 and vars['avg_score']>= pass_rate :
        resp1 = resp_dict['resp1']
        txt += resp1 + "\n\n"
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

    use_neural_network = False # True or False
    if use_neural_network:                    
        pass
        #if (nn_model.model_name != "") and ((vars['mcnt'] + vars['acnt']) >0):
        #    mavg = vars['mcq_avg']
        #    mcqavgatt = list_avg( vars['mcq_attempts'] )
        #    mcqmaxatt = max( vars['mcq_attempts'] )
        #    aavg = vars['as_avg'] 
        #    acnt = vars['acnt']  
        #    as_avgatt = list_avg( vars['as_attempts'] )
        #    as_maxatt = max( vars['as_attempts'] )
        #    grad_pred_nn = nn_model.pred(client_name,mavg,mcqavgatt,mcqmaxatt,aavg,as_avgatt,as_maxatt,acnt)
        #    txt += "\n\nAI grading prediction : " +  "{:.2%}".format(grad_pred[0]) + "\n\n"
    else:
        if (dt_model.model_name != "") and ((vars['mcnt'] + vars['acnt']) >0):
            grad_pred = dt_model.predict(vars['mcq_avg'] , vars['as_avg'], 13)
            txt += "\n\nAI grading prediction : " +  "{:.2%}".format(grad_pred[0]) + "\n\n"        
    return txt

def load_vars(df, sid):
    vars = df[df.studentid==sid].iloc[0].to_dict()
    for x in list(vars):        
        if (x in ['amt', 'grade']) or ('_avg' in x):
            vars[x] = eval(str(vars[x]))
    vars['mcq_due_dates'] = []
    vars['as_due_dates'] = []
    #print(*vars.items(), sep = '\n')    
    return vars

def job_request(bot_req,chat_id,client_name,func_req,func_param):
    if func_req == "":
        return
    gen_job_id = lambda : (''.join(random.choice( "ABCDEFGHJKLMNPQRTUVWXY0123456789" ) for i in range(12))).upper()    
    date_now = time.strftime('%Y%m%d', time.localtime() )
    time_now = time.strftime('%H%M%S', time.localtime() )
    job_id = gen_job_id()
    try:
        query = 'insert into job_list(date,time_start,time_end,job_id,client_name,chat_id,'
        query += 'bot_req, func_req, func-param, status, message)'
        query += ' values(_d, "_t", "0", "_j", "_c", _x, "_b", "_f", "_p","open","");'
        query = query.replace('_d',date_now)
        query = query.replace('_t',str(time_now))
        query = query.replace('_j',job_id)
        query = query.replace('_c',client_name)
        query = query.replace('_x',str(chat_id))
        query = query.replace('_b',bot_req)
        query = query.replace('_f',func_req)
        query = query.replace('_p',func_param)
        query = query.replace('-p','_p')
        rds_update(query)
        #print(query)
    except:
        print(f"Error for job {job_id} on function {func_req}")
    return

def edxapi_getuser(headers, url):
    edx_info = {}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:        
        data = response.content.decode('utf-8')        
        if len(data)>0:
            edx_info = json.loads(data)
    return edx_info

def rds_loadcourse(client_name, stud):
    course_id_list = []
    course_name_list = []
    qry0 = "SELECT DISTINCT ud.courseid, pb.course_name FROM userdata ud INNER JOIN playbooks pb "
    qry0 += "ON ud.client_name=pb.client_name AND ud.courseid=pb.course_id "
    qry = qry0 + f"WHERE ud.studentid={str(stud)} and ud.client_name='{client_name}';"            
    df = rds_df(qry)    
    if df is not None:
        df.columns = ['courseid','course_name']
        course_id_list = [x for x in df.courseid]
        course_name_list = [x for x in df.course_name]
    return (course_id_list, course_name_list)

def profiler_report(client_name, output_file):
    try:
        ok = 1
        mcqinfo = rds_df( f"SELECT * FROM mcqas_info WHERE client_name='{client_name}';")
        mcqinfo.columns = get_columns("mcqas_info")
        features = ['mcq_avgscore', 'mcq_cnt', 'as_avgscore']
        df = mcqinfo[['grade'] + features ]
        pf = ProfileReport(df)
        pf.to_file(output_file)
    except:
        ok = 0
    return ok

def mcq_avg_dict(client_name, course_id, xvar, rsum, avgmode = True):
    if rsum < 1:
        return {}
    sep = '+' if avgmode else ','
    updqry = sep.join([ xvar + str(x) for x in range( 1, rsum + 1 )])
    if avgmode:
        query = "select studentid, ((" + updqry + ") / " + str(rsum) +  ".0"
        query += ") as xvar from userdata where courseid = '" + course_id + "' and client_name = '" + client_name + "';"
        df = rds_df(query)
        df.columns = ['studentid','xvar']
    else:
        query = "select studentid, " + updqry + " from userdata where courseid = '" + course_id + "' and client_name = '" + client_name + "';"
        df = rds_df(query)
        df.columns = ['studentid'] + [ xvar + str(x) for x in range( 1, rsum + 1 )]
        updqry  =  ', '.join([ "x." + xvar + str(x) for x in range( 1, rsum + 1 ) ])
        df_expr = "max(" + updqry + ")"
        df["xvar"] = df.apply(lambda x : df_expr, axis=1)
    if df is None:
        rdict = dict()
    else:
        list1 = [ r for r in df.studentid ]
        list2 = [ r for r in df.xvar ]
        if list1==[]:
            rdict = dict()
        else:
            rdict = dict(zip( list1 , list2))    
    return rdict

def analyze_cohort(client_name, course_id):
    global dt_model
    try:
        x = dt_model.score
    except:
        dt_model = vmaiglib.MLGrader()
        dt_model.load_model("dt_model.bin")
    itemlist = lambda x,y : "\n".join([ ",".join(x[n*y:][:y]) for n in range(int((len(x)+y-1)/y))])
    result_table = [ [' ' for m in range(4)] for n in range(18) ]
    avgsum_list = vmedxlib.count_avg_cols(client_name, course_id, 13, "mcq_avg")
    rr = [ 1 if r>0 else 0 for r in avgsum_list ]
    rsum = sum(rr)
    idx = 0
    if rsum>0:
        for n in range(13) :
            if rr[n]>0:
                result_table[idx][0] = '# ' + str(n+1)
                result_table[idx][1] = "{:.2%}".format(avgsum_list[n])
                idx+=1
        rdict = mcq_avg_dict(client_name, course_id, "mcq_avg", rsum, True)
        xdict = mcq_avg_dict(client_name, course_id, "mcq_attempts", rsum, True)
        ydict = mcq_avg_dict(client_name, course_id, "mcq_attempts", rsum, False)
        result_table[idx][0] = "Avg MCQ Score"
        result_table[idx][1] = "Student #"
        idx += 1
        result_table[idx][0] = ">= 70%"
        rlist = [str(r) for r in rdict if rdict[r] >= 0.7]
        result_table[idx][1] = itemlist(rlist,7)
        idx += 1
        result_table[idx][0] = "between 30% & 70%"
        rlist = [ str(r) for r in rdict if rdict[r] > 0.3 and rdict[r] < 0.7 ]
        result_table[idx][1] = itemlist(rlist,7)
        idx += 1
        result_table[idx][0] = "below 30%"
        rlist = [ str(r) for r in rdict if rdict[r] <= 0.3 ]
        result_table[idx][1] = itemlist(rlist,7)
        idx += 1
        result_table[idx][0] = "Avg Attempts >= 2"
        rlist = [ str(r) for r in xdict if xdict[r] >= 2 ]
        result_table[idx][1] = itemlist(rlist,7)

    avgsum_list = vmedxlib.count_avg_cols(client_name, course_id, 13, "as_avg")
    rr = [ 1 if r>0 else 0 for r in avgsum_list ]
    rsum = sum(rr)    
    idx = 0
    if rsum>0:        
        for n in range(13) :
            if rr[n]>0:
                result_table[idx][2] = '# ' + str(n+1)
                result_table[idx][3] = "{:.2%}".format(avgsum_list[n])
                idx+=1
        rdict = mcq_avg_dict(client_name, course_id, "as_avg", rsum, True)
        xdict = mcq_avg_dict(client_name, course_id, "as_attempts", rsum, True)
        ydict = mcq_avg_dict(client_name, course_id, "as_attempts", rsum, False)
        result_table[idx][2] = "Avg Assignment Score"
        result_table[idx][3] = "Student #"
        idx += 1
        result_table[idx][2] = ">= 70%"
        rlist = [str(r) for r in rdict if rdict[r] >= 0.7]
        result_table[idx][3] = itemlist(rlist,7)
        idx += 1
        result_table[idx][2] = "between 30% & 70%"
        rlist = [ str(r) for r in rdict if rdict[r] > 0.3 and rdict[r] < 0.7 ]
        result_table[idx][3] = itemlist(rlist,7)
        idx += 1
        result_table[idx][2] = "below 30%"
        rlist = [ str(r) for r in rdict if rdict[r] <= 0.3 ]
        result_table[idx][3] = itemlist(rlist,7)
        idx += 1
        result_table[idx][2] = "Avg Attempts >= 2"
        rlist = [ str(r) for r in xdict if xdict[r] >= 2 ]
        result_table[idx][3] = itemlist(rlist,7)
    df =  pd.DataFrame( result_table )
    df.columns = ['MCQ Test #', 'Average Score' , 'Assignment Test #', 'Average Score']
    tt = "Assignment & MCQ Summary for " + course_id
    tbl = render_table(df, header_columns=0, col_width=6, title_name=tt) 
    return tbl

def checkjoblist(vmbot):
    client_name = vmbot.client_name
    df = rds_df(f"select * from job_list where status='open' and client_name='{client_name}';")    
    if df is None:
        #print("no job found")
        return
    df.columns = get_columns("job_list")
    jobitem = df.iloc[0].to_dict()
    vmbot.job_items = jobitem
    msg =  runbotjob(vmbot)
    if msg == "":
        msg = "job complete complete"
    time_now = time.strftime('%H%M%S', time.localtime() )
    time_end = str(time_now)
    status = "completed"
    job_id = jobitem['job_id']
    updqry = f"update job_list set time_end = '{time_end}', status = '{status}', message = '{msg}' where job_id = '{job_id}';"
    rds_update(updqry)
    return

def runbotjob(vmbot):
    global edx_api_header, edx_api_url    
    jobitem = vmbot.job_items
    client_name = vmbot.client_name
    #print(*jobitem.items(), sep = '\n')            
    job_id = jobitem['job_id']
    #client_name = jobitem['client_name']
    chat_id = int(jobitem['chat_id'])
    bot_req = jobitem['bot_req']
    func_req = jobitem['func_req']
    func_param = jobitem['func_param']
    func_svc_list = ["update_assignment" , "update_mcq" , "edx_import", "update_schedule"]    
    edx_time = vmbot.edx_time
    txt = "job "
    updqry = f"update job_list set status = 'running', message = '' where job_id = '{job_id}';"
    rds_update(updqry)
    jobitem['status'] = 'running'
    #try:
        #vmbot.bot.sendMessage(chat_id,f"running job {job_id}")
    #except:
        #print(f"running job {job_id}")            
    vmsvclib.rds_connstr = ""
    vmsvclib.rdscon = None        
    if func_req == "generate_mcq_as":
        try:
            vmedxlib.generate_mcq_as(func_param)
            txt += " completed successfully."
        except:
            txt += " failed."               
    elif func_req in ["edx_mass_import", "mass_update_assignment", "mass_update_mcq", "mass_update_schedule", "mass_update_usermaster"]:
        #try:
        #    func_svc = "vmedxlib." + func_req + "(client_name)"
        #    status = eval(func_svc)
        if func_req == "mass_update_assignment":
            vmedxlib.mass_update_assignment(client_name)
        elif func_req == "mass_update_mcq":
            vmedxlib.mass_update_mcq(client_name)            
        elif func_req == "mass_update_schedule":
            vmedxlib.mass_update_schedule(client_name)
        elif func_req == "edx_mass_import":
            vmedxlib.edx_mass_import(client_name)        
        txt += " completed successfully."
        #except:
        #    txt += " failed."
    elif func_req in func_svc_list :
        course_id = func_param              
        #try:            
        #    func_svc = "vmedxlib." + func_req + "(course_id, client_name)"
        #    status = eval(func_svc)        
        if func_req == "update_assignment":
            vmedxlib.update_assignment(course_id, client_name)
        elif func_req == "update_mcq":
            vmedxlib.update_mcq(course_id, client_name)            
        elif func_req == "update_schedule":
            vmedxlib.update_schedule(course_id, client_name)
        elif func_req == "edx_import":
            vmedxlib.edx_import(course_id, client_name)
        txt += " completed successfully."
        #except:
        #    txt += " failed."
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
    try:        
        vmbot.bot.sendMessage(chat_id,f"completed job {job_id}")
    except:
        print(f"completed job {job_id}")
    jobitem['status'] = 'completed'
    vmbot.job_items = {}
    return txt

#------------------------------------------------------------------------------------------------------
if __name__ == "__main__":        
    version = sys.version_info
    if version.major == 3 and version.minor >= 7:        
        do_main()
        vmsvclib.rds_connstr = ""
        vmsvclib.rdscon = None    
        #client_name = 'Sambaash'
        print("this is vmbotlib")
    else:
        print("Unable to use this version of python\n", version)

        
