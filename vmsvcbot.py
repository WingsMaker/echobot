#
#
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
# ∙∙·▫▫ᵒᴼᵒ▫ₒₒ▫ᵒᴼ OmniMentor Service Bot ᴼᵒ▫ₒₒ▫ᵒᴼᵒ▫▫·∙∙
#------------------------------------------------------------------------------------------------------
#
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore")

import os, sys, time
import random, wget
import subprocess
import pandas as pd
from pandas_profiling import ProfileReport
import json
import datetime
import re
import requests

import telepot
from telepot.loop import MessageLoop
from telepot.delegate import pave_event_space, per_chat_id, create_open, per_callback_query_chat_id
from telepot.namedtuple import ReplyKeyboardMarkup
from telepot.helper import IdleEventCoordinator

import vmedxlib
import vmnlplib
import vmaiglib
import vmffnnlib
import vmsvclib
from vmsvclib import *

global svcbot, edx_api_header, edx_api_url

developerid = 71354936
omchat = vmnlplib.NLP_Parser()
dt_model = vmaiglib.MLGrader()
nn_model = vmffnnlib.NNGrader()
option_mainmenu = 'svcbot_menu'
option_back = "◀️" 
option_nlp = "NLP"
option_ml = "Machine Learning"
option_2fa = "2FA"
option_syscfg = "System 🖥️"
option_cmd = "Commands Shell 📺"
option_usermgmt = "Manage Users 👥"
svcbot_menu = [[option_nlp, option_ml, option_syscfg], [option_usermgmt, option_cmd, option_back]]
nlp_prompts = "Bot Prompts"
nlp_dict = "Dictionary 📖"
nlp_corpus = "Corpus"
nlp_stopwords = "Stopwords"
nlp_faq = "FAQ List"
nlp_train = "Train NLP"
nlp_response = "Responses"
nlp_menu = [[nlp_dict, nlp_prompts, nlp_corpus, nlp_response], [nlp_train, nlp_stopwords, nlp_faq, option_back]]
ml_data = "Model Data"
ml_pipeline = "ML Pipeline"
ml_report = "ML EDA"
ml_train = "Train Model" 
ml_graph = "ML Graph"
ml_menu = [[ml_data, ml_pipeline, ml_report],[ml_graph, ml_train, option_back]]
sys_import = "Mass Import"
sys_update = "Mass Update"
sys_params = "System Parameters"
sys_logs = "System Logs"
sys_jobs = "System Jobs"
stage_master = "Schedule Template"
option_client = "Client Copy"
option_searchbyname = "Name Search"
option_searchbyemail = "Email Search"
option_resetuser = "Reset User"
option_admin_users = "Admin Users"
option_binded_users = "Binded Users"
option_active_users = "Active Users"
option_blocked_users = "Blocked Users"
users_menu = [[option_searchbyname, option_searchbyemail, option_resetuser, option_active_users],[option_admin_users, option_binded_users, option_blocked_users, option_back]]

system_menu = [[sys_params, sys_logs, sys_jobs],[option_client, stage_master, option_back]]

piece = lambda txtstr,seperator,pos : txtstr.split(seperator)[pos]
string2date = lambda x,y : datetime.datetime.strptime(x,y).date()
str2date = lambda x : string2date(x,"%d/%m/%Y")            
date_today = datetime.datetime.now().date()
dmy_str = lambda v : piece(v,'-',2) + '/' + piece(v,'-',1) + '/' + piece(v,'-',0)
ymd_str = lambda v : piece(v,'-',0) + '/' + piece(v,'-',1) + '/' + piece(v,'-',2)

class BotInstance():
    def __init__(self, Token, client_name, max_duration, adminchatid):
        self.Token = ""
        self.bot_name = ""
        self.bot_id = ""
        self.bot_running = False
        self.user_list = {}        
        self.chat_list = {}
        self.code2fa_list = {}
        self.vars = dict()
        self.Token = Token
        self.mainmenu = svcbot_menu 
        self.cmd_dict = {}
        self.keys_dict = {}
        self.job_items = {}
        self.client_name = client_name
        self.adminchatid = adminchatid
        self.keys_dict[option_mainmenu] = 1
        self.define_keys(svcbot_menu, self.keys_dict[option_mainmenu])
        self.define_keys(nlp_menu, self.keys_dict[option_nlp])
        self.define_keys(ml_menu, self.keys_dict[option_ml])
        self.define_keys(system_menu, self.keys_dict[option_syscfg])
        self.define_keys( users_menu, self.keys_dict[ option_usermgmt ])
        self.keys_dict[option_2fa] = (self.keys_dict[option_mainmenu]*10) + 1        
        #print(*(self.keys_dict).items(), sep = '\n')

        self.bot = telepot.DelegatorBot(Token, [
            pave_event_space()( [per_chat_id(), per_callback_query_chat_id()],
            create_open, MessageCounter, timeout=max_duration, include_callback_query=True),
        ])
        info = self.bot.getMe()
        self.bot_name = info['username']
        self.bot_id = info.get('id')
        print('Frontend bot : ', self.bot_id)
        msg = self.bot_name + ' started running. URL is https://t.me/' + self.bot_name
        print(msg)
        try:
            MessageLoop(self.bot).run_as_thread()
            self.bot_running = True
        except:
            print("Error running this bot instance")
        return

    def __str__(self):
        return "ServiceBot for OmniMentor Bots"

    def __repr__(self):
        return 'BotInstance()'

    def broadcast(self, msg):
        for d in self.user_list:
            self.bot.sendMessage(d, msg)

    def define_keys(self, telegram_menu, start_key):
        button_list = lambda x : str(x).replace('[','').replace(']','').replace(", ",",").replace("'","").split(',')
        menu_keys = start_key*100 + 1
        self.cmd_dict[start_key] = button_list(telegram_menu)
        for menu_item in self.cmd_dict[start_key] :
            if (menu_item != option_back) and (menu_item != ''):
                self.keys_dict[menu_item] = menu_keys 
                menu_keys += 1
        return 

class MessageCounter(telepot.helper.ChatHandler):
    def __init__(self, *args, **kwargs):
        super(MessageCounter, self).__init__(*args, **kwargs)
        self.is_admin = False
        self.chatid = 0
        self.username = ""
        self.edited = 0
        self.menu_id = 0
        self.mainmenu = []
        self.parentbot = None

    def reset(self):
        self.__init__()
        return

    def logoff(self):
        try:
            if self.chatid in self.parentbot.user_list:
                self.parentbot.user_list.pop(self.chatid)
        except:
            pass
        txt = "Have a great day!"
        bot_prompt(self.bot, self.chatid, txt, [['/start']])
        self.chatid = 0
        self.reset
        self.menu_id = 0
        return

    def on_close(self, exception):
        self.logoff()
        self.sender.sendMessage('session time out, goodbye.')
        return

    def on_chat_message(self, msg):
        global svcbot
        try:
            content_type, chat_type, chat_id = telepot.glance(msg)
            self.parentbot = svcbot
            self.mainmenu = svcbot.mainmenu            
            bot = self.bot
            self.chatid = chat_id
            keys_dict = svcbot.keys_dict
            adminchatid = svcbot.adminchatid
            client_name = svcbot.client_name
        except:
            return
        resp = ""
        retmsg = ''
        username = ""
        if content_type == 'text':
            resp = msg['text'].strip()
            if 'from' in list(msg):
                username = msg['from']['first_name']
                self.username = username
            if 'reply_to_message' in list(msg) and 'For your approval with 2FA code :' in str(msg):
                msglist = str(msg).replace('"',"'").replace("'message_id':",chr(4644)).split(chr(4644))[3].split(',')
                reply_id = int([x for x in msglist if 'from' in x ][0].split(':')[2].strip())
                if 'username'  in str(msglist):
                    req_user = [x for x in msglist if 'username' in x ][0].replace("'",'').split(':')[1].strip()
                else:
                    req_user = [x for x in msglist if 'first_name' in x ][0].replace("'",'').split(':')[1].strip()
                code2fa = [x for x in msglist if 'bot_command' in x ][0].split(' ')[-1].replace("'",'')
                txt = "Hi " + req_user + ", your 2FA code is : " + code2fa
                bot.sendMessage(reply_id,txt)
        elif (content_type=="document") :
            #if msg['document']['mime_type']=="text/plain":
            fid = msg[content_type]['file_id']
            fname = get_attachment(bot, fid)
            #fcaption = msg['document']['caption']
            file_name = msg['document']['file_name']
            pcmd = f"cp -f {fname} ~/om/{file_name} ; rm -f {fname}"
            shellcmd(pcmd)
            bot.sendMessage(chat_id, f"file {file_name} received.")
                
        elif content_type != "text":
            print( json.dumps(msg) )
            txt = "Thanks for the " + content_type + " but I do not need it for now."
            bot.sendMessage(chat_id,txt)

        if resp=='/end':
            self.is_admin = (chat_id == adminchatid)
            self.logoff()
            self.menu_id = 0
            
        #if resp=='/z':
            #print(client_name)        

        elif resp=='/stop' and (chat_id in [adminchatid, developerid]):
            self.parentbot.broadcast('System shutting down.')
            self.parentbot.bot_running = False            
            retmsg = 'System already shutdown.'
          
        elif resp == '/start':
            result ='<pre> ▀▄▀▄▀▄ OmniMentor ▄▀▄▀▄▀\n Powered by Sambaash</pre>\nContact <a href=\"tg://user?id=1064466049">@OmniMentor</a>'
            bot.sendMessage(chat_id,result,parse_mode='HTML')
            self.reset            
            if (chat_id in [adminchatid, developerid]):
                self.is_admin = True
                txt = "Welcome to the ServiceBot"
                self.menu_id = keys_dict[option_mainmenu]
                bot_prompt(bot, chat_id, txt, self.mainmenu)
                self.parentbot.user_list[chat_id] = [self.username, ""]
            else:
                self.is_admin = False
                txt = "Following user requesting for admin access :\n"
                txt += f"Command of request : {resp} \n\n"
                txt += json.dumps(msg)
                code2FA = ''.join(random.choice( "ABCDEFGHJKLMNPQRTUVWXY0123456789" ) for i in range(32))
                code2FA = code2FA.upper()
                self.parentbot.code2fa_list[chat_id] = code2FA
                txt += "\n\nFor your approval with 2FA code : " + code2FA
                bot.sendMessage(adminchatid, txt)
                txt = "Please enter the 2FA code :"
                bot_prompt(bot, chat_id, txt, [])
                self.menu_id = keys_dict[option_2fa]
        elif self.menu_id == keys_dict[option_mainmenu]:
            if resp == option_nlp :
                txt = "This section maintain NLP corpus and trains model.\n"
                txt += "You can test your NLP dialog from here."
                txt = banner_msg("NLP", txt)
                bot_prompt(bot, chat_id, txt, nlp_menu)
                self.menu_id = keys_dict[option_nlp]
            elif resp == option_ml :                
                txt = 'You are in ML options.'
                bot_prompt(self.bot, self.chatid, txt, ml_menu)
                self.menu_id = keys_dict[option_ml]
            elif resp == option_syscfg :
                txt = banner_msg("System Mode", "This section updates master data at the background")
                bot_prompt(bot, chat_id, txt, system_menu)
                self.menu_id = keys_dict[option_syscfg]
            elif resp == option_usermgmt:                
                txt = 'To search for student-ID or reset User by student-ID.'
                bot_prompt(self.bot, self.chatid, txt, users_menu)
                self.menu_id = keys_dict[option_usermgmt]
            elif resp == option_cmd :                    
                txt = "You are now connected to Cmd mode.\nType cmd to list out the commands."
                txt = banner_msg("Service Console", txt)
                bot_prompt(bot, chat_id, txt, [[option_back]])
                self.menu_id = keys_dict[option_cmd]
            elif resp == option_back :
                self.logoff()
                
        elif self.menu_id == keys_dict[option_nlp]:
            retmsg = "The section handle all the natural language processing matters."
            if resp == nlp_dict :
                html_table(bot, chat_id, "", "dictionary", "DICTIONARY TABLE", "dictionary.html")
            elif resp == nlp_prompts :
                html_table(bot, chat_id, "", "prompts", "RESPONSES TABLE", "prompts.html")
            elif resp == nlp_response :
                html_table(bot, chat_id, "", "progress", "RESPONSE TEXT", "response.html")
            elif resp == nlp_corpus  :
                html_table(bot, chat_id, "", "ft_corpus", "FASTTEXT CORPUS", "ft_corpus.html")
            elif resp == nlp_stopwords :
                html_table(bot, chat_id, "", "stopwords", "STOPWORDS TABLE", "stopwords.html")
            elif resp == nlp_faq:
                html_table(bot, chat_id, "", "faq", "FAQ TABLE", "faq.html")
            elif resp == nlp_train :
                if omchat.train_model() :
                    retmsg = "NLP model using the corpus table has been trained with model file saved as ft_model.bin"
                else:
                    retmsg = "NLP model using the corpus table was not trained properly"
            elif (resp == option_back) or (resp == "0"):
                bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                self.menu_id = keys_dict[option_mainmenu]

        elif self.menu_id == keys_dict[option_ml]:
            txt = "The section handle all the machine learning related processes."
            self.sender.sendMessage(txt)
            if resp == ml_data :
                html_table(bot, chat_id, client_name, "mcqas_info", "ML Model Data", "mcqas_info.html")
            elif resp == ml_pipeline :
                botname = (self.bot.getMe())['username']
                func_req = "generate_mcq_as"
                func_param = ""
                job_request(botname,chat_id, client_name, func_req,func_param)
                retmsg = 'Sending request edx_bot'
            elif resp == ml_report :
                retmsg = "Generating profiler report for quick data analysis."
                fn="mcqas_info.html"
                if profiler_report(client_name, fn)==1:                    
                    bot.sendDocument(chat_id=self.chatid, document=open(fn, 'rb'))
                else:
                    result = "<a href=\"https://omnimentor.lithan.com/prod/mcqas_info.html\">Profiler Report</a>"
                    bot.sendMessage(chat_id,result,parse_mode='HTML')
            elif resp == ml_graph  :
                retmsg = "Generating decision tree graph to explain the model."
                fn = 'mcqas_info.jpg'
                use_neural_network = False # True or False
                if use_neural_network:                    
                    nn_model.plot_graph(fn)
                    status = 1
                else:
                    status = dt_model.tree_graph(fn)
                if status==1:
                    f = open(fn, 'rb')
                    bot.sendPhoto(self.chatid, f)
            elif resp == ml_train :                
                use_neural_network = False # or False
                if use_neural_network:
                    retmsg = nn_model.train_model(client_name, "ffnn_model.hdf5")
                else:
                    retmsg = dt_model.train_model(client_name, "dt_model.bin")
            elif (resp == option_back) or (resp == "0"):
                bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                self.menu_id = keys_dict[option_mainmenu]

        elif self.menu_id == keys_dict[option_syscfg] :
            if resp == option_back :
                bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                self.menu_id = keys_dict[option_mainmenu]
            elif resp == sys_params:
                html_table(bot, chat_id, client_name, "params", "System Parameters", "system_params.html")
            elif resp == sys_logs:
                html_table(bot, chat_id, "", "syslog", "System Logs", "system_logs.html")
            elif resp == sys_jobs :
                html_table(bot, chat_id, client_name, "job_list", "System joblist", "joblist.html")
            elif resp == stage_master :                
                html_table(bot, chat_id, client_name, "stages_master", "Schedule Template", "stages_master.html")
            elif resp == option_client :
                df = rds_df("select distinct client_name from user_master order by client_name;")
                if df is None:
                    retmsg = "Information not available"
                else:
                    df.columns = ['client_name']
                    client_list = [x for x in df.client_name if x not in ['Sambaash','Demo']] + [option_back]
                    bot_prompt(bot, chat_id, "Client copy to Sambaash client from :", [client_list])
                    self.menu_id = keys_dict[option_client]
                                    
        elif self.menu_id == keys_dict[option_2fa]:
            code2FA = self.parentbot.code2fa_list[chat_id]
            if code2FA == resp:
                self.is_admin = True
                txt = banner_msg("Welcome","You are now connected to Mentor mode.")
                self.menu_id = keys_dict[option_mainmenu]
                bot_prompt(bot, chat_id, txt, self.mainmenu)
                self.parentbot.code2fa_list.pop(chat_id)
                self.parentbot.user_list[chat_id] = [self.username, ""]
            else:
                txt  = "Sorry the 2FA code is invalid, please try again."
                self.is_admin = False
                bot_prompt(bot, chat_id, txt, [['/start']])
                self.menu_id = 0

        elif self.menu_id ==  keys_dict[option_usermgmt]:
            if (resp == option_back) or (resp == "0"):
                bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                self.menu_id = keys_dict[option_mainmenu]
            if resp == option_searchbyname:
                txt = "Search Student-ID by name"
                bot_prompt(self.bot, self.chatid, txt, [[option_back]])
                self.menu_id = keys_dict[option_searchbyname]                
            elif resp == option_searchbyemail:
                txt = "Search Student-ID by email"                
                bot_prompt(self.bot, self.chatid, txt, [[option_back]])
                self.menu_id = keys_dict[option_searchbyemail]                
            elif resp == option_resetuser:
                txt = "Please enter valid Student-ID :"
                bot_prompt(self.bot, self.chatid, txt, [[option_back]])
                self.menu_id = keys_dict[option_resetuser]                
            elif resp == option_admin_users:
                query = f"select studentid,username,email from user_master where client_name = '{client_name}' and usertype=11 limit 50;"
                result = "List of admin users (top 50)\n"
                df = rds_df(query)
                if df is None:
                    self.sender.sendMessage("Sorry, no results found.")
                    return
                df.columns = ['studentid','username','email']
                html_list(self.bot, chat_id, df, df.columns, [10,30,40], result, 25)
                return                
            elif resp == option_blocked_users:
                query = f"select studentid,username,email from user_master where client_name = '{client_name}' and usertype=0 limit 50;"
                result = "List of blocked users (top 50)\n"
                df = rds_df(query)
                if df is None:
                    self.sender.sendMessage("Sorry, no results found.")
                    return
                df.columns = ['studentid','username','email']
                html_list(self.bot, chat_id, df, df.columns, [10,30,40], result, 25)
                return
            elif resp == option_binded_users :
                query = f"UPDATE user_master SET binded=0 WHERE chat_id=0 and client_name = '{client_name}';"
                rds_update(query)
                query = f"select studentid,username,email,chat_id from user_master where client_name = '{client_name}' and binded=1 limit 50;"
                result = "List of binded users (top 50)\n"
                df = rds_df(query)
                if df is None:
                    self.sender.sendMessage("Sorry, no results found.")
                    return
                df.columns = ['studentid','username','email','chat_id']
                html_list(self.bot, chat_id, df, df.columns, [10,30,40,20], result, 25)
                return
            elif resp == option_active_users :
                query = f"select studentid,username,email,chat_id from user_master where client_name = '{client_name}' and usertype>0 limit 50;"
                result = "List of active users (top 50)\n"
                df = rds_df(query)
                if df is None:
                    self.sender.sendMessage("Sorry, no results found.")
                    return
                df.columns = ['studentid','username','email','chat_id']
                html_list(self.bot, chat_id, df, df.columns, [10,30,40,20], result, 25)
                return

        elif self.menu_id in [keys_dict[option_searchbyname],keys_dict[option_searchbyemail]] :
            if (resp == option_back) or (resp == "0"):
                txt = "You are back to the user management menu."
                bot_prompt(self.bot, self.chatid, txt, users_menu)
                self.menu_id = keys_dict[option_usermgmt]
                return
            idx = [keys_dict[option_searchbyname],keys_dict[option_searchbyemail]].index(self.menu_id)
            resptxt = resp.lower().strip()
            if idx==0:
                query = f"select studentid,username,email from user_master where client_name = '{client_name}' and lower(username) like '%{resptxt}%' ;"
                result = "Name search matching " + resp + "\n"
            if idx==1:
                query = f"select studentid,username,email from user_master where client_name = '{client_name}' and lower(email) like '%{resptxt}%' ;"
                result = "Email search matching " + resp + "\n"
            df = rds_df(query)
            if df is None:
                self.sender.sendMessage("Sorry, no results found.")
                return
            df.columns = ['studentid','username','email']
            html_list(self.bot, chat_id, df, df.columns, [10,30,40], result, 20)
            return
            
        elif self.menu_id ==  keys_dict[option_resetuser]:            
            if (resp == option_back) or (resp == "0"):
                txt = "You are back to the user management menu."
                bot_prompt(self.bot, self.chatid, txt, users_menu)
                self.menu_id = keys_dict[option_usermgmt]
                return                            
            sid = 0
            opt_blockuser = 'Block this user'
            opt_setadmin = 'Set as Admin'
            opt_setlearner = 'Set as Learner'
            opt_resetemail = 'Change Email'
            opt_unbind = 'Reset Binding'            
            if resp.isnumeric():
                sid = int(resp)
                if sid > 0:
                    query = f"select * from user_master where client_name = '{client_name}' and studentid = {str(sid)} ;"
                    df = rds_df(query)
                    if df is None:
                        sid = 0
                if sid == 0:
                    retmsg = "Unable to find the matchnig record. Please try again."
                else:
                    df.columns = get_columns("user_master")
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
                    query += f" a.client_name = '{client_name}' and a.studentid={sid} " 
                    query += "order by a.courseid;"                  
                    df = rds_df(query)
                    if df is not None:
                        df.columns = ['courseid']
                        if len(df)>0:
                            txt += "Courses:\n" + '\n'.join([x for x in df.courseid])
                    txt += "\nWhat would you like to do ?"
                    useraction_menu = [[opt_blockuser, opt_setadmin , opt_setlearner],[opt_resetemail, opt_unbind, option_back]]
                    bot_prompt(self.bot, self.chatid, txt, useraction_menu)
            elif resp == opt_blockuser:
                query = f"update user_master set usertype = 0 where client_name = '{client_name}' and studentid = {self.student_id} ;"
                rds_update(query)
                retmsg = f"User with Student-ID {self.student_id} has been blocked."
            elif resp == opt_setadmin:
                query = f"update user_master set usertype = 11 where client_name = '{client_name}' and studentid = {self.student_id} ;"
                rds_update(query)
                retmsg = f"User with Student-ID {self.student_id} has been set as admin."
            elif resp == opt_setlearner:
                query = f"update user_master set usertype = 1 where client_name = '{client_name}' and studentid = {self.student_id} ;"
                rds_update(query)
                retmsg = f"User with Student-ID {self.student_id} has been set as learner."
            elif resp == opt_resetemail:
                query = f"update user_master set email = '{resp}' where client_name = '{client_name}' and studentid = {self.student_id} ;"
                rds_update(query)
                retmsg = f"User email with Student-ID {self.student_id} has been set to {self.student_id}."
            elif resp == opt_unbind:
                query = f"update user_master set binded = 0, chat_id = 0 where client_name = '{client_name}' and studentid = {self.student_id} ;"
                rds_update(query)
                retmsg = f"User telegram account has been unbinded from Student-ID {self.student_id}."

        elif self.menu_id == keys_dict[option_client]:
            #svcbot.client_name = client_name = resp            
            #bot_prompt(bot, chat_id, f"Default client_name set to {resp}", system_menu)
            if resp != option_back:                
                self.sender.sendMessage(f"Copying from {resp} to Sambaash....")
                table_list = ['course_module' , 'iu_stages', 'mcqas_info', 'mcq_data', 'mcq_score']
                table_list += ['module_iu' , 'playbooks', 'stages', 'stages_master', 'userdata' , 'user_master']                
                for tbl in table_list:
                    #self.sender.sendMessage(f"Duplicating data for table {tbl}....")
                    #print(f"Duplicating data for table {tbl}....")
                    qry =  f"select * from {tbl} WHERE client_name = '{resp}';"
                    df = rds_df(qry)
                    if df is not None:
                        df.columns = get_columns(tbl)
                        df['client_name'] = 'Sambaash'
                        qry = f"DELETE FROM {tbl} WHERE client_name = 'Sambaash';"
                        rds_update(qry)
                        copydbtbl(df, tbl)  
                self.sender.sendMessage("Client copy  has been completed.")
            bot_prompt(bot, chat_id, "You are back in the main menu", system_menu)
            self.menu_id = keys_dict[option_syscfg]
            
        elif self.menu_id == keys_dict[option_cmd]:
            if resp == option_back :
                bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                self.menu_id = keys_dict[option_mainmenu]
            else:
                txt = shellcmd(resp)
                txt_list = txt.split('\n')
                cnt = int((len(txt_list)+19)/20)
                for n in range(cnt):
                    m = n*20
                    result = '\n'.join(txt_list[m:][:20])
                    result = '<pre>' + result + '</pre>'
                    bot.sendMessage(chat_id, result, parse_mode='HTML')

        while len(retmsg) > 0:
            txt = retmsg[:100]
            retmsg = retmsg[100:]
            #txt = retmsg[:4000]
            #retmsg = retmsg[4000:]
            self.sender.sendMessage(txt)
            retmsg=""
        return

def profiler_report(client_name, output_file):
    try:
        ok = 1
        cols = ['grade', 'mcq_avgscore', 'mcq_cnt', 'as_avgscore'] 
        mcqinfo = rds_df( f"SELECT grade,mcq_avgscore,mcq_cnt,as_avgscore FROM mcqas_info WHERE client_name='{client_name}';")
        if mcqinfo is None:        
            ok = 0
            return
        else:
            mcqinfo.columns = cols
        features = ['mcq_avgscore', 'mcq_cnt', 'as_avgscore']        
        df = mcqinfo[cols]
        pf = ProfileReport(df)
        pf.to_file(output_file)
    except:
        ok = 0
    return ok

def checkjoblist(svcbot):
    result = rds_param("select count(*) as cnt from job_list where status='open';")
    cnt = int("0" + str(result))
    if cnt == 0:
        return
    df = rds_df("select * from job_list where status='open' and client_name='System' order by date,time_start limit 1;")    
    if df is None:
        return
    df.columns = get_columns("job_list")
    jobitem = df.iloc[0].to_dict()
    svcbot.job_items = jobitem
    msg =  runbotjob(svcbot)
    if msg == "":
        msg = "job complete complete"
    time_now = time.strftime('%H%M%S', time.localtime() )
    time_end = str(time_now)
    status = "completed"
    job_id = jobitem['job_id']
    updqry = f"update job_list set time_end = '{time_end}', status = '{status}', message = '{msg}' where job_id = '{job_id}';"
    rds_update(updqry)
    return

def runbotjob(svcbot):
    global edx_api_header, edx_api_url    
    adminchatid = svcbot.adminchatid
    jobitem = svcbot.job_items
    job_id = jobitem['job_id']
    client_name = jobitem['client_name']
    chat_id = jobitem['chat_id']
    bot_req = jobitem['bot_req']
    func_req = jobitem['func_req']
    func_param = jobitem['func_param']
    func_svc_list = ["update_assignment" , "update_mcq" , "edx_import", "update_schedule"]
    edx_time = edx_load_config(client_name)
    txt = "job "
    updqry = f"update job_list set status = 'running', message = '' where job_id = '{job_id}';"
    rds_update(updqry)
    jobitem['status'] = 'running'
    #svcbot.bot.sendMessage(adminchatid,f"running job {job_id}")
    #print(f"running job {job_id}")            
    if func_req == "generate_mcq_as":
        try:
            vmedxlib.generate_mcq_as(func_param)
            txt += " completed successfully."
        except:
            txt += " failed."               
    elif func_req in ["edx_mass_import", "mass_update_assignment", "mass_update_mcq", "mass_update_schedule", "mass_update_usermaster"]:
        try:
            func_svc = "vmedxlib." + func_req + "(client_name)"
            status = eval(func_svc)
            txt += " completed successfully."
        except:
            txt += " failed."
    elif func_req in func_svc_list :
        course_id = func_param              
        try:
            func_svc = "vmedxlib." + func_req + "(course_id, client_name)"
            status = eval(func_svc)
            txt += " completed successfully."
        except:
            txt += " failed."
    else:
        try:
            func_svc = "vmedxlib." + func_req + "(" + func_param + ")"
            status = eval(func_svc)
            if status:
                txt += " completed successfully."
            else:
                txt += " failed."
        except:
            txt += " failed."
    try:        
        svcbot.bot.sendMessage(adminchatid,f"completed job {job_id}")
    except:
        print(f"completed job {job_id}")
    jobitem['status'] = 'completed'
    svcbot.job_items = {}
    return txt

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

def edx_load_config(client_name):
    global edx_api_header, edx_api_url    
    # load client config from RDS
    df = rds_df(f"select * from params where client_name = '{client_name}';")
    if df is None:
        return 0
    df.columns = get_columns("params")
    par_val = ['' + str(x) for x in df.value]
    par_key = [x for x in df.key]
    par_dict = dict(zip(par_key, par_val))
    edx_time = int(par_dict['edx_import'])    
    hdr = par_dict['edx_api_header']
    #hdr= eval(hdr)
    edx_api_header = eval(hdr)
    edx_api_url = par_dict['edx_api_url']
    edx_api_header = edx_api_header
    edx_api_url = edx_api_url
    vmedxlib.edx_api_url = edx_api_url
    vmedxlib.edx_api_header = edx_api_header
    return edx_time

def do_main():
    global svcbot, edx_api_header, edx_api_url    
    err = 0    
    # load system config from RDS
    vmsvclib.rds_connstr = ""
    vmsvclib.rdscon = None
    df = rds_df("select * from params where client_name = 'System';")
    if df is None:
        print("unable to proceed, params table not found")
        return        
    df.columns = get_columns("params")
    par_val = ['' + str(x) for x in df.value]
    par_key = [x for x in df.key]
    par_dict = dict(zip(par_key, par_val))
    SvcBotToken = par_dict['ServiceBot']
    adminchatid = int(par_dict['adminchatid'])
    client_name = par_dict['client_name']
    gmt = int(par_dict['GMT'])
    #max_duration = int(par_dict['max_duration'])
    max_duration = 300
    
    edx_time = edx_load_config(client_name)
    
    omchat.load_modelfile("ft_model.bin", client_name)
    dt_model.load_model("dt_model.bin")
    nn_model.model_loader("ffnn_model.hdf5")
    svcbot = BotInstance(SvcBotToken, client_name, max_duration, adminchatid) 
    print(svcbot)
    #svcbot.bot.sendMessage(adminchatid, "Click /start to connect the ServiceBot")
    #edx_cnt = 0
    while svcbot.bot_running:  
        time.sleep(3)
    try:
        os.kill(os.getpid(), 9)
    except:
        err = 1
    return


#------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    version = sys.version_info
    if version.major == 3 and version.minor >= 7:
        do_main()
    else:
        print("Unable to use this version of python\n", version)
    
