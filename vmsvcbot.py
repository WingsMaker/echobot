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
import asyncio
import telepot.aio
from telepot.aio.loop import MessageLoop
from telepot.aio.delegate import pave_event_space, per_chat_id, create_open

import vmedxlib
import vmnlplib
import vmaiglib
import vmffnnlib
import vmsvclib
from vmsvclib import *

global svcbot, bot_intance, edx_api_header, edx_api_url

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
option_client = "Clients"
option_usermgmt = "Manage Users 👥"
svcbot_menu = [[option_nlp, option_ml, option_syscfg], [option_usermgmt, option_client, option_back]]
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
option_searchbyname = "Name Search"
option_searchbyemail = "Email Search"
option_resetuser = "Reset User"
option_admin_users = "Admin Users"
option_binded_users = "Binded Users"
option_active_users = "Active Users"
option_blocked_users = "Blocked Users"
users_menu = [[option_searchbyname, option_searchbyemail, option_resetuser, option_active_users], \
    [option_admin_users, option_binded_users, option_blocked_users, option_back]]
opt_blockuser = 'Block this user'
opt_setadmin = 'Set as Admin'
opt_setlearner = 'Set as Learner'
opt_resetemail = 'Change Email'
opt_unbind = 'Reset Binding'
useraction_menu = [[opt_blockuser, opt_setadmin , opt_setlearner],[opt_resetemail, opt_unbind, option_back]]
sys_params = "System Parameters"
sys_pyt = "Python Shell 🐍"
sys_cmd = "Commands Shell 📺"
system_menu = [[sys_params, sys_pyt, sys_cmd, option_back]]
sys_clientcopy = "Client Copy"
opt_clientcopy = "Target Client"
sys_clientname = "Default Client"
client_menu = [[sys_clientname, sys_clientcopy, option_back]]

piece = lambda txtstr,seperator,pos : txtstr.split(seperator)[pos]
string2date = lambda x,y : datetime.datetime.strptime(x,y).date()
str2date = lambda x : string2date(x,"%d/%m/%Y")
date_today = datetime.datetime.now().date()
dmy_str = lambda v : piece(v,'-',2) + '/' + piece(v,'-',1) + '/' + piece(v,'-',0)
ymd_str = lambda v : piece(v,'-',0) + '/' + piece(v,'-',1) + '/' + piece(v,'-',2)

class BotInstance():
    def __init__(self, Token, client_name, max_duration, adminchatid):
        global svcbot, bot_intance
        bot_intance = self
        self.Token = ""
        self.bot_name = ""
        self.bot_id = ""
        self.user_list = {}
        self.chat_list = {}
        self.code2fa_list = {}
        self.vars = dict()
        self.Token = Token
        self.mainmenu = svcbot_menu
        self.cmd_dict = {}
        self.keys_dict = {}
        self.job_items = {}
        self.clearstop = True
        self.client_name = client_name
        self.adminchatid = adminchatid
        self.keys_dict[option_mainmenu] = 1
        self.define_keys(svcbot_menu, self.keys_dict[option_mainmenu])
        self.define_keys(nlp_menu, self.keys_dict[option_nlp])
        self.define_keys(ml_menu, self.keys_dict[option_ml])
        self.define_keys(system_menu, self.keys_dict[option_syscfg])
        self.define_keys(users_menu, self.keys_dict[option_usermgmt])
        self.define_keys(useraction_menu, self.keys_dict[option_resetuser])
        self.keys_dict[option_2fa] = (self.keys_dict[option_mainmenu]*10) + 1
        self.define_keys( client_menu, self.keys_dict[option_client])
        self.keys_dict[opt_clientcopy] = (self.keys_dict[option_client]*10) + 1        
        self.bot = telepot.aio.DelegatorBot(Token, [
            pave_event_space()( per_chat_id(),
            create_open, MessageCounter, timeout=max_duration),     
        ])        
        svcbot = self.bot
        self.loop = None
        return

    def __str__(self):
        return "ServiceBot for OmniMentor Bots"

    def __repr__(self):
        return 'BotInstance()'

    def define_keys(self, telegram_menu, start_key):
        button_list = lambda x : str(x).replace('[','').replace(']','').replace(", ",",").replace("'","").split(',')
        menu_keys = start_key*100 + 1
        self.cmd_dict[start_key] = button_list(telegram_menu)
        for menu_item in self.cmd_dict[start_key] :
            if (menu_item != option_back) and (menu_item != ''):
                self.keys_dict[menu_item] = menu_keys 
                menu_keys += 1
        return 
        

class MessageCounter(telepot.aio.helper.ChatHandler):
    def __init__(self, *args, **kwargs):
        super(MessageCounter, self).__init__(*args, **kwargs)
        self.is_admin = False
        self.chatid = 0
        self.username = ""
        self.edited = 0
        self.menu_id = 0
        self.mainmenu = []
        self.vars = dict()

    def reset(self):
        self.__init__()
        return

    async def logoff(self):
        try:
            if self.chatid in bot_intance.user_list:
                bot_intance.user_list.pop(self.chatid)
        except:
            pass
        txt = "Have a great day!"        
        await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup([['/start']]))        
        self.chatid = 0
        self.reset
        self.menu_id = 0
        return

    async def on_close(self, exception):
        txt = 'session time out, goodbye.\nPress /start to reconnect.'        
        await self.sender.sendMessage(txt)
        await self.logoff()
        return

    def reply_markup(self, buttons=[], opt_resize = True):
        mark_up = None
        if buttons == []:
            mark_up = {'hide_keyboard': True}
        else:
            mark_up = ReplyKeyboardMarkup(keyboard=buttons,one_time_keyboard=True,resize_keyboard=opt_resize)
        return mark_up

    async def on_chat_message(self, msg):
        global svcbot, bot_intance
        try:
            content_type, chat_type, chat_id = telepot.glance(msg)
            bot = svcbot            
            self.mainmenu = bot_intance.mainmenu
            self.chatid = chat_id
            keys_dict = bot_intance.keys_dict
            adminchatid = bot_intance.adminchatid
            client_name = bot_intance.client_name
        except:
            return
        resp = ""
        retmsg = ''
        html_msglist = []
        html_title = ""
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
                await bot.sendMessage(reply_id,txt)
        elif (content_type=="document") :
            try:
                fid = msg['document']['file_id']
                fpdic = await bot.getFile(fid)
                fpath = fpdic['file_path']
                file_name = msg['document']['file_name']
                fn = "https://api.telegram.org/file/bot" + bot._token + "/"  + fpath
                fname = wget.download(fn)
                pcmd = f"cp -f {fname} ./om/{file_name} ; rm -f {fname}"
                shellcmd(pcmd)
                await bot.sendMessage(chat_id, f"file {file_name} received.")
            except:
                return                
        elif content_type != "text":
            print( json.dumps(msg) )
            txt = "Thanks for the " + content_type + " but I do not need it for now."
            await bot.sendMessage(chat_id,txt)

        if resp=='/end':
            self.is_admin = (chat_id == adminchatid)
            await self.logoff()
            self.menu_id = 0
            txt = "sesson closed."
            await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup([['/start']]))
            
        elif resp=='/stop' and (chat_id in [adminchatid, developerid]):
            if bot_intance.clearstop:                
                return
            for d in bot_intance.user_list:
                await self.bot.sendMessage(d, "System shutting down.")
            await self.bot.sendMessage(chat_id,'System already shutdown.')
            bot_intance.loop.stop()
          
        elif (chat_id in [adminchatid, developerid]) and resp.startswith('/!'):
            if len(resp) > 3:
                try:
                    fn = resp[3:]
                    await bot.sendDocument(chat_id=self.chatid, document=open(fn, 'rb'))
                except:
                    return
        elif resp == '/start':
            bot_intance.clearstop = False
            result ='<pre> ▀▄▀▄▀▄ OmniMentor ▄▀▄▀▄▀\n Powered by Sambaash</pre>\nContact <a href=\"tg://user?id=1064466049">@OmniMentor</a>'
            await bot.sendMessage(chat_id,result,parse_mode='HTML')
            self.reset            
            if (chat_id in [adminchatid, developerid]):
                self.is_admin = True
                txt = "Welcome to the ServiceBot"
                self.menu_id = keys_dict[option_mainmenu]
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.mainmenu))
                bot_intance.user_list[chat_id] = [self.username, ""]
            else:
                self.is_admin = False
                txt = "Following user requesting for admin access :\n"
                txt += f"Command of request : {resp} \n\n"
                txt += json.dumps(msg)
                code2FA = ''.join(random.choice( "ABCDEFGHJKLMNPQRTUVWXY0123456789" ) for i in range(32))
                code2FA = code2FA.upper()
                bot_intance.code2fa_list[chat_id] = code2FA
                txt += "\n\nFor your approval with 2FA code : " + code2FA
                await self.bot.sendMessage(adminchatid, txt)
                txt = "Please enter the 2FA code :"
                await self.bot.sendMessage(chat_id, txt)
                self.menu_id = keys_dict[option_2fa]
        elif self.menu_id == keys_dict[option_mainmenu]:
            if resp == option_nlp :
                txt = "This section maintain NLP corpus and trains model.\n"
                txt += "You can test your NLP dialog from here."
                txt = banner_msg("NLP", txt)                
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(nlp_menu))
                self.menu_id = keys_dict[option_nlp]
            elif resp == option_ml :                
                txt = 'You are in ML options.'
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(ml_menu))
                self.menu_id = keys_dict[option_ml]
            elif resp == option_syscfg :
                txt = banner_msg("System Mode", "This section updates master data at the background")                
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(system_menu))
                self.menu_id = keys_dict[option_syscfg]
            elif resp == option_usermgmt:                
                txt = 'To search for user or reset the user.'
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(users_menu))
                self.menu_id = keys_dict[option_usermgmt]
            elif resp == option_client :
                txt = "Current client is " + client_name 
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(client_menu))
                self.menu_id = keys_dict[option_client]
            elif resp == option_back :
                await self.logoff()
                txt = "sesson closed."
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup([['/start']]))
                
        elif self.menu_id == keys_dict[option_nlp]:
            if resp == nlp_dict :
                f=html_tbl("", "dictionary", "DICTIONARY TABLE", "dictionary.html")
                if f is None:
                    await self.bot.sendMessage( chat_id, "Information not available" )
                else:
                    await self.bot.sendDocument(chat_id, document=f)
            elif resp == nlp_prompts :
                f=html_tbl("", "prompts", "RESPONSES TABLE", "prompts.html")
                if f is None:
                    await self.bot.sendMessage( chat_id, "Information not available" )
                else:
                    await self.bot.sendDocument(chat_id, document=f)
            elif resp == nlp_response :
                f=html_tbl("", "progress", "RESPONSE TEXT", "response.html")
                if f is None:
                    await self.bot.sendMessage( chat_id, "Information not available" )
                else:
                    await self.bot.sendDocument(chat_id, document=f)
            elif resp == nlp_corpus  :
                f=html_tbl("", "ft_corpus", "FASTTEXT CORPUS", "ft_corpus.html")
                if f is None:
                    await self.bot.sendMessage( chat_id, "Information not available" )
                else:
                    await self.bot.sendDocument(chat_id, document=f)
            elif resp == nlp_stopwords :
                f=html_tbl("", "stopwords", "STOPWORDS TABLE", "stopwords.html")
                if f is None:
                    await self.bot.sendMessage( chat_id, "Information not available" )
                else:
                    await self.bot.sendDocument(chat_id, document=f)
            elif resp == nlp_faq:
                f=html_tbl("", "faq", "FAQ TABLE", "faq.html")
                if f is None:
                    await self.bot.sendMessage( chat_id, "Information not available" )
                else:
                    await self.bot.sendDocument(chat_id, document=f)
            elif resp == nlp_train :
                if omchat.train_model() :
                    retmsg = "NLP model using the corpus table has been trained with model file saved as ft_model.bin"
                else:
                    retmsg = "NLP model using the corpus table was not trained properly"
            elif (resp == option_back) or (resp == "0"):
                txt = "You are back in the main menu"
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.mainmenu))
                self.menu_id = keys_dict[option_mainmenu]

        elif self.menu_id == keys_dict[option_ml]:
            txt = "The section handle all the machine learning related processes."
            await self.sender.sendMessage(txt)
            if resp == ml_data :
                f=html_tbl(client_name, "mcqas_info", "ML Model Data", "mcqas_info.html")
                if f is None:
                    await self.bot.sendMessage( chat_id, "Information not available" )
                else:
                    await self.bot.sendDocument(chat_id, document=f)
            elif resp == ml_pipeline :
                await self.bot.sendMessage(chat_id, 'please wait for a moment.')
                vmedxlib.generate_mcq_as(client_name)
                retmsg = 'Job completed.'                
            elif resp == ml_report :
                retmsg = "Generating profiler report for quick data analysis."
                await self.bot.sendMessage(chat_id, 'please wait for a moment.')
                fn="mcqas_info.html"
                if profiler_report(client_name, fn)==1:                    
                    await bot.sendDocument(chat_id=self.chatid, document=open(fn, 'rb'))
                else:
                    result = "<a href=\"https://omnimentor.lithan.com/om/mcqas_info.html\">Profiler Report</a>"
                    await bot.sendMessage(chat_id,result,parse_mode='HTML')
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
                    await bot.sendPhoto(self.chatid, f)
            elif resp == ml_train :                
                use_neural_network = False # or False
                if use_neural_network:
                    retmsg = nn_model.train_model(client_name, "ffnn_model.hdf5")
                else:
                    retmsg = dt_model.train_model(client_name, "dt_model.bin")
            elif (resp == option_back) or (resp == "0"):
                txt = "You are back in the main menu"
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.mainmenu))
                self.menu_id = keys_dict[option_mainmenu]

        elif self.menu_id == keys_dict[option_syscfg] :
            if resp == option_back :
                txt = "You are back in the main menu"
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.mainmenu))
                self.menu_id = keys_dict[option_mainmenu]
            elif resp == sys_params:
                f=html_tbl(client_name, "params", "System Parameters", "system_params.html")
                if f is None:
                    await self.bot.sendMessage( chat_id, "Information not available" )
                else:
                    await self.bot.sendDocument(chat_id, document=f)
            elif resp == sys_pyt :
                txt = "You are now connected to python shell mode."
                txt = banner_msg("Service Console", txt)
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup([[option_back]]))
                self.menu_id = keys_dict[sys_pyt]
            elif resp == sys_cmd :
                txt = "You are now connected to command shell mode."
                txt = banner_msg("Service Console", txt)
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup([[option_back]]))
                self.menu_id = keys_dict[sys_cmd]
                                    
        elif self.menu_id == keys_dict[option_2fa]:
            code2FA = bot_intance.code2fa_list[chat_id]
            if code2FA == resp:
                self.is_admin = True
                txt = banner_msg("Welcome","You are now connected to Mentor mode.")
                self.menu_id = keys_dict[option_mainmenu]
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.mainmenu))
                bot_intance.code2fa_list.pop(chat_id)
                bot_intance.user_list[chat_id] = [self.username, ""]
            else:
                txt  = "Sorry the 2FA code is invalid, please try again."
                self.is_admin = False
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup([['/start']]))
                self.menu_id = 0

        elif self.menu_id ==  keys_dict[option_usermgmt]:
            if (resp == option_back) or (resp == "0"):
                txt = "You are back in the main menu"
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.mainmenu))
                self.menu_id = keys_dict[option_mainmenu]
            if resp == option_searchbyname:
                txt = "Search Student-ID by name"
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup([[option_back]]))
                self.menu_id = keys_dict[option_searchbyname]
            elif resp == option_searchbyemail:
                txt = "Search Student-ID by email"                
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup([[option_back]]))
                self.menu_id = keys_dict[option_searchbyemail]                
            elif resp == option_resetuser:
                txt = "Please enter valid Student-ID :"
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup([[option_back]]))
                self.menu_id = keys_dict[option_resetuser]                
            elif resp == option_admin_users:
                query = f"select studentid,username,email from user_master where client_name = '{client_name}' and usertype=11 limit 50;"
                result = "List of admin users (top 50)\n"
                df = rds_df(query)
                if df is None:
                    await self.sender.sendMessage(txt)
                    return
                df.columns = ['studentid','username','email']
                html_title = result
                html_msglist = html_report(df, df.columns, [10,30,40], 25)                
            elif resp == option_blocked_users:
                query = f"select studentid,username,email from user_master where client_name = '{client_name}' and usertype=0 limit 50;"
                result = "List of blocked users (top 50)\n"
                df = rds_df(query)
                if df is None:
                    await self.sender.sendMessage("Sorry, no results found.")
                    return
                df.columns = ['studentid','username','email']
                html_title = result
                html_msglist = html_report(df, df.columns, [10,30,40], 25)                
            elif resp == option_binded_users :
                query = f"UPDATE user_master SET binded=0 WHERE chat_id=0 and client_name = '{client_name}';"
                rds_update(query)
                query = f"select studentid,username,email,chat_id from user_master where client_name = '{client_name}' and binded=1 limit 50;"
                result = "List of binded users (top 50)\n"
                df = rds_df(query)
                if df is None:
                    await self.sender.sendMessage("Sorry, no results found.")
                    return
                df.columns = ['studentid','username','email','chat_id']
                html_title = result
                html_msglist = html_report(df, df.columns, [10,30,40,20], 25)
            elif resp == option_active_users :
                html_title = "List of acive users"
                body = ""
                gap = ' '*30
                for u in bot_intance.user_list:
                    body += (bot_intance.user_list[u][0] + gap)[:30] + (str(u) + gap)[:15] + '\n'                
                html_msglist = [body]
        elif self.menu_id in [keys_dict[option_searchbyname],keys_dict[option_searchbyemail]] :
            if (resp == option_back) or (resp == "0"):
                txt = "You are back to the user management menu."
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(users_menu))
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
                await self.sender.sendMessage("Sorry, no results found.")
                return
            df.columns = ['studentid','username','email']
            html_title = result
            html_msglist = html_report(df, df.columns, [10,30,40], 20)                        
        elif self.menu_id ==  keys_dict[option_resetuser]:            
            if (resp == option_back) or (resp == "0"):
                txt = "You are back to the user management menu."
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(users_menu))
                self.menu_id = keys_dict[option_usermgmt]
                return                            
            sid = 0
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
                    await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(useraction_menu))
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
                retmsg = "Please enter the new email address:"
                self.menu_id = bot_intance.keys_dict[opt_resetemail]
            elif resp == opt_unbind:
                query = f"update user_master set binded = 0, chat_id = 0 where client_name = '{client_name}' and studentid = {self.student_id} ;"
                rds_update(query)
                retmsg = f"User telegram account has been unbinded from Student-ID {self.student_id}."

        elif self.menu_id == keys_dict[opt_resetemail]:
                if '@' in resp:
                    query = f"update user_master set email = '{resp}' where client_name = '{client_name}' and studentid = {self.student_id};"
                    rds_update(query)
                    retmsg = f"Email address for Student-ID {self.student_id} has been set to {resp}."
                self.menu_id = keys_dict[option_resetuser]
        
        elif self.menu_id == keys_dict[option_client] :
            if resp == option_back :
                txt = "You are back in the main menu"
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.mainmenu))
                self.menu_id = keys_dict[option_mainmenu]
            elif resp == sys_clientcopy :
                df = rds_df("select distinct client_name from user_master order by client_name;")
                if df is None:
                    retmsg = "Information not available"
                else:
                    df.columns = ['client_name']
                    #client_list = [x for x in df.client_name if x not in ['Sambaash']] + [option_back]
                    client_list = [x for x in df.client_name] + [option_back]
                    txt = "Client copy to Sambaash client from :"
                    await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup([client_list]))
                    self.menu_id = keys_dict[opt_clientcopy]
            elif resp == sys_clientname :
                df = rds_df("select distinct client_name from user_master order by client_name;")
                if df is None:
                    retmsg = "Information not available"
                else:
                    df.columns = ['client_name']
                    client_list = [x for x in df.client_name] 
                    txt = "Set the default client to :"
                    await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup([client_list]))
                    self.menu_id = keys_dict[sys_clientname]

        elif self.menu_id == keys_dict[opt_clientcopy]:
            if resp == option_back:
                txt = "You are back in the client menu"
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(client_menu))
                self.menu_id = keys_dict[option_client]
                return
            self.vars['src_clt'] = resp
            txt = "Enter the target client name (case sensitive (0 to exit):"
            await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(client_menu))
            self.menu_id = keys_dict[sys_clientcopy]
        
        elif self.menu_id == keys_dict[sys_clientcopy]:
            if resp != '0':
                src_clt = self.vars['src_clt']
                tgt_clt = resp
                await self.sender.sendMessage(f"Copying from {src_clt} to {tgt_clt}....")
                table_list = ['course_module' , 'iu_stages', 'mcqas_info', 'mcq_data', 'mcq_score']
                table_list += ['module_iu' , 'playbooks', 'stages', 'stages_master', 'userdata' , 'user_master']                
                for tbl in table_list:                    
                    print(f"Duplicating data for table {tbl}....")
                    qry =  f"select * from {tbl} WHERE client_name = '{src_clt}';"
                    df = rds_df(qry)
                    if df is not None:
                        df.columns = get_columns(tbl)
                        df['client_name'] = tgt_clt
                        qry = f"DELETE FROM {tbl} WHERE client_name = '{tgt_clt}';"
                        rds_update(qry)
                        copydbtbl(df, tbl)  
                await self.sender.sendMessage("Client copy  has been completed.")
            txt = "You are back in the client menu"
            await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(client_menu))
            self.menu_id = keys_dict[option_client]
            
        elif self.menu_id == keys_dict[sys_clientname]:            
            bot_intance.client_name = client_name = resp            
            txt = "Default client_name set to {resp}"
            await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(client_menu))
            self.menu_id = keys_dict[option_client]

        elif self.menu_id == keys_dict[sys_pyt]:
            if resp == option_back :
                txt = "You are back in the system menu"
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(system_menu))
                self.menu_id = keys_dict[option_syscfg]
            else:
                try:
                    txt = pycmd(resp)
                except:
                    txt = ""
                txt_list = txt.split('\n')
                cnt = int((len(txt_list)+19)/20)
                html_msglist = []
                for n in range(cnt):
                    m = n*20
                    result = '\n'.join(txt_list[m:][:20])
                    html_msglist.append(result)
            
        elif self.menu_id == keys_dict[sys_cmd]:
            if resp == option_back :
                txt = "You are back in the system menu"
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(system_menu))
                self.menu_id = keys_dict[option_syscfg]
            else:
                try:
                    txt = shellcmd(resp)
                except:
                    txt = ""
                txt_list = txt.split('\n')
                cnt = int((len(txt_list)+19)/20)
                html_msglist = []
                for n in range(cnt):
                    m = n*20
                    result = '\n'.join(txt_list[m:][:20])
                    html_msglist.append(result)

        if html_msglist != []:
            title = "" if html_title=="" else ("<b>" + html_title + "</b>\n")
            for msg in html_msglist:
                txt = title + "<pre>" + msg +  "</pre>"
                await self.bot.sendMessage(chat_id,txt,parse_mode='HTML')

        while len(retmsg) > 0:
            txt = retmsg[:100]
            retmsg = retmsg[100:]
            await self.sender.sendMessage(txt)
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
    global svcbot, bot_intance, edx_api_header, edx_api_url    
    err = 0    
    with open("vmbot.json") as json_file:  
        bot_info = json.load(json_file)
    vmsvclib.rds_connstr = bot_info['omdb']
    vmsvclib.rdscon = None
    vmsvclib.rds_pool = 0
    vmsvclib.rdsdb = None
    df = rds_df("select * from params where client_name = 'System';")
    if df is None:
        print("unable to proceed, params table not found")
        return        
    df.columns = get_columns("params")
    par_val = ['' + str(x) for x in df.value]
    par_key = [x for x in df.key]
    par_dict = dict(zip(par_key, par_val))
    #SvcBotToken = par_dict['ServiceBot']
    SvcBotToken = '989298710:AAEi6VVxa5dFBNJHQrQgKqcqdxj0QRJ9Bx4' # OmniMentorDemoBot
    #adminchatid = int(par_dict['adminchatid'])
    adminchatid = 71354936
    client_name = par_dict['client_name']
    gmt = int(par_dict['GMT'])
    #max_duration = int(par_dict['max_duration'])
    max_duration = 300
    omchat.load_modelfile("ft_model.bin", client_name)
    dt_model.load_model("dt_model.bin")
    nn_model.model_loader("ffnn_model.hdf5")
    print("Running the service bot now")
    bot_intance = BotInstance(SvcBotToken, client_name, max_duration, adminchatid) 
    #svcbot.sendMessage(adminchatid, "Click /start to connect the ServiceBot")
    loop = asyncio.get_event_loop()
    loop.create_task(MessageLoop(svcbot).run_forever())
    bot_intance.loop = loop
    loop.run_forever()
    return

#------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    version = sys.version_info
    if version.major == 3 and version.minor >= 6:
        do_main()
    else:
        print("Unable to use this version of python\n", version)
    
