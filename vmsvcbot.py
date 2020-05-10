import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore")

import sqlite3
import pymysql
import pymysql.cursors

import pandas as pd
import pandas.io.formats.style
import os, sys, time, string
import random, wget, json
import subprocess
import cryptography
from cryptography.fernet import Fernet

import telepot
from telepot.loop import MessageLoop
from telepot.delegate import pave_event_space, per_chat_id, create_open, per_callback_query_chat_id
from telepot.namedtuple import ReplyKeyboardMarkup
from telepot.namedtuple import InlineKeyboardMarkup, InlineKeyboardButton
from telepot.helper import IdleEventCoordinator

from vmbotlib import write2html 
from vmnlplib import NLP_Parser

global svcbot

omchat = NLP_Parser()
adminchatid = 1064466049
max_duration = 604800
max_rows = 20
option_back = "◀️"
option_lang = "Language"
option_chat = "Chat 💬"
option_cmd = "Cmd Mode"
option_edx = "SQL Mode"
option_py = "Script Mode"
option_nlp = "NLP"
nlp_corpus = "Corpus"
nlp_train = "Train NLP"

svcbot_menu = [[option_nlp, option_edx, option_chat], [option_py, option_cmd, option_back]]
nlp_menu = [[nlp_corpus, nlp_train, option_back]]

SvcBotToken = "906052064:AAHGP6uDK4D77t9jGl5MbYfI_3IixJdFpC8"    # @omnimentorservicebot

piece = lambda txtstr,seperator,pos : txtstr.split(seperator)[pos]

class BotInstance():
    def __init__(self):
        self.Token = ""        
        self.bot_name = ""
        self.bot_id = ""
        self.bot_running = False
        self.bot = None
        self.user_list = {}        
        self.chat_list = {}
        self.code2fa_list = {}
        self.vars = dict()
        return

    def __str__(self):
        return "OmniMentor ServiceBot"

    def __repr__(self):
        return 'BotInstance()'

    def runbot(self, Token):
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

    def broadcast(self, msg):
        for d in self.user_list:
            self.bot.sendMessage(d, msg)
        return

class MessageCounter(telepot.helper.ChatHandler):
    def __init__(self, *args, **kwargs):
        super(MessageCounter, self).__init__(*args, **kwargs)
        self.router.routing_table['_alarm'] = self.on__alarm
        self.is_admin = False
        self.chatid = 0
        self.username = ""
        self.alarm_msg = ''
        self.edited = 0
        self.menu_id = 1
        self.lang = "en"
        self.mainmenu = []
        self.is_svcbot = True

    def reset(self):
        self.__init__()
        return

    def logoff(self):
        global svcbot
        if self.chatid in svcbot.user_list:
            svcbot.user_list.pop(self.chatid)
        txt = "Have a great day!"
        bot_prompt(self.bot, self.chatid, txt, [['/start']])
        self.chatid = 0
        self.reset
        self.menu_id = 1
        self.lang = "en"
        return

    def on_close(self, exception):
        self.logoff()
        self.sender.sendMessage('session time out, goodbye.')
        return

    def on__alarm(self, event):
        self.sender.sendMessage(self.alarm_msg)

    def session_info(self):
        global svcbot
        txt = "Summary:\n"
        if len(svcbot.user_list)==0:
            txt += 'No users online\n'
        else:
            txt += 'List of users online:\n'
            txt += '\n'.join([ str(r) + "     " + svcbot.user_list[r][0] for r in svcbot.user_list ])
        txt += "\nmenu id = " + str(self.menu_id)
        txt += "\nToken = "  + self.bot._token
        txt += "\nType : svcbot" if self.is_svcbot else "echobot"
        return txt

    def on_chat_message(self, msg):
        global svcbot
        try:
            content_type, chat_type, chat_id = telepot.glance(msg)
            bot = self.bot
            self.is_svcbot = (self.bot._token == SvcBotToken)
            self.chatid = chat_id
            self.mainmenu = svcbot_menu 
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
        elif content_type != "text":
            print(content_type)
            print(str(msg))
            txt = "Thanks for the " + content_type + " but I do not need it for now."
            bot.sendMessage(chat_id,txt)
            return

        if resp=='/?':
            retmsg = self.session_info()

        elif resp.startswith('/@') and self.is_admin:
            param = resp[2:].split('/')
            try:
                delay = int(param[0])
                self.alarm_msg = param[1]
                self.scheduler.event_later(delay*60, ('_alarm', {'payload': delay}))
                retmsg = 'Alarm is set at %.1f minutes from now.' % delay
            except:
                pass

        elif resp=='/end':
            endchat(bot, chat_id)
            self.is_admin = (chat_id == adminchatid)
            self.logoff()
            self.menu_id = 1

        elif resp=='/stop' and (chat_id in [adminchatid, 56381493, 71354936]):
            svcbot.broadcast('System shutting down.')
            svcbot.bot_running = False
            retmsg = 'System already shutdown.'

        elif resp == '/start':
            self.reset
            if self.is_svcbot:
                if chat_id in [adminchatid, 56381493, 71354936] :
                    self.is_admin = True
                    txt = banner_msg("Welcome","You are now connected to admin mode.")
                    self.menu_id = 2
                    bot_prompt(bot, chat_id, txt, self.mainmenu)
                    svcbot.user_list[chat_id] = [self.username, self.lang]
                else:
                    self.is_admin = False
                    txt = "Following user requesting for admin access :\n"
                    txt += f"Command of request : {resp} \n\n"
                    txt += json.dumps(msg)
                    code2FA = ''.join(random.choice( "ABCDEFGHJKLMNPQRTUVWXY0123456789" ) for i in range(32))
                    code2FA = code2FA.upper()
                    svcbot.code2fa_list[chat_id] = code2FA
                    txt += "\n\nFor your approval with 2FA code : " + code2FA
                    bot.sendMessage(adminchatid, txt)
                    txt = "Please enter the 2FA code :"
                    bot_prompt(bot, chat_id, txt, [])
                    self.menu_id = 23
            else:
                self.is_admin = True
                self.menu_id = 2
                self.lang = 'en'
                txt = "This is a translation chatbot with voice support.\nPlease select your language."
                bot_prompt(bot, chat_id, txt, self.mainmenu)
                svcbot.user_list[chat_id] = [self.username, self.lang]

        elif chat_id in svcbot.chat_list and (resp.strip() != "") :
            tid = svcbot.chat_list[chat_id]
            if resp.lower() == 'bye':
                endchat(bot, chat_id)
                bot_prompt(bot, chat_id, "You are back to the main menu", self.mainmenu)
                self.menu_id = 2
            elif resp in self.mainmenu[0]:
                bot_prompt(bot, chat_id, "Bot> you are in a livechat.", [ ['bye'] ] )
                self.menu_id = 20
            else:
                if self.menu_id != 20:
                    bot_prompt(bot, chat_id, "Bot> you are in a livechat now.", [ ['bye'] ] )
                    self.menu_id=20
                peermsg(bot, chat_id, resp)

        elif self.menu_id == 1:
            retmsg = resp

        elif self.menu_id == 2:
            if chat_id in svcbot.chat_list:
                tid = svcbot.chat_list[ chat_id ]
                bot.sendMessage(tid , resp)
                bot_prompt(bot, chat_id, "(type bye when you want to end the conversation)", [['bye']])
                self.menu_id = 20
            elif resp == option_chat :
                self.menu_id = livechat(bot, chat_id, self.username)
            elif resp == option_py :
                txt = "You are now connected to Script mode.\nDo not use double quote \" for string quotation."
                txt = banner_msg("Python Shell", txt)
                bot_prompt(bot, chat_id, txt, [[option_back]])
                self.menu_id = 3
            elif resp == option_edx and self.is_svcbot:
                if os.name == "nt":
                    txt = "You are now connected to Sqlite database via SQL."
                else:
                    txt = "You are now connected to EdX database via SQL."
                txt = banner_msg("SQL Console for EdX", txt)
                bot_prompt(bot, chat_id, txt, [[option_back]])
                self.menu_id = 5
            elif resp == option_cmd :
                txt = "You are now connected to Cmd mode.\nType cmd to list out the commands."
                txt = banner_msg("Service Console", txt)
                bot_prompt(bot, chat_id, txt, [[option_back]])
                self.menu_id = 6
            elif resp == option_nlp :
                txt = "This section maintain NLP corpus and trains model.\n"
                txt += "You can test your NLP dialog from here."
                txt = banner_msg("NLP", txt)
                bot_prompt(bot, chat_id, txt, nlp_menu)
                self.menu_id = 7
            elif resp == option_back :
                endchat(bot, chat_id)
                self.logoff()
            else:
                retmsg = resp

        elif self.menu_id in range(3,8):
            if resp == option_back :
                bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                self.menu_id = 2
            elif self.menu_id == 3 and self.is_svcbot:
                retmsg = pycmd(resp)
            elif self.menu_id == 4:
                txt = omchat.get_response(resp)
                txt = "`" + txt + "`"
                bot.sendMessage(chat_id, txt, parse_mode='markdown')
            elif self.menu_id == 5 and self.is_svcbot:
                retmsg = edxsql( resp )
            elif self.menu_id == 6 and self.is_svcbot:
                retmsg = shellcmd(resp)
            elif self.menu_id == 7 and self.is_svcbot:
                if resp == nlp_train:
                    if omchat.train_model() :
                        retmsg = "NLP model using the corpus table has been trained with model file saved as ft_model.bin"
                    else:
                        retmsg = "NLP model using the corpus table was not trained properly"
                elif resp == nlp_corpus:
                    fn = "ft_corpus.html"
                    bot.sendMessage(chat_id,"preparing...one moment")
                    df = sql2var("nlp-conf.db", "select * from ft_corpus", "", True)
                    write2html(df, title='FASTTEXT CORPUS', filename=fn)
                    if os.name == "nt":
                        bot.sendDocument(chat_id, document=open(fn, 'rb'))
                    else:
                        try: # save to /home/omnimentor/webpage/svc , the actual code not ready
                            txt = "https://om.sambaash.com/svc/" + fn
                            bot.sendMessage(chat_id, txt)
                        except:
                            bot.sendDocument(chat_id, document=open(fn, 'rb'))
                else:
                    retmsg = omchat.get_response(resp)

        ## trigger when live chat is initiated
        elif self.menu_id == 20:
            if resp.lower() == 'bye':
                endchat(bot, chat_id)
                bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                self.menu_id = 2
            else:
                peermsg(bot, chat_id, resp)

        elif self.menu_id == 22:
            if chat_id in svcbot.chat_list:
                tid = svcbot.chat_list[ chat_id ]
                bot.sendMessage(tid , resp)
                bot_prompt(bot, chat_id, "(type bye when you want to end the conversation)", [['bye']])
                self.menu_id = 20
            else:
                rlist = resp.split(' ')
                sid = rlist[0]
                if sid.isnumeric():
                    sid = int(sid)
                    if sid in svcbot.chat_list:
                        retmsg = "User " + rlist[-1] + " is on another conversation."
                    else:
                        self.menu_id = livechat(bot, chat_id, self.username,sid)
                elif resp == option_back:
                    bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                    self.menu_id = 2

        elif self.menu_id == 23:
            code2FA = svcbot.code2fa_list[chat_id]
            if code2FA == resp:
                self.is_admin = True
                txt = banner_msg("Welcome","You are now connected to Mentor mode.")
                self.menu_id = 2
                bot_prompt(bot, chat_id, txt, self.mainmenu)
                svcbot.code2fa_list.pop(chat_id)
                svcbot.user_list[chat_id] = [self.username, self.lang]
            else:
                txt  = "Sorry the 2FA code is invalid, please try again."
                self.is_admin = False
                bot_prompt(bot, chat_id, txt, [['/start']])
                self.menu_id = 1

        if retmsg != '':
            self.sender.sendMessage(retmsg)
        return

def livechat(bot, chat_id, user_name, sid = 0):
    if sid > 0:
            user_from = svcbot.user_list[chat_id][0] + "("  + str(chat_id) + ")"
            tname = svcbot.user_list[sid][0]
            txt = banner_msg("Live Chat","Hi " + tname + ", you are in the live chat with " + user_from)
            bot.sendMessage(sid,txt)
            svcbot.chat_list[sid] = chat_id
            svcbot.chat_list[chat_id] = sid
            user_to = svcbot.user_list[sid][0] + "("  + str(sid) + ")"
            txt = banner_msg("Live Chat","Hi " + user_name + ", you are in the live chat with " + user_to)
            bot_prompt(bot, chat_id, txt, [['bye']])
            menu_id = 20
    else:
        online_users = [ [str(r) + "     " + svcbot.user_list[r][0] ] for r in svcbot.user_list if r != chat_id]
        if len(online_users) > 0:
            txt = 'Chat with online users 🗣'
            online_users = online_users + [ [option_back] ]
            bot_prompt(bot, chat_id, txt, online_users)
            menu_id = 22
        else:
            txt = "Hi I am " + (bot.getMe())['username'] + "\nThere is no online users at the moment, you can chat with me now."
            bot_prompt(bot, chat_id, txt, [ [option_back] ] )
            menu_id = 4
    return menu_id 

def endchat(bot, chat_id):
    chat_found = False
    if chat_id in svcbot.chat_list:
        chat_found = True
        tid = svcbot.chat_list[chat_id]
        txt = "Live chat session disconnected. 👋"
        bot.sendMessage(chat_id, txt)
        bot.sendMessage(tid, txt)
        if chat_id in svcbot.chat_list:
            svcbot.chat_list.pop(chat_id)
        if tid in svcbot.chat_list:
            svcbot.chat_list.pop(tid)
    return chat_found

def peermsg(bot, chat_id,  resp):
    tid = svcbot.chat_list[chat_id]
    dest_lang = svcbot.user_list[ tid ][1]
    bot.sendMessage(tid, resp)
    return

def bot_prompt(bot, chat_id, txt, buttons, opt_resize = True):
    if chat_id == 0:
        return
    if chat_id < 0:
        sent = bot.sendMessage(chat_id, txt)
        edited = telepot.message_identifier(sent)
        return
    if buttons == []:
        hide_keyboard = {'hide_keyboard': True}
        bot.sendMessage(chat_id, txt, reply_markup=hide_keyboard)
        return
    if chat_id > 0:
        mark_up = ReplyKeyboardMarkup(keyboard=buttons,one_time_keyboard=True,resize_keyboard=opt_resize)
    else:
        if type(buttons[0])==list:
            menu_keyboard = [[InlineKeyboardButton(text=c[0], callback_data=c[0])] for c in buttons if len(c[0])<=60]
        else:
            menu_keyboard=[list(map(lambda c: InlineKeyboardButton(text=c, callback_data=c), buttons))]
        mark_up = InlineKeyboardMarkup(inline_keyboard=menu_keyboard)
    try:
        sent = bot.sendMessage(chat_id, txt, reply_markup=mark_up)
    except:
        sent = bot.sendMessage(chat_id, txt)
    edited = telepot.message_identifier(sent)
    return

def shellcmd(cmd):
    try:
        ps = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        output = (ps.communicate()[0]).decode("utf8")
    except:
        output = ''
    return output

def pycmd(resp):
    vars = svcbot.vars
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
    svcbot.vars = vars
    return result
    
def edxsql(query):
    if os.name == "nt":
        df = sql2var("nlp-conf.db", query, "", True)
    else:
        txt = ""
        conn_str = "mysql://sambaash:bHyyZ3krZ4fguwcrAD7v@52.226.130.222/edxapp"
        try:
            host=piece(piece(piece(conn_str,':',2),"@",1),'/',0)
            user=piece(piece(conn_str,':',1),'/',2)
            pw=piece(piece(conn_str,':',2),"@",0)
            edxcon = pymysql.connect(host=host,
                    user=user,
                    password=pw,
                    db='edxapp',
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor)
            edxcur = edxcon.cursor()
            edxcur.execute(query)
            rows = edxcur.fetchall()
            df = pd.DataFrame.from_dict(rows)
            edxcon.commit()
            edxcon.close()
        except:
            return ""
    if len(df) >= max_rows:
        txt = str(df.head(max_rows))
    else:
        txt = str(df)
    return txt

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

def decrypt(msg):
    key = '4jYUFl-wbMZ4NIiI2kG3LMFD5KTTKiT5ZiE6Yhoshp0='
    cto = Fernet(key)
    try:
        txt = msg.encode()
    except:
        txt = msg
    resp = cto.decrypt(txt)
    return resp.decode()

def encrypt(msg):
    key = '4jYUFl-wbMZ4NIiI2kG3LMFD5KTTKiT5ZiE6Yhoshp0='
    cto = Fernet(key)
    txt = cto.encrypt(msg.encode())
    return txt

def banner_msg(banner_title, banner_msg):
    txt = "▓▓▓▒▒▒▒▒▒▒░░░  " + banner_title + "  ░░░▒▒▒▒▒▒▒▓▓▓"
    txt += "\n" + banner_msg + "\n"
    return txt

def do_main():
    global svcbot
    err = 0
    svcbot = BotInstance()
    svcbot.runbot(SvcBotToken)
    print(svcbot)
    try:
        svcbot.bot.sendMessage(adminchatid,"Click /start to connect the ServiceBot")
    except:
        pass
    omchat.load_model("ft_model.bin", "nlp-conf.db")
    while svcbot.bot_running:
        time.sleep(3)
    try:
        os.kill(os.getpid(), 9)
    except:
        err = 1
    return (err==0)

#------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    version = sys.version_info
    if version.major == 3 and version.minor >= 7:
        do_main()
    else:
        print("Unable to use this version of python\n", version)
