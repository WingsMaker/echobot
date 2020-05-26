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
# ∙∙·▫▫ᵒᴼᵒ▫ₒₒ▫ᵒᴼⓈⓔⓡⓥⓘⓒⓔⒷⓞⓣᴼᵒ▫ₒₒ▫ᵒᴼᵒ▫▫·∙∙
#------------------------------------------------------------------------------------------------------
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore")

import os, sys, time
import random, wget, json
import subprocess
import telepot
from telepot.loop import MessageLoop
from telepot.delegate import pave_event_space, per_chat_id, create_open, per_callback_query_chat_id
from telepot.namedtuple import ReplyKeyboardMarkup
from telepot.helper import IdleEventCoordinator

from vmsvclib import *
from vmnlplib import NLP_Parser

global svcbot

omchat = NLP_Parser()
#adminchatid = 71354936
adminchatid = 1064466049
max_duration = 28800
max_rows = 20
option_mainmenu = 'svcbot_menu'
option_back = "◀️"
option_chat = "Chat 💬"
option_cmd = "Cmd Mode 💻"
option_py = "Script Mode 📜"
option_edx = "SQL Mode"
option_nlp = "NLP"
nlp_corpus = "Corpus"
nlp_train = "Train NLP"
option_chatlist = "Chat List"
option_chatempty = "Chat Empty"
option_2fa = "2FA"
nlpconfig = "nlp-conf.db"

svcbot_menu = [[option_nlp, option_edx, option_chat], [option_py, option_cmd, option_back]]
nlp_menu = [[nlp_corpus, nlp_train, option_back]]

#SvcBotToken = "906052064:AAHGP6uDK4D77t9jGl5MbYfI_3IixJdFpC8"    # @omnimentorservicebot
SvcBotToken = "812577272:AAEgRcGYOGzkN9AoJQKLusspiowlUuGrtj0"    # @OmniMentorBot

piece = lambda txtstr,seperator,pos : txtstr.split(seperator)[pos]

class BotInstance():
    def __init__(self, Token):
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
        self.keys_dict = {}
        self.keys_dict[option_mainmenu] = 1
        self.define_keys(svcbot_menu, self.keys_dict[option_mainmenu])
        self.define_keys(nlp_menu, self.keys_dict[option_nlp])
        self.keys_dict[option_chatlist] = (self.keys_dict[option_chat]*10) + 1
        self.keys_dict[option_chatempty] = (self.keys_dict[option_chat]*10) + 2
        self.keys_dict[option_2fa] = (self.keys_dict[option_mainmenu]*10) + 1
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
        for menu_item in button_list(telegram_menu):
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
        self.parentbot = svcbot
        self.mainmenu = svcbot.mainmenu

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
            bot = self.bot
            self.chatid = chat_id
            keys_dict = svcbot.keys_dict
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
        elif content_type in ['audio','voice'] and 'audio/' in msg[content_type]['mime_type']:
            fid = msg[content_type]['file_id']
            fname = get_attachment(bot, fid)
            txt = process_voice(fname)
            if txt != "":                
                bot.sendMessage(chat_id, "you said : " + txt)
                resp = txt
        elif content_type != "text":
            print( json.dumps(msg) )
            txt = "Thanks for the " + content_type + " but I do not need it for now."
            bot.sendMessage(chat_id,txt)

        if resp=='/end':
            endchat(bot, self.parentbot, chat_id)
            self.is_admin = (chat_id == adminchatid)
            self.logoff()
            self.menu_id = 0

        elif resp=='/stop' and (chat_id==adminchatid):
            self.parentbot.broadcast('System shutting down.')
            self.parentbot.bot_running = False            
            retmsg = 'System already shutdown.'

        elif resp == '/start':
            self.reset
            if chat_id in [adminchatid , 71354936, 56381493, 263090563]:
                self.is_admin = True
                txt = banner_msg("Welcome","You are now connected to admin mode.")
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

        elif chat_id in self.parentbot.chat_list and (resp.strip() != "") :
            tid = self.parentbot.chat_list[chat_id]
            if resp.lower() == 'bye':
                endchat(bot, self.parentbot, chat_id)
                bot_prompt(bot, chat_id, "You are back to the main menu", self.mainmenu)
                self.menu_id = keys_dict[option_mainmenu]
            elif resp in self.mainmenu[0]:
                bot_prompt(bot, chat_id, "Bot> you are in a livechat.", [ ['bye'] ] )
                self.menu_id = keys_dict[option_chat]
            else:
                if self.menu_id != keys_dict[option_chat]:
                    bot_prompt(bot, chat_id, "Bot> you are in a livechat now.", [ ['bye'] ] )
                    self.menu_id = keys_dict[option_chat]
                peermsg(bot, self.parentbot, chat_id, resp)

        elif self.menu_id == keys_dict[option_mainmenu]:
            if chat_id in self.parentbot.chat_list:
                tid = self.parentbot.chat_list[ chat_id ]
                bot.sendMessage(tid , resp)
                bot_prompt(bot, chat_id, "(type bye when you want to end the conversation)", [['bye']])
                self.menu_id = keys_dict[option_chat]
            elif (resp == option_chat ) or (resp == '/chat'):
                self.menu_id = livechat(bot, self.parentbot, chat_id, self.username)
            elif resp == option_py :
                txt = "You are now connected to Script mode.\nDo not use double quote \" for string quotation."
                txt = banner_msg("Python Shell", txt)
                bot_prompt(bot, chat_id, txt, [[option_back]])
                self.menu_id = keys_dict[option_py]
            elif resp == option_cmd :
                txt = "You are now connected to Cmd mode.\nType cmd to list out the commands."
                txt = banner_msg("Service Console", txt)
                bot_prompt(bot, chat_id, txt, [[option_back]])
                self.menu_id = keys_dict[option_cmd]
            elif resp == option_edx :
                if os.name == "nt":
                    txt = "You are now connected to Sqlite database via SQL."
                else:
                    txt = "You are now connected to EdX database via SQL."
                txt = banner_msg("SQL Console for EdX", txt)
                bot_prompt(bot, chat_id, txt, [[option_back]])
                self.menu_id = keys_dict[option_edx]
            elif resp == option_nlp :
                txt = "This section maintain NLP corpus and trains model.\n"
                txt += "You can test your NLP dialog from here."
                txt = banner_msg("NLP", txt)
                bot_prompt(bot, chat_id, txt, nlp_menu)
                self.menu_id = keys_dict[option_nlp]
            elif resp == option_back :
                endchat(bot, self.parentbot, chat_id)
                self.logoff()
            else:
                pass

        elif self.menu_id in range(3,8):
            if resp == option_back :
                bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                self.menu_id = keys_dict[option_mainmenu]
            elif self.menu_id == keys_dict[option_chatempty]:
                (txt, accuracy)  = omchat.get_response(resp)
                bot.sendMessage(chat_id, txt, parse_mode='markdown')

        elif self.menu_id == keys_dict[option_nlp] :
            if resp == option_back :
                bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                self.menu_id = keys_dict[option_mainmenu]
            else:
                if resp == nlp_train:
                    if omchat.train_model() :
                        retmsg = "NLP model using the corpus table has been trained with model file saved as ft_model.bin"
                    else:
                        retmsg = "NLP model using the corpus table was not trained properly"
                elif resp == nlp_corpus:
                    fn = "ft_corpus." + str(chat_id) + "html"
                    bot.sendMessage(chat_id,"preparing...one moment")
                    df = sql2var(nlpconfig, "select * from ft_corpus", "", True)
                    write2html(df, title='FASTTEXT CORPUS', filename=fn)
                    bot.sendDocument(chat_id, document=open(fn, 'rb'))
                else:                    
                    (retmsg,accuracy)  = omchat.get_response(resp)

        elif self.menu_id == keys_dict[option_edx] :
            if resp == option_back :
                bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                self.menu_id = keys_dict[option_mainmenu]
            else:
                fn = "sql_output" + str(chat_id) + ".html"
                if edxsql(resp,fn)==0:
                    retmsg =  "Unable to execute the query."
                else:
                    bot.sendDocument(chat_id, document=open(fn, 'rb'))

        elif self.menu_id == keys_dict[option_py] :
            if resp == option_back :
                bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                self.menu_id = keys_dict[option_mainmenu]
            else:
                retmsg = pycmd(resp, self.parentbot)

        elif self.menu_id == keys_dict[option_cmd] :
            if resp == option_back :
                bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                self.menu_id = keys_dict[option_mainmenu]
            else:
                retmsg = shellcmd(resp)

        ## trigger when live chat is initiated
        elif self.menu_id == keys_dict[option_chat]:
            if resp.lower() == 'bye':
                endchat(bot, self.parentbot, chat_id)
                bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                self.menu_id = keys_dict[option_mainmenu]
            else:
                peermsg(bot, self.parentbot, chat_id, resp)

        elif self.menu_id ==  keys_dict[option_chatlist]:
            if chat_id in self.parentbot.chat_list:
                tid = self.parentbot.chat_list[ chat_id ]
                bot.sendMessage(tid , resp)
                bot_prompt(bot, chat_id, "(type bye when you want to end the conversation)", [['bye']])
                self.menu_id = keys_dict[option_chat]
            else:
                rlist = resp.split(' ')
                sid = rlist[0]
                if sid.isnumeric():
                    sid = int(sid)
                    if sid in self.parentbot.chat_list:
                        retmsg = "User " + rlist[-1] + " is on another conversation."
                    else:
                        self.menu_id = livechat(bot, self.parentbot, chat_id, self.username,sid)
                elif resp == option_back:
                    bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                    self.menu_id = keys_dict[option_mainmenu]

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

        if retmsg != '':
            self.sender.sendMessage(retmsg)
        return

def livechat(bot, parentbot, chat_id, user_name, sid = 0):
    if sid > 0:
            user_from = parentbot.user_list[chat_id][0] + "("  + str(chat_id) + ")"
            tname = parentbot.user_list[sid][0]
            txt = banner_msg("Live Chat","Hi " + tname + ", you are in the live chat with " + user_from)
            bot.sendMessage(sid,txt)
            parentbot.chat_list[sid] = chat_id
            parentbot.chat_list[chat_id] = sid
            user_to = parentbot.user_list[sid][0] + "("  + str(sid) + ")"
            txt = banner_msg("Live Chat","Hi " + user_name + ", you are in the live chat with " + user_to)
            bot_prompt(bot, chat_id, txt, [['bye']])
            menu_id = keys_dict[option_chat]
    else:
        online_users = [ [str(r) + "     " + parentbot.user_list[r][0] ] for r in parentbot.user_list if r != chat_id]
        if len(online_users) > 0:
            txt = 'Chat with online users 🗣'
            online_users = online_users + [ [option_back] ]
            bot_prompt(bot, chat_id, txt, online_users)
            menu_id = parentbot.keys_dict[option_chatlist]
        else:
            txt = "Hi I am " + (bot.getMe())['username'] + "\nThere is no online users at the moment, you can chat with me now."
            bot_prompt(bot, chat_id, txt, [ [option_back] ] )
            menu_id = parentbot.keys_dict[option_chatempty] 
    return menu_id

def endchat(bot, parentbot, chat_id):
    chat_found = False
    if chat_id in parentbot.chat_list:
        chat_found = True
        tid = parentbot.chat_list[chat_id]
        txt = "Live chat session disconnected. 👋"
        bot.sendMessage(chat_id, txt)
        bot.sendMessage(tid, txt)
        if chat_id in parentbot.chat_list:
            parentbot.chat_list.pop(chat_id)
        if tid in parentbot.chat_list:
            parentbot.chat_list.pop(tid)
    return chat_found

def peermsg(bot, parentbot, chat_id,  resp):
    tid = parentbot.chat_list[chat_id]
    bot.sendMessage(tid, resp)
    return

def do_main():
    global svcbot
    err = 0
    omchat.load_model("ft_model.bin", nlpconfig)

    svcbot = BotInstance(SvcBotToken)
    print(svcbot)
    try:
        svcbot.bot.sendMessage(adminchatid,"Click /start to connect the ServiceBot")
    except:
        pass
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
