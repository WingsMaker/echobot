import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore")

import sqlite3
import pymysql

import pandas as pd
import pandas.io.formats.style
import os, sys, time
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

import speech_recognition as sr
from gtts import gTTS          # Google text to speech
import googletrans
from googletrans import Translator
from nltk.chat.iesha  import iesha_chatbot

global svcbot

translator = Translator()
adminchatid = 71354936
max_duration = 3600
max_rows = 20
option_back = "◀️"
option_lang = "Language 🇸🇬"
option_chat = "Chat 💬"
option_voice = "Text2Voice 🎤"
option_cmd = "Cmd Mode 💻"
option_py = "Script Mode 📜"

svcbot_menu = [[option_chat, option_py, option_cmd, option_back]]
echobot_menu = [[option_chat, option_lang, option_voice, option_back]]

lang_opts = ['English', '简体中文','繁體中文','हिंदी','தமிழ்','বাংলা','Filipino','Indonesian', 'Malay',\
    'မြန်မာ','ไทย','Việt Nam','日本語','한국어', 'Nederlands','Français','Deutsch','Italiano','Español']
lang_codes = ['en','zh-cn','zh-tw','hi','ta','bn','pil','id','ms','my','th','vi','ja','ko','nl','fr','de','it','es']
lang_audio = ['en-US','zh-CN','zh-TW','hi-IN','ta-Sg','bn-BD','fil-PH','id-ID','ms-MY','my-MM','th-TH','vi-VN','ja-JP','ko-KR','nl-NL','fr-FR','de-DE','it-IT','es-ES']
lang_menu = [  (lang_opts + [option_back])[n*5:][:5] for n in range(4) ]

SvcBotToken = "1231701118:AAGImKeF8SULGP5ktSnsjuUxD7Jg0RRo0Y4"  # @echochatbot
#SvcBotToken = "812577272:AAEgRcGYOGzkN9AoJQKLusspiowlUuGrtj0"    # @OmniMentorBot

piece = lambda txtstr,seperator,pos : txtstr.split(seperator)[pos]

class BotInstance():
    def __init__(self, Token, svc_mode = False):
        self.Token = ""
        self.bot_name = ""
        self.bot_id = ""
        self.bot_running = False
        self.user_list = {}        
        self.chat_list = {}
        self.code2fa_list = {}
        self.vars = dict()
        self.is_svcbot = svc_mode        
        self.Token = Token
        self.mainmenu = svcbot_menu if svc_mode else echobot_menu
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
        return "ServiceBot for translation"

    def __repr__(self):
        return 'BotInstance()'

    def broadcast(self, msg):
        for d in self.user_list:
            self.bot.sendMessage(d, msg)

class MessageCounter(telepot.helper.ChatHandler):
    def __init__(self, *args, **kwargs):
        super(MessageCounter, self).__init__(*args, **kwargs)
        self.is_admin = False
        self.chatid = 0
        self.username = ""
        self.edited = 0
        self.menu_id = 1
        self.lang = "en"
        self.txt2voice = False

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
        #self.lang = "en"
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
        elif content_type == 'voice':
            try:
                fid = msg['voice']['file_id']
                fpath = bot.getFile(fid)['file_path']
                fn = "https://api.telegram.org/file/bot" + bot._token + "/"  + fpath
                txt = process_voice(fn, self.lang)
                if txt !="":
                    bot.sendMessage(chat_id, txt)
                    resp = txt
            except:
                pass
        elif content_type=="audio":
            fid = msg['audio']['file_id']
            fpath = bot.getFile(fid)['file_path']
            fn = "https://api.telegram.org/file/bot" + bot._token + "/"  + fpath
            if fn[-4:]=='.ogg':
                txt = process_voice(fn, self.lang)
                if txt !="":
                    bot.sendMessage(chat_id,txt)
                    resp = txt
        # OCR temporary removed
        #elif (content_type=="photo") and (os.name != "nt"):
        #    fid = msg['photo'][0]['file_id']
        #    fpath = bot.getFile(fid)['file_path']
        #    fn = "https://api.telegram.org/file/bot" + bot._token + "/"  + fpath
        #    fname = wget.download(fn)
        #    fn = str(chat_id)
        #    cmd = "tesseract " + fname + " " + fn
        #    shellcmd(cmd)
        #    fn = fn + ".txt"
        #    fid = open(fn)
        #    resp = fid.read()
        #    fid.close()
        #    cmd = "/bin/rm -f " + fname + " " + fn
        #    shellcmd(cmd)
        #    resp = "Thanks for the photo but I am not able read it" if resp=="" else resp
        elif content_type != "text":
            print(content_type)
            print(str(msg))
            txt = "Thanks for the " + content_type + " but I do not need it for now."
            bot.sendMessage(chat_id,txt)
            return

        if resp=='/end':
            endchat(bot, chat_id)
            self.is_admin = (chat_id == adminchatid)
            self.logoff()
            self.menu_id = 1

        elif resp=='/stop' and (chat_id==adminchatid):
            svcbot.broadcast('System shutting down.')
            svcbot.bot_running = False            
            retmsg = 'System already shutdown.'

        elif resp == '/start':
            self.reset            
            if svcbot.is_svcbot:
                if chat_id==adminchatid :
                    self.is_admin = True
                    txt = banner_msg("Welcome","You are now connected to admin mode.")
                    self.menu_id = 2
                    bot_prompt(bot, chat_id, txt, svcbot.mainmenu)
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
                bot_prompt(bot, chat_id, txt, svcbot.mainmenu)
                svcbot.user_list[chat_id] = [self.username, self.lang]

        elif chat_id in svcbot.chat_list and (resp.strip() != "") :
            tid = svcbot.chat_list[chat_id]
            if resp.lower() == 'bye':
                endchat(bot, chat_id)
                bot_prompt(bot, chat_id, "You are back to the main menu", svcbot.mainmenu)
                self.menu_id = 2
            elif resp == option_lang :
                txt = "Select the prefered language"
                bot_prompt(bot, chat_id, txt, lang_menu)
                self.menu_id = 21
            elif resp in svcbot.mainmenu[0]:
                bot_prompt(bot, chat_id, "Bot> you are in a livechat.", [ ['bye'] ] )
                self.menu_id = 20
            else:
                if self.menu_id != 20:
                    bot_prompt(bot, chat_id, "Bot> you are in a livechat now.", [ ['bye'] ] )
                    self.menu_id=20
                peermsg(bot, chat_id, resp)

        elif self.menu_id == 1:
            retmsg = translate(self.lang,resp)

        elif self.menu_id == 2:
            if chat_id in svcbot.chat_list:
                tid = svcbot.chat_list[ chat_id ]
                bot.sendMessage(tid , resp)
                bot_prompt(bot, chat_id, "(type bye when you want to end the conversation)", [['bye']])
                self.menu_id = 20
            elif (resp == option_chat ) or (resp == '/chat'):
                self.menu_id = livechat(bot, chat_id, self.username)
            elif (resp == option_lang ) or (resp == '/lang'):
                txt = "welcome to the translation bot\npnPlease select your language:"
                bot_prompt(bot, chat_id, txt, lang_menu)
                self.menu_id = 24
            elif (resp == option_voice ) or (resp == '/voice'):                
                self.txt2voice = not self.txt2voice
                retmsg = "text to speech turn " + (" on." if self.txt2voice else "off.")
            elif resp == option_py :
                txt = "You are now connected to Script mode.\nDo not use double quote \" for string quotation."
                txt = banner_msg("Python Shell", txt)
                bot_prompt(bot, chat_id, txt, [[option_back]])
                self.menu_id = 3
            elif resp == option_cmd :
                txt = "You are now connected to Cmd mode.\nType cmd to list out the commands."
                txt = banner_msg("Service Console", txt)
                bot_prompt(bot, chat_id, txt, [[option_back]])
                self.menu_id = 5
            elif resp == option_back :
                endchat(bot, chat_id)
                self.logoff()
            else:
                if svcbot.is_svcbot:
                    retmsg = translate(self.lang,resp)
                else:
                    txt = translate(self.lang,resp)
                    if self.txt2voice :
                        text2voice(self.bot, self.chatid, self.lang, txt)
                    else:
                        retmsg = txt

        elif self.menu_id in range(3,8):
            if resp == option_back :
                bot_prompt(bot, chat_id, "You are back in the main menu", svcbot.mainmenu)
                self.menu_id = 2
            elif self.menu_id == 3 and svcbot.is_svcbot:
                retmsg = pycmd(resp)
            elif self.menu_id == 4:
                txt = iesha_chatbot.respond(resp)                                
                txt = "`" + txt + "`"
                bot.sendMessage(chat_id, txt, parse_mode='markdown')
            elif self.menu_id == 5 and svcbot.is_svcbot:
                retmsg = shellcmd(resp)

        ## trigger when live chat is initiated
        elif self.menu_id == 20:
            if resp.lower() == 'bye':
                endchat(bot, chat_id)
                bot_prompt(bot, chat_id, "You are back in the main menu", svcbot.mainmenu)
                self.menu_id = 2
            else:
                peermsg(bot, chat_id, resp)

        elif self.menu_id == 21:
            if resp in lang_opts:
                n = lang_opts.index(resp)
                self.lang = lang_codes[n]
                txt = f"You had selected {resp} language"
                svcbot.user_list[chat_id][1] = self.lang
                self.sender.sendMessage(txt)
                self.menu_id = livechat(bot, chat_id, self.username)

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
                    bot_prompt(bot, chat_id, "You are back in the main menu", svcbot.mainmenu)
                    self.menu_id = 2

        elif self.menu_id == 23:
            code2FA = svcbot.code2fa_list[chat_id]
            if code2FA == resp:
                self.is_admin = True
                txt = banner_msg("Welcome","You are now connected to Mentor mode.")
                self.menu_id = 2
                bot_prompt(bot, chat_id, txt, svcbot.mainmenu)
                svcbot.code2fa_list.pop(chat_id)
                svcbot.user_list[chat_id] = [self.username, self.lang]
            else:
                txt  = "Sorry the 2FA code is invalid, please try again."
                self.is_admin = False
                bot_prompt(bot, chat_id, txt, [['/start']])
                self.menu_id = 1

        elif self.menu_id == 24:
            if resp in lang_opts:
                n = lang_opts.index(resp)
                self.lang = lang_codes[n]
                txt = f"You had selected {resp} language"
                svcbot.user_list[chat_id][1] = self.lang
            else:
                txt = "You are back in main menu."
            bot_prompt(bot, chat_id, txt, svcbot.mainmenu)
            self.menu_id = 2

        if retmsg != '':
            self.sender.sendMessage(retmsg)
        return

def livechat(bot, chat_id, user_name, sid = 0):
    global  svcbot
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
    global svcbot 
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
    global svcbot 
    tid = svcbot.chat_list[chat_id]
    dest_lang = svcbot.user_list[ tid ][1]
    dt = translator.detect(resp)
    if dt.lang == dest_lang:
        txt = resp
    else:
        result = translator.translate(resp, dest = dest_lang)
        txt = resp + '\n' + result.text
    if 'en' not in [dt.lang , dest_lang]:
        result = translator.translate(resp, dest = 'en')
        txt += '\n' + result.text
    bot.sendMessage(tid, txt)
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

def translate(lang, txt):
    resp = ""
    try:
        dt = translator.detect(txt)
        if dt.lang == lang:
            return txt
        result = translator.translate(txt, dest=lang)
        resp = result.text
    except:
        pass
    return resp

def text2voice(bot, chat_id, lang, resp):
    try:
        bot.sendMessage(chat_id, resp)
        mp3 = 'echobot.mp3'
        myobj = gTTS(text=resp, lang=lang, slow=False)
        myobj.save(mp3)
        fn = convert_audio(mp3, "ogg")
        if fn != "":
            bot.sendAudio(chat_id, (fn, open(fn, 'rb')), title='text to voice')
            os.remove(fn)
        os.remove(mp3)
    except:
        pass
    return

def shellcmd(cmd):
    try:
        ps = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        output = (ps.communicate()[0]).decode("utf8")
    except:
        output = ''
    return output

def pycmd(resp):
    global svcbot
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

def convert_audio(fname, fmt = ".wav"):
    try:
        fn = fname.split('.')[0]
        fn += "." + fmt
        cmd = f"ffmpeg -y -i {fname} {fn}"
        txt = shellcmd(cmd)
    except:
        fn = ""
    return fn

def wav2txt(wavfile, lang = 'en'):
    r = sr.Recognizer()
    result = ""
    if isinstance(r, sr.Recognizer):
        wav = sr.AudioFile(wavfile)
        with wav as source:
            audio = r.record(source)
        try:
            print("Reading the audio file....")
            result = r.recognize_google(audio)
        except:
            try:
                result = r.recognize_google(audio,lang)
            except:
                pass
    else:
        #print("`recognizer` must be `Recognizer` instance")
        pass
    return result

def process_voice(fn, lang):
    try:
        fname = wget.download(fn)
        wav = convert_audio(fname, "wav")
        if wav != "":
            txt = wav2txt(wav, lang)
            os.remove(wav)
        os.remove(fname)
    except:
        txt = ""
        pass
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
    svcbot = BotInstance(SvcBotToken, False)
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
