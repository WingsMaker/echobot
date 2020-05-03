import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore")

import os, sys, time, string, wget, subprocess
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

#---------------------------------------------------------------------------------------------------------------------------------------
#import urllib3
#proxy_url = "http://proxy.server:3128"
#telepot.api._pools = {    'default': urllib3.ProxyManager(proxy_url=proxy_url, num_pools=3, maxsize=10, retries=False, timeout=30),}
#telepot.api._onetime_pool_spec = (urllib3.ProxyManager, dict(proxy_url=proxy_url, num_pools=1, maxsize=1, retries=False, timeout=30))
#---------------------------------------------------------------------------------------------------------------------------------------

global chat_list,user_list,run_server

translator = Translator()
adminchatid = 71354936
max_duration = 604800
max_rows = 20
option_back = "◀️"
option_lang = "Language"
option_chat = "Chat 💬"

echobot_menu = [[option_chat, option_lang, option_back]]

lang_opts = ['English', '简体中文','繁體中文','हिंदी','தமிழ்','বাংলা','Filipino','Indonesian', 'Malay',\
    'မြန်မာ','ไทย','Việt Nam','日本語','한국어', 'Nederlands','Français','Deutsch','Italiano','Español']
lang_codes = ['en','zh-cn','zh-tw','hi','ta','bn','tl','id','ms','my','th','vi','ja','ko','nl','fr','de','it','es']
lang_menu = [  (lang_opts + [option_back])[n*5:][:5] for n in range(4) ]
EchoBotToken = "1042610944:AAGme_h2ztEG50jwbW8_cVEi0mgXjkXijd8" # @limkopibot

class BotInstance():
    def __init__(self, Token, InlineMode=False):
        self.Token = ""
        self.bot_name = ""
        self.bot_id = ""
        self.bot_running = False
        self.bot = None
        self.bot = ServiceBot(Token)
        self.bot.mainmenu = echobot_menu
        self.bot.thisbot = self.bot
        info = self.bot.getMe()
        self.bot_name = info['username']
        self.bot_id = info.get('id')
        self.Token = Token
        print('Frontend bot : ', self.bot_id)
        msg = self.bot_name + ' started running. URL is https://t.me/' + self.bot_name
        #print(msg)
        try:
            MessageLoop(self.bot).run_as_thread()
            self.bot_running = True
        except:
            print("Error running this bot instance")
        return

    def __str__(self):
        return "TranslationBot by @kimhuat\nUse @"+ self.bot_name

    def __repr__(self):
        return 'BotInstance()'

class ServiceBot(telepot.Bot):
    def __init__(self, *args, **kwargs):
        super(ServiceBot, self).__init__(*args, **kwargs)
        self._answerer = telepot.helper.Answerer(self)
        self.chatid = 0
        self.username = ""
        self.edited = 0
        self.menu_id = 1
        self.lang = "en"
        self.mainmenu = []
        self.thisbot = None

    def on_callback_query(self, msg):
        query_id, from_id, query_data = telepot.glance(msg, flavor='callback_query')

    def on_chosen_inline_result(self, msg):
        result_id, from_id, query_string = telepot.glance(msg, flavor='chosen_inline_result')

    def on_inline_query(self, msg):
        query_id, from_id, query_string = telepot.glance(msg, flavor='inline_query')
        if query_string == "":
            return
        def lang_options():
            cnt = len(lang_opts)
            articles = [{'type': 'article','id': 'L'+str(n+1) ,'title': lang_opts[n] ,'message_text': translator.translate(query_string, dest = lang_codes[n]).text } for n in range(cnt)]
            return articles

        self._answerer.answer(msg, lang_options)
        return

    def reset(self):
        self.__init__()
        return

    def logoff(self):
        global user_list
        if self.chatid in user_list:
            user_list.pop(self.chatid)
        txt = "Have a great day!"
        bot_prompt(self.thisbot, self.chatid, txt, [['/start']])
        self.chatid = 0
        self.reset
        self.menu_id = 1
        self.lang = "en"
        return

    def on_chat_message(self, msg):
        global user_list, chat_list, run_server
        content_type, chat_type, chat_id = telepot.glance(msg)
        self.chatid = chat_id
        bot = self.thisbot
        resp = ""
        retmsg = ''
        username = ""
        if content_type == 'text':
            resp = msg['text'].strip()
            if 'from' in list(msg):
                username = msg['from']['first_name']
                self.username = username
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
                    bot.sendMessage(chat_id, txt)
                    resp = txt
        elif content_type != "text":
            txt = "Thanks for the " + content_type + " but I do not need it for now."
            bot.sendMessage(chat_id, txt)
            return

        if resp=='/end':
            endchat(bot, chat_id)
            self.logoff()
            self.menu_id = 1

        elif resp == '/start':
            self.reset
            self.menu_id = 2
            self.lang = 'en'
            txt = "This is a translation chatbot with voice support.\nPlease select your language."
            bot_prompt(bot, chat_id, txt, self.mainmenu)
            user_list[chat_id] = [self.username, self.lang]

        elif chat_id in chat_list and (resp.strip() != "") :
            tid = chat_list[chat_id]
            if resp.lower() == 'bye':
                endchat(bot, chat_id)
                bot_prompt(bot, chat_id, "You are back to the main menu", self.mainmenu)
                self.menu_id = 2
            elif resp == option_lang :
                txt = "Select the prefered language"
                bot_prompt(bot, chat_id, txt, lang_menu)
                self.menu_id = 21
            elif resp in self.mainmenu[0]:
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
            if chat_id in chat_list:
                tid = chat_list[ chat_id ]
                bot.sendMessage(tid , resp)
                bot_prompt(bot, chat_id, "(type bye when you want to end the conversation)", [['bye']])
                self.menu_id = 20
            elif resp == option_chat :
                self.menu_id = livechat(bot, chat_id, self.username)
            elif resp == option_lang :
                txt = "welcome to the translation bot\nplease select the prefered language:"
                bot_prompt(bot, chat_id, txt, lang_menu)
                self.menu_id = 24
            elif resp == option_back :
                endchat(bot, chat_id)
                self.logoff()
            else:
                txt = translate(self.lang,resp)
                text2voice(bot, chat_id, self.lang, txt)

        elif self.menu_id == 4:
            if resp == option_back :
                bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                self.menu_id = 2
            elif self.menu_id == 4:
                txt = iesha_chatbot.respond(resp)
                txt = "`" + txt + "`"
                bot.sendMessage(chat_id, txt, parse_mode='markdown')

        ## trigger when live chat is initiated
        elif self.menu_id == 20:
            if resp.lower() == 'bye':
                endchat(bot, chat_id)
                bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                self.menu_id = 2
            else:
                peermsg(bot, chat_id, resp)

        elif self.menu_id == 21:
            if resp in lang_opts:
                n = lang_opts.index(resp)
                self.lang = lang_codes[n]
                txt = f"You had selected {resp} language"
                user_list[chat_id][1] = self.lang
                bot.sendMessage(chat_id, txt)
                self.menu_id = livechat(bot, chat_id, self.username)

        elif self.menu_id == 22:
            if chat_id in chat_list:
                tid = chat_list[ chat_id ]
                bot.sendMessage(tid , resp)
                bot_prompt(bot, chat_id, "(type bye when you want to end the conversation)", [['bye']])
                self.menu_id = 20
            else:
                rlist = resp.split(' ')
                sid = rlist[0]
                if sid.isnumeric():
                    sid = int(sid)
                    if sid in chat_list:
                        retmsg = "User " + rlist[-1] + " is on another conversation."
                    else:
                        self.menu_id = livechat(bot, chat_id, self.username,sid)
                elif resp == option_back:
                    bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                    self.menu_id = 2

        elif self.menu_id == 24:
            if resp in lang_opts:
                n = lang_opts.index(resp)
                self.lang = lang_codes[n]
                txt = f"You had selected {resp} language"
                user_list[chat_id][1] = self.lang
            else:
                txt = "You are back in main menu."
            bot_prompt(bot, chat_id, txt, self.mainmenu)
            self.menu_id = 2

        if retmsg != '':
            bot.sendMessage(chat_id, retmsg)
        return

def livechat(bot, chat_id, user_name, sid = 0):
    global user_list
    if sid > 0:
            user_from = user_list[chat_id][0] + "("  + str(chat_id) + ")"
            tname = user_list[sid][0]
            txt = banner_msg("Live Chat","Hi " + tname + ", you are in the live chat with " + user_from)
            bot.sendMessage(sid,txt)
            chat_list[sid] = chat_id
            chat_list[chat_id] = sid
            user_to = user_list[sid][0] + "("  + str(sid) + ")"
            txt = banner_msg("Live Chat","Hi " + user_name + ", you are in the live chat with " + user_to)
            bot_prompt(bot, chat_id, txt, [['bye']])
            menu_id = 20
    else:
        online_users = [ [str(r) + "     " + user_list[r][0] ] for r in user_list if r != chat_id]
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
    global chat_list, user_list
    chat_found = False
    if chat_id in chat_list:
        chat_found = True
        tid = chat_list[chat_id]
        txt = "Live chat session disconnected. 👋"
        bot.sendMessage(chat_id, txt)
        bot.sendMessage(tid, txt)
        if chat_id in chat_list:
            chat_list.pop(chat_id)
        if tid in chat_list:
            chat_list.pop(tid)
    return chat_found

def peermsg(bot, chat_id,  resp):
    global user_list
    tid = chat_list[chat_id]
    dest_lang = user_list[ tid ][1]
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
            #print("Reading the audio file....")
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

def banner_msg(banner_title, banner_msg):
    txt = "▓▓▓▒▒▒▒▒▒▒░░░  " + banner_title + "  ░░░▒▒▒▒▒▒▒▓▓▓"
    txt += "\n" + banner_msg + "\n"
    return txt

def do_main():
    global user_list, Token, chat_list, run_server
    run_server = True
    user_list = {}
    job_list = []
    chat_list = {}
    err = 0
    echobot = BotInstance(EchoBotToken , True)
    echobot.bot.sendMessage(adminchatid,"Click /start to starts a chat.")
    #print(echobot)
    while run_server:
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
