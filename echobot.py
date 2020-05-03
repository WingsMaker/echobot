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

import googletrans
from googletrans import Translator

#---------------------------------------------------------------------------------------------------------------------------------------
import urllib3
proxy_url = "http://proxy.server:3128"
telepot.api._pools = {    'default': urllib3.ProxyManager(proxy_url=proxy_url, num_pools=3, maxsize=10, retries=False, timeout=30),}
telepot.api._onetime_pool_spec = (urllib3.ProxyManager, dict(proxy_url=proxy_url, num_pools=1, maxsize=1, retries=False, timeout=30))
#---------------------------------------------------------------------------------------------------------------------------------------

global chat_list,user_list

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

    def on_chat_message(self, msg):
        content_type, chat_type, chat_id = telepot.glance(msg)
        if content_type == 'text':
            resp = msg['text'].strip()
            if resp == '/start':
                txt = "The chatmode has been removed, please use @echochatbot"
                self.thisbot.sendMessage(chat_id, txt)            
            else:
                result = translator.translate(resp, dest = "en")
                txt = result.text
                self.thisbot.sendMessage(chat_id, txt)            
        return


def do_main():
    global user_list, Token, chat_list
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
