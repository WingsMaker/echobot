import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore")

import os, sys, time
import telepot
from telepot.loop import MessageLoop
from telepot.namedtuple import ReplyKeyboardMarkup
from telepot.delegate import pave_event_space, per_inline_from_id, create_open
from telepot.namedtuple import InlineQueryResultArticle, InputTextMessageContent
from telepot.helper import IdleEventCoordinator, InlineUserHandler, AnswererMixin
from googletrans import Translator

global echobot

translator = Translator()
adminchatid = 71354936
max_duration = 3600
option_back = "◀️"

lang_opts = ['English', '简体中文','繁體中文','हिंदी','தமிழ்','বাংলা','Filipino','Indonesian', 'Malay',\
    'မြန်မာ','ไทย','Việt Nam','日本語','한국어', 'Nederlands','Français','Deutsch','Italiano','Español']
lang_codes = ['en','zh-cn','zh-tw','hi','ta','bn','tl','id','ms','my','th','vi','ja','ko','nl','fr','de','it','es']

EchoBotToken = "1042610944:AAGme_h2ztEG50jwbW8_cVEi0mgXjkXijd8" # @limkopibot
#EchoBotToken = "812577272:AAEgRcGYOGzkN9AoJQKLusspiowlUuGrtj0"  # @OmniMentorBot

class BotInstance():
    def __init__(self, Token, InlineMode=False):
        self.bot_running = False
        self.bot = telepot.DelegatorBot(Token, [
            pave_event_space()(
                per_inline_from_id(), create_open, QueryCounter, timeout=max_duration),
        ])        
        self.answerer =  telepot.helper.Answerer(self.bot)
        self.bot.thisbot = self.bot
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

    def __str__(self):
        return "TranslationBot by @kimhuat\nUse @"+ self.bot_name

    def __repr__(self):
        return 'BotInstance()'

# https://telepot.readthedocs.io/en/latest/#inline-handler-per-user
class QueryCounter(telepot.helper.InlineUserHandler, telepot.helper.AnswererMixin):
    def __init__(self, *args, **kwargs):
        super(QueryCounter, self).__init__(*args, **kwargs)        

    def on_callback_query(self, msg):
        query_id, from_id, query_data = telepot.glance(msg, flavor='callback_query')

    def on_chosen_inline_result(self, msg):
        result_id, from_id, query_string = telepot.glance(msg, flavor='chosen_inline_result')

    def on_inline_query(self, msg):
        global echobot
        query_id, from_id, query_string = telepot.glance(msg, flavor='inline_query')
        if query_string == "":
            return

        def lang_options():
            cnt = len(lang_opts)
            articles = [{'type': 'article','id': 'L'+str(n+1) ,'title': lang_opts[n] ,'message_text': translator.translate(query_string, dest = lang_codes[n]).text } for n in range(cnt)]
            return articles

        self.answerer.answer(msg, lang_options)
        return

def do_main():
    global echobot
    err = 0
    echobot = BotInstance(EchoBotToken , True)
    try:
        echobot.bot.sendMessage(adminchatid,"Example:\n@" + echobot.bot_name + " your_text")
    except:
        print(echobot)
    while echobot.bot_running :
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
