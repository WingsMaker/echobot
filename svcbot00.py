import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore")

import os, sys, time
import telepot
from telepot.loop import MessageLoop
from telepot.namedtuple import ReplyKeyboardMarkup
from googletrans import Translator

global chat_list,user_list

translator = Translator()
adminchatid = 71354936
max_duration = 604800
option_back = "◀️"
lang_opts = ['English', '简体中文','繁體中文','हिंदी','தமிழ்','বাংলা','Filipino','Indonesian', 'Malay',\
    'မြန်မာ','ไทย','Việt Nam','日本語','한국어', 'Nederlands','Français','Deutsch','Italiano','Español']
lang_codes = ['en','zh-cn','zh-tw','hi','ta','bn','tl','id','ms','my','th','vi','ja','ko','nl','fr','de','it','es']
lang_menu = [  (lang_opts + [option_back])[n*5:][:5] for n in range(4) ]

EchoBotToken = "1042610944:AAGme_h2ztEG50jwbW8_cVEi0mgXjkXijd8" # @limkopibot
#EchoBotToken = "812577272:AAEgRcGYOGzkN9AoJQKLusspiowlUuGrtj0"  # @OmniMentor

class BotInstance():
    def __init__(self, Token, InlineMode=False):
        self.Token = ""
        self.bot_name = ""
        self.bot_id = ""
        self.bot_running = False
        self.bot = None
        self.bot = ServiceBot(Token)
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
        self.menu_id = 1
        self.lang = "en"

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
                self.menu_id = 1
            elif resp.startswith('/lang'):
                if resp=='/lang' :
                    txt = "Select the prefered language"
                    bot_prompt(self.thisbot, chat_id, txt, lang_menu)
                    self.menu_id = 2
                else:
                    txt = resp[6:].lower()
                    langs = [x.lower() for x in lang_opts]
                    if txt in langs:
                        n = langs.index(txt)
                        self.lang = lang_codes[n]
                        language = lang_opts[n]
                    else:
                        self.lang = "en"
                        language = lang_opts[0]
                    txt = f"You had selected {language} language"
                    bot_prompt(self.thisbot, chat_id, txt, [])
                    self.menu_id = 1
            elif self.menu_id == 2:
                if resp == option_back:
                    txt = "Languages option closed."
                    bot_prompt(self.thisbot, chat_id, txt, [])
                    self.menu_id = 1
                elif resp in lang_opts:
                    n = lang_opts.index(resp)
                    self.lang = lang_codes[n]
                    language = lang_opts[n]
                    txt = f"You had selected {language} language"
                    bot_prompt(self.thisbot, chat_id, txt, [])
                    self.menu_id = 1
                else:
                    self.lang = "en"
                    language = lang_opts[0]
                    txt = f"You had selected {language} language"
                    bot_prompt(self.thisbot, chat_id, txt, [])
                    self.menu_id = 1
            else:
                result = translator.translate(resp, dest = self.lang)
                txt = result.text
                self.thisbot.sendMessage(chat_id, txt)
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

