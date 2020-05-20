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
import pdftotext
import cv2
import pytesseract 
import pyaudio
import speech_recognition as sr
import gtts
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
option_text2voice = "Text2Voice 🎧"
option_voice2text = "Voice2Text 🎤"

echobot_menu = [[option_chat, option_lang, option_text2voice, option_voice2text, option_back]]

lang_opts = ['English', '简体中文','繁體中文','हिंदी','தமிழ்','বাংলা','Filipino','Indonesian', 'Malay',\
    'မြန်မာ','ไทย','Việt Nam','日本語','한국어', 'Nederlands','Français','Deutsch','Italiano','Español']
lang_codes = ['en','zh-cn','zh-tw','hi','ta','bn','pil','id','ms','my','th','vi','ja','ko','nl','fr','de','it','es']
lang_menu = [  (lang_opts + [option_back])[n*5:][:5] for n in range(4) ]
lang_vopts = ['English', '华语','粤语','हिंदी','தமிழ்','বাংলা','Filipino','Indonesian', 'Malay',\
    'မြန်မာ','ไทย','Việt Nam','日本語','한국어', 'Nederlands','Français','Deutsch','Italiano','Español']
lang_audio = ['en-US','zh-CN','zh-YUE','hi-IN','ta-Sg','bn-BD','fil-PH','id-ID','ms-MY','my-MM','th-TH','vi-VN','ja-JP','ko-KR','nl-NL','fr-FR','de-DE','it-IT','es-ES']
lang_v2t = [(lang_vopts + ['auto'])[n*5:][:5] for n in range(4) ] + [[option_back]]

SvcBotToken = "1231701118:AAGImKeF8SULGP5ktSnsjuUxD7Jg0RRo0Y4"  # @echochatbot
#SvcBotToken = "812577272:AAEgRcGYOGzkN9AoJQKLusspiowlUuGrtj0"    # @OmniMentorBot

class BotInstance():
    def __init__(self, Token):
        self.Token = ""
        self.bot_name = ""
        self.bot_id = ""
        self.bot_running = False
        self.user_list = {}        
        self.chat_list = {}
        self.vars = dict()
        self.Token = Token
        self.mainmenu = echobot_menu
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
        self.lang_v2t = "en-US"
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
        voice2txt = False
        img2txt = False
        pdf2txt = False
        if content_type == 'text':
            resp = msg['text'].strip()
            if 'from' in list(msg):
                username = msg['from']['first_name']
                self.username = username
        elif content_type in ['audio','voice'] and 'audio/' in msg[content_type]['mime_type']:
            fid = msg[content_type]['file_id']
            voice2txt = True
        elif (content_type=="document") :
            ftype = msg['document']['mime_type']
            if ftype=="application/pdf":
                fid = msg[content_type]['file_id']
                pdf2txt = True                
            elif ftype=="image/jpeg":
                print( json.dumps(msg) )
                fid = msg[content_type]['thumb']['file_id']
                img2txt = True
            elif ftype=="audio/x-wav":
                fid = msg[content_type]['file_id']
                voice2txt = True
            else:
                print( json.dumps(msg) )
        elif (content_type=="photo") :
            fid = msg[content_type][0]['file_id']
            img2txt = True
        #elif (content_type=="video") and msg[content_type]['mime_type']=='video/mp4':
        elif (content_type=="video") :
            fid = msg[content_type]['file_id']
            voice2txt = True
        elif (content_type=="video_note") :
            fid = msg[content_type]['file_id']
            voice2txt = True
        elif content_type != "text":
            print( json.dumps(msg) )
            txt = "Thanks for the " + content_type + " but I do not need it for now."
            bot.sendMessage(chat_id,txt)
            return

        if pdf2txt or img2txt or voice2txt:
            fname = get_attachment(bot, fid)

        if pdf2txt:
            resp = readtxt_pdf(fname)
            os.remove(fname)

        if img2txt:
            resp = readtxt_image(fname)
            os.remove(fname)

        if voice2txt:
            if self.lang_v2t=="auto":
                bot.sendMessage(chat_id, "detecting the language...")
            txt = process_voice(fname, self.lang_v2t)
            if txt == '':
                print("Unable to understand the voice")
            else:
                bot.sendMessage(chat_id, txt)
                resp = txt            

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
                txt = "welcome to the translation bot\nPlease select your language:"
                bot_prompt(bot, chat_id, txt, lang_menu)
                self.menu_id = 23
            elif (resp == option_text2voice ) or (resp == '/voice'):                
                self.txt2voice = not self.txt2voice
                retmsg = "text to speech turn " + (" on." if self.txt2voice else "off.")
            elif (resp == option_voice2text):
                txt = "To recognise a voice and translated into following language :"
                bot_prompt(bot, chat_id, txt, lang_v2t)
                self.menu_id = 24
            elif resp == option_back :
                endchat(bot, chat_id)
                self.logoff()
            else:
                dt = translator.detect(resp)                
                if (dt.lang).lower() == self.lang:
                    if self.txt2voice :
                        text2voice(self.bot, self.chatid, self.lang, resp)
                else:                    
                    txt = translate(self.lang,resp)
                    if self.txt2voice :
                        text2voice(self.bot, self.chatid, self.lang, txt)
                    retmsg = txt                

        elif self.menu_id in [3,4]:
            if resp == option_back :
                bot_prompt(bot, chat_id, "You are back in the main menu", svcbot.mainmenu)
                self.menu_id = 2
            elif self.menu_id == 4:
                txt = iesha_chatbot.respond(resp)                                
                txt = "`" + txt + "`"
                bot.sendMessage(chat_id, txt, parse_mode='markdown')

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
            if resp in lang_opts:
                n = lang_opts.index(resp)                
                lang = lang_codes[n]
                self.lang = lang
                svcbot.user_list[chat_id][1] = lang
                txt = f"You had selected {resp} language"
            #elif resp == option_back :
            else:
                txt = "You are back in main menu."
            bot_prompt(bot, chat_id, txt, svcbot.mainmenu)
            self.menu_id = 2

        elif self.menu_id == 24:
            if resp in lang_vopts:
                n = lang_vopts.index(resp)
                lang = lang_audio[n]                    
                self.lang_v2t = lang
                txt = f"You had selected {resp} language"
            elif resp == "auto":
                self.lang_v2t = "auto"
                txt = f"You had selected auto detection."
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
    try:
        if chat_id > 0:
            mark_up = ReplyKeyboardMarkup(keyboard=buttons,one_time_keyboard=True,resize_keyboard=opt_resize)
            sent = bot.sendMessage(chat_id, txt, reply_markup=mark_up)
        else:
            sent = bot.sendMessage(chat_id, txt)
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
        #bot.sendMessage(chat_id, resp)
        mp3 = 'echobot' + str(chat_id) + '.mp3'
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

def wav2txt(wavfile, lang):    
    pass_rate = 0.8
    best_score = pass_rate
    lang_detected = 'en'
    transcript = ""    
    if lang=="auto":
        lang_list = ['en', 'en-UK','en-US','zh-CN','zh-TW', 'zh-YUE','hi-IN','ta-Sg','bn-BD','fil-PH','id-ID','ms-MY','my-MM','th-TH','vi-VN','ja-JP','ko-KR','nl-NL','fr-FR','de-DE','it-IT','es-ES']
    else:
        lang_list = [lang]
    try:
        r = sr.Recognizer()
        if isinstance(r, sr.Recognizer):
            wav = sr.AudioFile(wavfile)
            with wav as source:
                audio = r.record(source)
            for vlang in lang_list:
                score = 0
                txt = ""
                result = r.recognize_google(audio,language=vlang, show_all=True)                    
                if 'alternative' in list(result):
                    txt = result['alternative'][0]['transcript']
                    score =  result['alternative'][0]['confidence']
                    if score >= pass_rate and score > best_score and txt != "":
                        best_score = score
                        lang_detected = vlang
                        transcript = txt
    except:
        print("Error using Recognizer")
        pass
    return (lang_detected, transcript, best_score)

def process_voice(fname, lang):    
    try:
        txt = ""
        if '.wav' in fname:
            wav = fname
        else:
            wav = convert_audio(fname, "wav")
        if wav != "":
            (lang_detected, txt, best_score) = wav2txt(wav, lang)
            os.remove(wav)
        if wav != fname:
            os.remove(fname)
    except:
        pass
    return txt

def readtxt_pdf(fn):
    txt = ""
    try:
        with open(fn, "rb") as f:
            pdf = pdftotext.PDF(f)
        txt = "".join(pdf)
    except:
        txt = "Thanks for the pdf but I am not able read it"        
    return txt

def readtxt_image(fn):
    txt = ""
    try:
        img = cv2.imread(fn)
        txt = pytesseract.image_to_string(img)
    except:
        txt = "Thanks for the image but I am not able read it"        
    return txt

def get_attachment(bot, fid):
    fpath = bot.getFile(fid)['file_path']
    fn = "https://api.telegram.org/file/bot" + bot._token + "/"  + fpath
    fname = wget.download(fn)
    return fname

def banner_msg(banner_title, banner_msg):
    txt = "▓▓▓▒▒▒▒▒▒▒░░░  " + banner_title + "  ░░░▒▒▒▒▒▒▒▓▓▓"
    txt += "\n" + banner_msg + "\n"
    return txt

def do_main():
    global svcbot
    err = 0
    svcbot = BotInstance(SvcBotToken)
    print(svcbot)
    try:
        svcbot.bot.sendMessage(adminchatid,"Click /start to session")
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
