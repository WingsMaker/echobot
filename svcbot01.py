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
import googletrans
from googletrans import Translator
from nltk.chat.iesha  import iesha_chatbot

from vmlkhlib import *
from vmsvclib import *

EchoBotToken = "1231701118:AAGImKeF8SULGP5ktSnsjuUxD7Jg0RRo0Y4"  # @echochatbot
#EchoBotToken = "812577272:AAEgRcGYOGzkN9AoJQKLusspiowlUuGrtj0"    # @OmniMentorBot

translator = Translator()
adminchatid = 71354936
max_duration = 28800
max_rows = 20
option_mainmenu = 'echobot_menu'
option_back = "◀️"
option_lang = "Language 🇸🇬"
option_chat = "Chat 💬"
option_chatlist = "Chat List"
option_chatempty = "Chat Empty"
option_text2voice = "Text2Voice 🎧"
option_voice2text = "Voice2Text 🎤"
option_face = "Face Recognition"
echobot_menu = [[option_chat, option_lang, option_face],[option_text2voice, option_voice2text, option_back]]
option_cv2 = "Train Image Model"
option_img = "Image Detection"
face_menu = [[option_cv2, option_img , option_back]]

lang_opts = ['English', '简体中文','繁體中文','हिंदी','தமிழ்','বাংলা','Filipino','Indonesian', 'Malay',\
    'မြန်မာ','ไทย','Việt Nam','日本語','한국어', 'Nederlands','Français','Deutsch','Italiano','Español']
lang_codes = ['en','zh-cn','zh-tw','hi','ta','bn','pil','id','ms','my','th','vi','ja','ko','nl','fr','de','it','es']
lang_menu = [  (lang_opts + [option_back])[n*5:][:5] for n in range(4) ]
lang_vopts = ['English', '华语','粤语','हिंदी','தமிழ்','বাংলা','Filipino','Indonesian', 'Malay',\
    'မြန်မာ','ไทย','Việt Nam','日本語','한국어', 'Nederlands','Français','Deutsch','Italiano','Español']
lang_audio = ['en-US','zh-CN','zh-YUE','hi-IN','ta-Sg','bn-BD','fil-PH','id-ID','ms-MY','my-MM','th-TH','vi-VN','ja-JP','ko-KR','nl-NL','fr-FR','de-DE','it-IT','es-ES']
lang_v2t = [(lang_vopts + ['auto'])[n*5:][:5] for n in range(4) ] + [[option_back]]

global echobot

piece = lambda txtstr,seperator,pos : txtstr.split(seperator)[pos]

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
        self.keys_dict = {}
        self.keys_dict[option_mainmenu] = 1
        self.define_keys(echobot_menu, self.keys_dict[option_mainmenu])
        self.define_keys(lang_menu, self.keys_dict[option_lang])
        self.define_keys(face_menu, self.keys_dict[option_face])
        self.keys_dict[option_chatlist] = (self.keys_dict[option_chat]*10) + 1
        self.keys_dict[option_chatempty] = (self.keys_dict[option_chat]*10) + 2
         
        #print("\n".join([ str(self.keys_dict[x]) + " "*(16-len(str({self.keys_dict[x]}))) + x for x in list(self.keys_dict)]))

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

    def define_keys(self, telegram_menu, start_key):
        button_list = lambda x : str(x).replace('[','').replace(']','').replace(", ",",").replace("'","").split(',')
        menu_keys = start_key*100 + 1
        for menu_item in button_list(telegram_menu):
            if (menu_item != option_back) and (menu_item != ''):
                self.keys_dict[menu_item] = menu_keys 
                menu_keys += 1
        return 

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
        self.menu_id = 0
        self.lang = "en"
        self.lang_v2t = "en-US"
        self.txt2voice = False
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
        #self.lang = "en"
        return

    def on_close(self, exception):
        self.logoff()
        self.sender.sendMessage('session time out, goodbye.')
        return

    def on_chat_message(self, msg):
        global echobot
        try:
            content_type, chat_type, chat_id = telepot.glance(msg)
            bot = self.bot
            self.chatid = chat_id
            keys_dict = echobot.keys_dict
        except:
            return
        resp = ""
        retmsg = ''
        username = ""
        voice2txt = False
        img2txt = False
        pdf2txt = False
        image_det = False
        video_det = False
        train_img = False
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
            voice2txt = True
        elif (content_type=="document") :
            ftype = msg['document']['mime_type']
            if ftype=="application/pdf":
                fid = msg[content_type]['file_id']
                pdf2txt = True                
            elif ftype=="image/jpeg" or ftype=="image/png":
                fid = msg[content_type]['thumb']['file_id']
                img2txt = True
            elif ftype=="audio/x-wav":
                fid = msg[content_type]['file_id']
                voice2txt = True
            elif ftype=="text/plain":
                fid = msg[content_type]['file_id']
                fname = get_attachment(bot, fid)
                f = open(fname, "r")
                resp = f.read()
                f.close()
                bot.sendMessage(chat_id,resp)
            else:
                print( json.dumps(msg) )
                txt = "Thanks for the " + content_type + " but I do not need it for now."
                bot.sendMessage(chat_id,txt)

        elif (content_type=="photo") :
            fid = msg[content_type][0]['file_id']
            if self.menu_id == keys_dict[option_img] :
                image_det = True
            else:
                img2txt = True

        #elif (content_type=="video") and msg[content_type]['mime_type']=='video/mp4':
        elif (content_type=="video_note") or (content_type=="video") :
            fid = msg[content_type]['file_id']
            if self.menu_id == keys_dict[option_cv2] :
                train_img = True
            elif self.menu_id == keys_dict[option_img] :
                video_det= True            
            else:
                voice2txt = True

        elif content_type != "text":
            print( json.dumps(msg) )
            txt = "Thanks for the " + content_type + " but I do not need it for now."
            bot.sendMessage(chat_id,txt)

        if pdf2txt or img2txt or voice2txt:
            fname = get_attachment(bot, fid)

        if pdf2txt:
            resp = readtxt_pdf(fname)
            os.remove(fname)
            if resp.strip() != "":
                bot.sendMessage(chat_id, resp)

        if img2txt:
            resp = readtxt_image(fname)
            os.remove(fname)
            if resp.strip() != "":
                bot.sendMessage(chat_id, resp)

        if voice2txt:
            if self.lang_v2t=="auto":
                bot.sendMessage(chat_id, "detecting the language...")
            resp = process_voice(fname, self.lang_v2t)
            if resp == '':
                print("Unable to understand the voice")
            else:
                bot.sendMessage(chat_id, resp)

        if image_det:            
            fname = get_attachment(bot, fid)
            (id, acc) = image_detect(fname, 'trainer.yml')
            if id==0:
                txt = "Not able to recognise this image."
            else:
                txt  =f"Telegram ID detected as {str(id)} , accuracy = {round(acc,2)}%"
            bot.sendMessage(chat_id, txt)
            self.menu_id = keys_dict[option_face]
            os.remove(fname)
            return

        if train_img:
            bot.sendMessage(chat_id, "video training in progress..")
            try:
                fname = get_attachment(bot, fid)
                video_training(fname, chat_id)
            except:
                pass
            txt = "video training completed."
            bot.sendMessage(chat_id, txt, face_menu)
            self.menu_id = keys_dict[option_face]
            os.remove(fname)
            return

        if video_det:
            fname = get_attachment(bot, fid)
            trainer = "trainer.yml"
            (id, acc) = video_detect(fname, trainer)
            txt = f"The identity found as {id} , {round(acc,2)}%"
            bot.sendMessage(chat_id, txt, face_menu)
            self.menu_id = keys_dict[option_face]
            os.remove(fname)
            return

        if (resp==option_back) and (self.menu_id  in [keys_dict[x] for x in [option_cv2, option_img]]):
            bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
            self.menu_id = keys_dict[option_mainmenu]
            return

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
            self.parentbot = echobot
            self.mainmenu = self.parentbot.mainmenu
            self.is_admin = True
            self.menu_id = keys_dict[option_mainmenu]
            self.lang = 'en'
            txt = "This is a translation chatbot with voice support.\nPlease select your language."
            bot_prompt(bot, chat_id, txt, self.mainmenu)
            self.parentbot.user_list[chat_id] = [self.username, self.lang]

        elif resp == '/pw':
            retmsg = "".join(random.sample(list(string.ascii_lowercase)+list(string.ascii_uppercase)+list(string.digits)+list(string.punctuation),8))

        elif resp.startswith("/d:"):
            retmsg = decrypt(resp[3:])

        elif resp.startswith("/e:"):
            retmsg = encrypt(resp[3:])

        elif chat_id in self.parentbot.chat_list and (resp.strip() != "") :
            tid = self.parentbot.chat_list[chat_id]
            if resp.lower() == 'bye':
                endchat(bot, self.parentbot, chat_id)
                bot_prompt(bot, chat_id, "You are back to the main menu", self.mainmenu)
                self.menu_id = keys_dict[option_mainmenu]
            elif resp == option_lang :
                txt = "Select the prefered language"
                bot_prompt(bot, chat_id, txt, lang_menu)
                self.menu_id = keys_dict[option_lang]
            elif resp in self.mainmenu[0]:
                bot_prompt(bot, chat_id, "you are in a livechat.", [ ['bye'] ] )
                self.menu_id = keys_dict[option_chat]
            else:
                if self.menu_id != keys_dict[option_chat]:
                    bot_prompt(bot, chat_id, "you are in a livechat now.", [ ['bye'] ] )
                    self.menu_id=20
                peermsg(bot, self.parentbot, chat_id, resp)

        elif self.menu_id == 0:
            retmsg = translate(self.lang,resp)

        elif self.menu_id == keys_dict[option_mainmenu]:
            if chat_id in self.parentbot.chat_list:
                tid = self.parentbot.chat_list[ chat_id ]
                bot.sendMessage(tid , resp)
                bot_prompt(bot, chat_id, "(type bye when you want to end the conversation)", [['bye']])
                self.menu_id = keys_dict[option_chat]
            elif (resp == option_chat ) or (resp == '/chat'):
                self.menu_id = livechat(bot, self.parentbot, chat_id, self.username)
            elif (resp == option_lang ) or (resp == '/lang'):
                txt = "welcome to the translation bot\nPlease select your language:"
                bot_prompt(bot, chat_id, txt, lang_menu)
                self.menu_id = keys_dict[option_lang]
            elif (resp == option_text2voice ) or (resp == '/voice'):                
                self.txt2voice = not self.txt2voice
                retmsg = "text to speech turn " + (" on." if self.txt2voice else "off.")
            elif (resp == option_voice2text):
                txt = "To recognise a voice and translated into following language :"
                bot_prompt(bot, chat_id, txt, lang_v2t)
                self.menu_id = keys_dict[option_voice2text]
            elif resp == option_face :
                txt = "This section is for face recognition."
                bot_prompt(bot, chat_id, txt, face_menu)
                self.menu_id = keys_dict[option_face]
            elif resp == option_back :
                endchat(bot, self.parentbot, chat_id)
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

        elif self.menu_id == keys_dict[option_chatempty] :
            if resp == option_back :
                bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                self.menu_id = keys_dict[option_mainmenu]
            else:
                txt = iesha_chatbot.respond(resp)
                txt = "`" + txt + "`"
                bot.sendMessage(chat_id, txt, parse_mode='markdown')

        elif self.menu_id == keys_dict[option_chat] :
            if resp.lower() == 'bye':
                endchat(bot, self.parentbot, chat_id)
                bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                self.menu_id = keys_dict[option_mainmenu]
            else:
                peermsg(bot, self.parentbot, chat_id, resp)

        elif self.menu_id == keys_dict[option_lang] :
            if resp in lang_opts:
                n = lang_opts.index(resp)
                self.lang = lang_codes[n]
                txt = f"You had selected {resp} language"
                self.parentbot.user_list[chat_id][1] = self.lang
                #self.sender.sendMessage(txt)
            else:
                txt = "You are back in the main menu"
            bot_prompt(bot, chat_id, txt, self.mainmenu)                
            self.menu_id = keys_dict[option_mainmenu]
            #self.menu_id = livechat(bot, self.parentbot, chat_id, self.username)

        elif self.menu_id == keys_dict[option_chatlist] :
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

        elif self.menu_id == keys_dict[option_lang] :
            if resp in lang_opts:
                n = lang_opts.index(resp)                
                lang = lang_codes[n]
                self.lang = lang
                self.parentbot.user_list[chat_id][1] = lang
                txt = f"You had selected {resp} language"
            else:
                txt = "You are back in main menu."
            bot_prompt(bot, chat_id, txt, self.mainmenu)
            self.menu_id = keys_dict[option_mainmenu]

        elif self.menu_id == keys_dict[option_voice2text] :
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
            bot_prompt(bot, chat_id, txt, self.mainmenu)
            self.menu_id = keys_dict[option_mainmenu]

        elif self.menu_id == keys_dict[option_face] :
            if resp == option_back :
                bot_prompt(bot, chat_id, "You are back in the main menu", self.mainmenu)
                self.menu_id = keys_dict[option_mainmenu]
            elif resp == option_cv2 :
                txt = "To train the image model, please upload video note of yourself."
                bot.sendMessage(chat_id, txt)
                self.menu_id = keys_dict[option_cv2]
            elif resp == option_img :
                txt = "To test the image detection, please upload a photo/video of yourself."
                bot.sendMessage(chat_id, txt)
                self.menu_id = keys_dict[option_img]

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
            menu_id = parentbot.keys_dict[option_chat]
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
    dest_lang = parentbot.user_list[ tid ][1]
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

def do_main():
    global echobot
    err = 0
    echobot = BotInstance(EchoBotToken)
    print(echobot)
    #echobot.bot.sendMessage(adminchatid,"Click /start to connect the ServiceBot")
    while echobot.bot_running:
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
