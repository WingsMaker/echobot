# webhook test
# Gaia bot linked to this via webhook
# http://t.me/theia_bot
# https://docs.pyrogram.org/telegram/functions/messages/delete-history
# https://docs.pyrogram.org/topics/advanced-usage
# https://github.com/pyrogram/tgcrypto
# pip install pyrogram
# interface example
# https://api.telegram.org/bot + app.bot_token # /sendMessage?chat_id=___&text=___
from pyrogram import Client, Filters
from pyrogram.api import functions, types
import time 

global stop_server , vars
webhook_user = 404110449

app = Client(
    "vmbot",
    api_id=973000,
    api_hash="c7015ac86de0ff207c936b228abe9678"
)

@app.on_message(Filters.private)
def hello(client, message):
    global stop_server, vars
    chat_id = message.from_user.id
    msg = message.text
    if msg=='/start':
        whoami = app.get_me()
        message.reply_text("Hello {}".format(message.from_user.first_name))
        message.reply_text("this is " + whoami.username)
        message.reply_text('you are '+ str(chat_id) )
        return
    elif msg=='/end':
        #app.send_message("@omnimentor", "Hi, this is Gaia")
        stop_server = True
        message.reply_text("bye")
        return
    elif chat_id==webhook_user :
        message.reply_text('Message from webhook :\n'+ msg )    
        return        
    elif msg=='/demo':
        app.send_message(
            "kimhuat",
            (
                "<b>bold</b>, "
                "<i>italic</i>, "
                "<u>underline</u>, "
                "<s>strike</s>, "
                "<a href=\"tg://user?id=71354936\">KimHuat</a>, "
                "<a href=\"https://pyrogram.org/\">URL</a>, "
                "<code>python example code</code>\n\n"
                "<pre>"
                "for i in range(10):\n"
                "    print(i)"
                "</pre>"
            ),
            parse_mode="html"
        )    
    elif msg=='/vars':
        #txt = str(vars)
        txt = "List of variables :\n"
        for d in vars:
            pval = vars[d]
            txt += d + " : \t\t" + str(pval)
        message.reply_text(txt)
        return
    elif '=' in msg:
        params = msg.split('=')
        pvar = params[0].strip()
        pval = params[1].strip()
        if pval.isnumeric() :
            vars[pvar] = eval(pval)
        else:
            vars[pvar] = pval
    else:
        for d in vars:
            pvar = "vars['" + d + "']"
            msg = msg.replace(d, pvar)
        txt = eval(msg)
        message.reply_text(txt)
    return

#below is the same as app.run() but allow exit condition
stop_server = False
vars = dict()
app.start()
while stop_server==False:
    time.sleep(3)
app.stop()
