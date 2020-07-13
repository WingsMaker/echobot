# https://chatterbot.readthedocs.io/en/stable/examples.html
# https://pypi.org/project/ChatterBot/
# python3 -m spacy download en

import chatterbot
from chatterbot import ChatBot
from chatterbot.trainers import ChatterBotCorpusTrainer
from chatterbot.trainers import ListTrainer
import pandas as pd
import sqlite3

def querydf(sqldb, query):
    df = None    
    try:        
        conn = sqlite3.connect(sqldb)
        df = pd.read_sql_query(query, conn)
        conn.close()        
    except:
        pass
    return df


conversation = [
    "Hi there!",
    "How are you doing?",
    "I'm doing great.",
    "That is good to hear",
    "Thank you.",
    "You're welcome."
]

chatbot = ChatBot("mychat")
try:        
    conn = sqlite3.connect("omdb.db")
    df = pd.read_sql_query("select * from ft_corpus", conn)
    conn.close()        
    conv_list = [x for x in df.prompt]
    resp_list = [x for x in df.response]
    resp_dict = dict(zip(conv_list,resp_list))
except:
    resp_dict = {}
    conv_list = conversation
finally:
    trainer = ListTrainer(chatbot)
    trainer.train(conv_list)


# Create a new trainer for the chatbot
trainer = ChatterBotCorpusTrainer(chatbot)
# Train the chatbot based on the english corpus
trainer.train("chatterbot.corpus.english")
trainer.train(
    "chatterbot.corpus.english.greetings",
    "chatterbot.corpus.english.conversations"
)

#   
# when is the deadline to submit my skills future?
# when must i submit my sfc?
# can i pay with sfc?
# what is the punishment if i don't complete my assignments by deadline?
# what is the punishment if i don't complete my mcq test by deadline?
while True:
    try:
        qn = input("enter your question:")        
        if qn in conv_list:
            response = resp_dict[qn]            
        else:
            response = chatbot.get_response(qn)            
        print(response)
    except(KeyboardInterrupt, EOFError, SystemExit):
        break    

print("End of test")        