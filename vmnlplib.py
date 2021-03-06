#  ___                  _ __  __            _ 
# / _ \ _ __ ___  _ __ (_)  \/  | ___ _ __ | |_ ___  _ __
#| | | | '_ ` _ \| '_ \| | |\/| |/ _ \ '_ \| __/ _ \| '__|
#| |_| | | | | | | | | | | |  | |  __/ | | | || (_) | |
# \___/|_| |_| |_|_| |_|_|_|  |_|\___|_| |_|\__\___/|_|
#
# NLP module using FastText                        
#------------------------------------------------------------------------------------------------------
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore")

import pandas as pd
import string, os, re
import fasttext
import random
import csv
import pickle
import nltk
from sklearn.metrics.pairwise import cosine_similarity
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.stem import WordNetLemmatizer
from sklearn.feature_extraction.text import TfidfVectorizer
import contextlib
import vmsvclib
from vmsvclib import *

fasttext.FastText.eprint = print

# ▓▓▓▒▒▒▒▒▒▒░░░  FastText NLP Model  ░░░▒▒▒▒▒▒▒▓▓▓
class NLP_Parser():
    def __init__(self):
        self.model = None
        self.TfidfVec = None
        self.corpus_df = None
        self.qn_resp = None
        self.faq_list = []
        self.regword_list = []

    def __str__(self):
        return "NLP model to generate the top matched response given user input."

    def __repr__(self):
        return 'NLP_Parser()'

    def LemTokens(self, tokens):
        lemmer_nlp = nltk.stem.WordNetLemmatizer()
        return [lemmer_nlp.lemmatize(token) for token in tokens]

    def LemNormalize(self, text):
        remove_punct_dict = dict((ord(punct), None) for punct in string.punctuation)
        return self.LemTokens(nltk.word_tokenize(text.lower().translate(remove_punct_dict)))

    def tokenizer(self, text):
        punct_dict = dict((ord(punct), None) for punct in string.punctuation)
        lemmer = nltk.stem.WordNetLemmatizer()
        LemTokens = lambda tokens : [lemmer.lemmatize(token) for token in tokens]
        return LemTokens(nltk.word_tokenize(text.lower().translate(punct_dict)))

    def stopwords_processor(self, text):
        temp_string = ''
        for word in text:
            temp_string += word
            temp_string += ' '
        return temp_string

    def load_modelfile(self, dumpfile):
        try:
            ok = 0
            vmsvclib.rdscon=vmsvclib.rds_connector()
            df = rds_df("select * from prompts;")
            if df is None:
                errlog("failure reading table : prompts")
                return 0
            df.columns = ['questions', 'resp']
            self.qn_resp = df.copy()

            df = rds_df("select * from ft_corpus;")
            if df is None:
                errlog("failure reading table : ft_corpus")
                return 0
            df.columns = ['label', 'prompt', 'response']
            self.corpus_df = df.copy()

            df1 = rds_df("select * from faq;")
            if df1 is None:
                return 0
            df1.columns = ['questions']

            df2 = rds_df("select * from dictionary;")
            if df2 is None:
                errlog("failure reading table : dictionary")
                return 0
            df2.columns = ['keywords']

            df3 = rds_df("select * from stopwords;")
            if df3 is None:
                errlog("failure reading table : stopwords")
                return 0
            df3.columns = ['keywords']

            self.faq_list = [ x for x in df1.questions ]
            self.regword_list = [x for x in df2.keywords]
            stopwords = [ x for x in df3.keywords ]
            stopwords_processed = self.LemNormalize(self.stopwords_processor(stopwords))
            self.stopwords = stopwords_processed
            TfidfVec = TfidfVectorizer(tokenizer=self.LemNormalize, stop_words=stopwords_processed, ngram_range=(1,2))
            with open(os.devnull, "w") as f, contextlib.redirect_stdout(f):
                ft_model = fasttext.load_model(dumpfile)
            self.model = ft_model
            ok = 1
        except:
            TfidfVec = TfidfVectorizer(tokenizer=self.tokenizer, stop_words='english')
        self.TfidfVec = TfidfVec
        if ok==0:
            errlog("model is incomplete")
        else:
            syslog("model loading completed")
        return

    def load_corpus(self):
        self.corpus_df = rds_df("select * from ft_corpus")

    def create_prompts_corpus(self, input_text):
        if self.model is None:
            errlog("model is incomplete")
            return ""
        if '\n' in input_text:
            inp_txt = ' '.join([txt.strip() for txt in input_text.split('\n')])
        else:
            inp_txt = input_text
        matched_label = self.model.predict(inp_txt, k=1)[0][0]
        df = self.corpus_df
        mask = (df.label == matched_label)
        corp = ''
        for prompt in df[mask]['prompt'].values:
            txt = prompt.replace('.', '. ').replace('?', '? ').replace('!', '! ')
            corp += txt
        return corp

    def get_response(self, input_text):
        if '\n' in input_text:
            inp_txt = ' '.join([txt.strip() for txt in input_text.split('\n')])
        else:
            inp_txt = input_text
        user_input = inp_txt.lower()
        response = ''
        matched_score = 0
        prompts_corpus = self.create_prompts_corpus(inp_txt)
        prompts_sent_token = nltk.sent_tokenize(prompts_corpus)
        prompts_sent_token.append(user_input)
        if self.TfidfVec is not None:
            TfidfVec = self.TfidfVec
            tfidf = TfidfVec.fit_transform(prompts_sent_token)
            cosine_vals = cosine_similarity(tfidf[-1], tfidf)
            flat = cosine_vals.flatten()
            score = 0
            matched_score = 0
            for i in range(0, len(flat)-1, 1):
                if flat[i] >= score:
                    score = flat[i]
                    matched_score = score
            matching_list = list(flat)
            token_match = ""
            if matched_score in matching_list:
                index = matching_list.index(matched_score)
                token_match = prompts_sent_token[index]
                df = self.corpus_df
                try:
                    df1 = df[df['prompt'] == token_match]
                except:
                    df1 = df[(df['prompt'].str.contains(prompts_sent_token[index]))]
                response += [ r for r in df1.response ][0]
                prompts_sent_token.remove(user_input)
        return (response, matched_score)

    def predict_label(self, input_text):
        output = []
        if '\n' in input_text:
            inp_txt = ' '.join([txt.strip() for txt in input_text.split('\n')])
        else:
            inp_txt = input_text
        user_input = inp_txt.lower()
        matched_label = self.model.predict(inp_txt, k=1)[0][0]
        matched_score = self.model.predict(inp_txt, k=1)[1][0]

        output.append((matched_label, matched_score))
        return (matched_label , matched_score)

    def find_matching(self, txt):
        accuracy = 0
        sent_tokens = self.faq_list.copy()
        try:
            sent_tokens.append(txt)
            TfidfVec = TfidfVectorizer(tokenizer=self.tokenizer, stop_words='english')
            tfidf = TfidfVec.fit_transform(sent_tokens)
            vals  = cosine_similarity(tfidf[-1], tfidf)
            idx   = vals.argsort()[0][-2]
            flat  = vals.flatten()
            flat.sort()
            accuracy = flat[-2]
        except:
            pass
        sent_tokens.remove(txt)
        result = ""
        if accuracy > 0:
            result = sent_tokens[idx]
        return (result, accuracy)

    def match_resp( self , user_resp):
        if user_resp == '':
            return ''
        if self.qn_resp is None:
            return ''
        try:
            prompts_qn = [x.lower() for x in self.qn_resp.questions]
            prompts_resp = [x for x in self.qn_resp.resp]
        except:
            prompts_resp = []
            prompts_qn = []
            return ''

        resp = user_resp.lower()
        if resp in prompts_qn:
            n = prompts_qn.index(resp)
            return prompts_resp[n]

        for n in range(len(prompts_qn)):
            qn = re.sub(r"[^a-zA-Z]", " ", prompts_qn[n]).lower().strip().replace("ing","")
            qlist = [ q for q in qn.split(' ') if q in self.regword_list ]
            if len(qlist)>0:
                reg = ' '.join(qlist)
                cond = '^.*' + reg.lower().replace(' ','.*') + '.*$'
                if re.search(cond, resp):
                    return prompts_resp[n]
        return ''

    def recommend_list(self, input_text):
        output = []
        if '\n' in input_text:
            inp_txt = ' '.join([txt.strip() for txt in input_text.split('\n')])
        else:
            inp_txt = input_text
        user_input = inp_txt.lower()

        matched_label = self.model.predict(inp_txt, k=1)[0][0]
        mask = (self.corpus_df.label == matched_label)
        if matched_label=='__label__conversational':
            wlist = [x for x in user_input.split(' ') if x not in self.stopwords]
            if wlist==[]:
                return []
            else:
                first_word = wlist[0]
                mask = (self.corpus_df.prompt.str.contains(first_word))
        df = self.corpus_df[mask]
        questions = [ x for x in df.prompt if x[-1]=='?' ]

        if len(questions) >= 3 :
            return random.sample(questions, 3)
        else:
            return questions

    def train_model(self, model_filename = 'ft_model.bin' ):
        try:
            err = 0
            df = self.corpus_df[['label', 'prompt']]
            df.to_csv('ft_input.csv')
            df1 = pd.read_csv('ft_input.csv')
            df1.to_csv('ft.txt', index=False, sep=' ', header=False, quoting=csv.QUOTE_NONE, quotechar="", escapechar=" ")
            fin = open("ft.txt", "rt")
            fout = open("ft_input.txt", "wt")
            for line in fin:
                fout.write(line.replace('  ', ' '))
            fin.close()
            fout.close()
            model = fasttext.train_supervised('ft_input.txt', epoch=100, lr=0.1, wordNgrams=2, loss='softmax', ws=10)
            model.save_model(model_filename)
        except:
            err = 1
        return ( err == 0)

if __name__ == "__main__":
    try:
        z = nltk.punkt
    except:
        nltk.download('punkt')
        nltk.download('stopwords')
        nltk.download('wordnet')
    rdsconnector = None
    ft_model = NLP_Parser()
    print(ft_model)
    with open("vmbot.json") as json_file:
        bot_info = json.load(json_file)
    vmsvclib.rds_connstr = bot_info['omdb']
    vmsvclib.rdscon = None
    vmsvclib.rds_pool = 0
    vmsvclib.rdsdb = None
    opts = [ 0, 1, 5 , 7 ]
    resp = "I was told it would be when my last contract began?"
    print(resp)
    if 0 in opts :
        print("Loading model from pickle")
        ft_model.load_modelfile("ft_model.bin")
        #faq_list = ft_model.faq_list
        #print( str(faq_list[:5]))
    if 1 in opts :
        (result, score) = ft_model.get_response(resp)
        #print(score, result)
        #txt = ft_model.ft_model(resp)
        print("results : ==>\n", result)
        print(score)
    if 2 in opts :
        ( result, accuracy ) = ft_model.find_matching(resp)
        print(result, accuracy)
    if 3 in opts :
        (result, score) = ft_model.predict_label(resp)
        print(score, result)
    if 4 in opts :
        recommendation = ft_model.recommend_list(resp)
        print(recommendation)
    if 5 in opts :
        result = ft_model.match_resp( resp )
        print(f"result = {result}")
    if 6 in opts :
        if ft_model.corpus_df is None:
            print("Loading corpus from database")
            ft_model.load_corpus()
        ft_model.train_model()
    if 7 in opts :
        from nltk.chat.eliza import eliza_chatbot
        result = eliza_chatbot.respond(resp)
        print(f"result = {result}")
    print("End of unit test")
