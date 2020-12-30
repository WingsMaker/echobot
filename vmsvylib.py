#  ___                  _ __  __            _ 
# / _ \ _ __ ___  _ __ (_)  \/  | ___ _ __ | |_ ___  _ __
#| | | | '_ ` _ \| '_ \| | |\/| |/ _ \ '_ \| __/ _ \| '__|
#| |_| | | | | | | | | | | |  | |  __/ | | | || (_) | |
# \___/|_| |_| |_|_| |_|_|_|  |_|\___|_| |_|\__\___/|_|
#
# Library functions for PSS/EOC survey                                      
#------------------------------------------------------------------------------------------------------
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore")

import pandas as pd
import nltk # preloading needed using nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import vmsvclib
from vmsvclib import *

class Omni_Survey():
    def __init__(self):
        self.survey_df = None
        self.table_name = ""
        self.analyzer = SentimentIntensityAnalyzer()

    def __str__(self):
        return "PSS Survey and EOC Survey class module"

    def __repr__(self):
        return 'Omni_Survey()'

    def getcohort(self, x):
        return '-'.join(x.split(':')[1].replace('+','-').split('-')[:-1][1:])

    def index_label(self, x):
        return x.replace('_index','').replace('_',' ').capitalize()

    def load_data(self, client_name , course_id, table_name):
        status = False        
        try:
            cohort_id = self.getcohort(course_id)
            pillar = rds_param(f"select pillar from playbooks where course_id='{course_id}' and client_name='{client_name}';")
            course_code = rds_param(f"select course_code from playbooks where course_id='{course_id}' and client_name='{client_name}';")
            query = f"SELECT * FROM {table_name} WHERE cohort = '{cohort_id}' and pillar = '{pillar}' and qualification = '{course_code}';"
            df = rds_df(query)
            cnt = len(df)
            if (df is None) or (cnt==0):
                txt = "There is no information available at the moment"
                print(txt)
                return status
            cols = get_columns(table_name)
            df.columns = cols            
            self.survey_df = df
            self.table_name = table_name
            status = True
        except:
            self.survey_df = None
        return status

    def feedback_by_index(self):
        if self.survey_df is None:
            txt = "There is no information available at the moment"
            print(txt)
            f = None
            return (f, txt)
        df = self.survey_df
        cohort_id = df.cohort.values[0]
        fn= f"feedback_{cohort_id}.png"
        title = "Overall Feedback Summary : " + cohort_id
        colors = {0:'deepskyblue', 1:'orange', 2:'darkgreen', 3:'red', 4:'yellow', 5:'magenta'}
        clabels = list(colors.keys())
        if self.table_name == "tb_pss":
            surveygroup = [[self.index_label(x),x] for x in df.columns if x.endswith('_index')]
            df1_list = ['studentid'] + [x for x in df.columns if x.endswith('_index')]
            mycolor = [ colors[x] for x in range(len(surveygroup)) ]
        elif self.table_name == "tb_eoc":
            surveygroup = [["Course Satisfaction","overall_index"],
                           ["Learning Summary","learning_index"],
                           ["Summary By Faculty","faculty_index"],
                           ["Summary By Content","content_index"],
                           ["Summary By Support","support_index"],
                           ["Quality of Resources","resource_index"]]
            df1_list = ['studentid', 'course_satisfaction_index', 'refer_others_index',
                'knowledge_acquired_index', 'apply_learning_index', 'confidently_apply_index',
                'expert_faculty_index', 'provide_useful_feedback_index', 'actively_engaged_index',
                'useful_content_index', 'met_objective_index',
                'support_services_index', 'open_to_feedback_index',
                'skills_acquisition', 'skills_utilization','content_vs_duration_index']
            colorscnt = [2, 3, 3, 2, 2, 3]
            mycolor = []
            for n in range(len(colorscnt)):
                mycolor += [ colors[n] for x in range(colorscnt[n])]                
        else:
            surveygroup = []
        tlabels = [x[0] for x in surveygroup]
        df1 = df[df1_list]
        df2 = df[[ x[1] for x in surveygroup ]]
        index_name1 = [self.index_label(x) for x in list(df1.columns)[1:]]
        list_index = list(df1.mean())[1:]
        cnt = len(df1.columns)
        index_name = [ '#'+str(x) for x in range(1,cnt) ]
        fig = plt.figure(figsize=(20, 8))
        plt.title(title, fontsize=30)
        plt.xlabel('Feedback Factors', fontsize=15)
        plt.ylabel('Feedback Index %', fontsize=15)
        plt.xticks(rotation=0)
        ax = plt.gca()
        width = 0.4
        xlen = len(index_name)
        plt.xlim([-width, xlen - width])
        handles = [plt.Rectangle((0,0),1,1, color=colors[label]) for label in clabels]
        plt.legend(handles, tlabels, bbox_to_anchor=(1,1), loc="upper left")
        plt.bar( index_name, list_index, color = mycolor)
        plt.savefig(fn, bbox_inches='tight') 
        plt.clf()
        f = open(fn, 'rb')
        df3 = pd.DataFrame({
            '#' : [ x for x in range(1,cnt) ],
            'Index Name' : index_name1,
            'Score %' : [ str(int(x*10)/10) for x in list_index]
        })
        msg = html_report(df3, df3.columns, [5, 30, 5],20)
        txt = "<b>" + title + "</b>\n" + "<pre>" + msg[0] + "</pre>"
        del df, df1, df2, df3
        return (f, txt)

    def feedback_by_votes(self):
        if self.survey_df is None:
            txt = "There is no information available at the moment"
            print(txt)
            f = None
            return (f, txt)
        df = self.survey_df
        cohort_id = df.cohort.values[0]
        fn= f"rating_{cohort_id}.png"
        title = "Votes counts for cohort : " + cohort_id
        color_list = ['blue', 'green', 'red','yellow','lawngreen']
        if self.table_name == "tb_pss":
            label_list = ['Sufficient','Inadequate','Good', 'Needs improvement','Low']
        elif self.table_name == "tb_eoc":
            label_list = ['Strongly Agree','Moderately Agree','Moderately Disagree',\
                          'Neither Agree/Disagree','Strongly Disagree']
        else:
            label_list = []
        ratings = []
        ratingtable = [[] for x in label_list]
        cnt=len(label_list)
        cols = list(df.columns)
        for col in cols:
            val = df[[col]].values[0][0]
            if type(val) == type('z') and (val in label_list ):
                col_dict=dict(df[[col]].groupby(df[col]).count()[col])
                ratings.append(col)
                for x in range(cnt):
                    y = label_list[x]
                    z = col_dict[y] if y in list(col_dict) else 0 
                    ratingtable[x].append( z )
        list_index = [ x+1 for x in range(len(ratings)) ]
        df_dict  = {'#': list_index }
        df1_dict = {'#': list_index }
        df1_dict['ratings'] = ratings
        for x in range(cnt):
            y = label_list[x]
            rating_sum = sum(ratingtable[x])
            if rating_sum > 0:
                df_dict[y] = ratingtable[x]
            df1_dict[y] = ratingtable[x]
        df4 = pd.DataFrame(df_dict)
        df4.set_index('#').plot(kind='bar',figsize=(20,8), rot = 60, color = color_list)
        cols = ['#' + str(x) for x in list_index]
        plt.title(title, fontsize=30)
        plt.xlabel('Topics')
        plt.ylabel('# of Votes')
        plt.xticks(range(len(cols)), cols, rotation = 0)
        plt.legend( bbox_to_anchor=(1,1), loc="upper left")
        plt.savefig(fn, bbox_inches='tight') 
        plt.clf()
        f = open(fn, 'rb')
        txt = ""
        for x in range(5):
            txt += "ABCDE"[x] + " = " + label_list[x] + "\n"
        df5 = pd.DataFrame(df1_dict)
        df5.columns = ['#','Topics','A','B','C','D','E']  
        msg = html_report(df5, df5.columns, [3,25,3,3,3,3,3],15)
        txt += msg[0]
        txt = "<b>" + title + "</b>\n" + "<pre>" + txt + "</pre>"
        return (f, txt)

    def feedback_by_comments(self):
        msg = []
        if self.survey_df is None:
            txt = "There is no information available at the moment"
            print(txt)
            return msg
        df = self.survey_df
        pos_comments = ["na", "good", "reasonable", "no comment"]
        for x in [x for x in list(df.columns) if 'comment' in x]:
            title = x
            txt = ""
            df['scores'] = df[x].apply(lambda x: self.analyzer.polarity_scores(x))
            df['compound'] = df['scores'].apply(lambda x: x['compound'])
            df_pos=df[df.compound >= 0.5 ][['learner_name',x]]
            m = len(df_pos)
            m = 5 if m>=5 else m
            if m>0:
                for n in range(m):
                    comments = df_pos[x].values[n]
                    if comments.lower() != "na":
                        txt += f"From : {df_pos.learner_name.values[n]} 👍🏽\n{comments}\n\n"
            df_neg=df[(df.compound <= -0.05)].sort_values(['compound'], ascending=[1])[['learner_name',x]]
            m = len(df_neg)
            m = 5 if m>=5 else m
            if m>0:
                for n in range(m):
                    comments = df_neg[x].values[n]
                    if comments.lower() not in pos_comments:
                        txt += f"From : {df_neg.learner_name.values[n]} 👎🏽\n{comments}\n\n"
            if txt != "":
                txt = "<b>" + title + "</b>\n" + "<pre>" + txt + "</pre>"
                msg.append(txt)
        return msg

if __name__ == "__main__":
    rdsconnector = None
    survey_analysis = Omni_Survey()
    print(survey_analysis)    
    with open("vmbot.json") as json_file:  
        bot_info = json.load(json_file)
    vmsvclib.rds_connstr = bot_info['omdb']
    vmsvclib.rdscon = None
    vmsvclib.rds_pool = 0
    vmsvclib.rdsdb = None
    vmsvclib.rds_schema = "omnimentor"
    client_name = "SambaashDev"
    table_name = ""
    course_id = "course-v1:Lithan+CCN-1119A+28Feb20"
    opts = [ 0, 2, 3, 4 ]
    if 0 in opts :
        table_name = "tb_pss"
    if 1 in opts :
        table_name = "tb_eoc"
    if table_name != "":
        print("Loading survey data from ",table_name)
        if survey_analysis.load_data(client_name, course_id, table_name):
            print(f"survey data has been loaded based on {course_id}")
        else:
            print(f"Unable to load survey data at the moment.")
    if 2 in opts :
        if survey_analysis.survey_df is not None:
            ( f, txt ) = survey_analysis.feedback_by_index()
            print(txt)
            if f is not None:
                print("Chart by index created")
    if 3 in opts :
        if survey_analysis.survey_df is not None:
            ( f, txt ) = survey_analysis.feedback_by_votes()
            print(txt)
            if f is not None:
                print("Chart by votes created")
    if 4 in opts :
        if survey_analysis.survey_df is not None:
            msg = survey_analysis.feedback_by_comments()
            if msg != []:
                print(msg)
    print("End of unit test")