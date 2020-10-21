#  ___                  _ __  __            _
# / _ \ _ __ ___  _ __ (_)  \/  | ___ _ __ | |_ ___  _ __
#| | | | '_ ` _ \| '_ \| | |\/| |/ _ \ '_ \| __/ _ \| '__|
#| |_| | | | | | | | | | | |  | |  __/ | | | || (_) | |
# \___/|_| |_| |_|_| |_|_|_|  |_|\___|_| |_|\__\___/|_|
#
# Library functions by KW
# MCQ Analytics module
#------------------------------------------------------------------------------------------------------
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import six
import math
import vmsvclib
from vmsvclib import *

debug = lambda x : vmsvclib.debug(x)

class MCQ_Diff():
    def __init__(self):
        self.mcqdf = None
    
    def __str__(self):
        return 'Function that generates statistics to evaluate difficulty of MCQs & Assignments.'
    
    def __repr__(self):
        return 'mcq_analysis()'

    def load_mcqdata(self, client_name , course_id):
        # Loading in the mcq data less the client_name, course_id, student_id from the database
        try:
            query = f"select * from mcq_data where client_name = '{client_name}' and course_id = '{course_id}';"
            mcqdf = rds_df( query )
            mcqdf.columns = get_columns("mcq_data")
            cols = ['score','mcq','qn','attempts']
            self.mcqdf = mcqdf[cols]
        except:
            self.mcqdf = None
        return
    
    # Takes in a database file as an input    
    def top10attempts(self):
        if self.mcqdf is None:
            return None
        df = self.mcqdf.copy()
        
        # Creating a new column that combines MCQ & Question columns for presentation purposes
        # Renaming columns for presentation purposes after groupby
        
        #df['MCQ No. Question No.'] = 'MCQ ' + df['mcq'] + ' Question ' + df['qn']
        df['MCQ No. Question No.'] = df.apply(lambda x : 'MCQ ' + str(x.mcq) + ' Question ' + str(x.qn), axis=1)    
        
        df.columns = ['Average Score %', 'MCQ', 'QN', 'Average Attempts', 'MCQ No. Question No.']
        #df['Average Score %'] = df['Average Score %'] * 100 
  
        # Using groupby to display top 10 questions sorted by average attempts in descending order
        # Can change the number of questions displayed by varying bracketed .head() number
        # Can change rounding significant figures by varying bracketed .roud() number
        df = df.groupby(['MCQ No. Question No.']).mean().round(2).sort_values(by=['Average Attempts'], ascending=False).reset_index().head(10)
        for c in ['MCQ', 'QN']:        
            df[c] = df[c].apply(lambda x : int(x))

        # Generating a png image of the table
        #title_name = "MCQ Analysis Difficulty By MCQ Attempts"
        #table = render_table(df, header_columns=0, col_width=3.5, title_name=title_name)
        #return table
        return df
    
    # Takes in database file as input
    def top10score(self):
        if self.mcqdf is None:
            return None
        df = self.mcqdf.copy()
        
        # Creating a new column that combines MCQ & Question columns for presentation purposes
        # Renaming columns for presentation purposes after groupby
        #df['MCQ No. Question No.'] = 'MCQ ' + df['mcq'] + ' Question ' + df['qn']
        df['MCQ No. Question No.'] = df.apply(lambda x : 'MCQ ' + str(x.mcq) + ' Question ' + str(x.qn), axis=1)
        df.columns = ['Average Score %', 'MCQ', 'QN', 'Average Attempts', 'MCQ No. Question No.']
        #df['Average Score %'] = df['Average Score %'] * 100
        df['Average Score %'] = df.apply(lambda x : float(x['Average Score %'])*100, axis=1)
       
        # Using groupby to display top 10 questions sorted by average score in ascending order
        # Can change the number of questions displayed by varying bracketed .head() number
        # Can change rounding significant figures by varying bracketed .roud() number        
        df = df.groupby(['MCQ No. Question No.']).mean().round(2).sort_values(by=['Average Score %'], ascending=True).reset_index().head(10)
        for c in ['MCQ', 'QN']:        
            df[c] = df[c].apply(lambda x : int(x))
        
        # Generating a png image of the table
        #title_name = "MCQ Analysis Difficulty By MCQ Scores"
        #table = render_table(df, header_columns=0, col_width=3.5, title_name=title_name)        
        #return table
        return df
    
    # Takes in two inputs, mcq number & database file
    def mcq_summary(self, mcq):        
        if self.mcqdf is None:
            return None
        df = self.mcqdf.copy()
        #for c in ['score', 'mcq', 'qn', 'attempts']:
        for c in ['mcq', 'qn']:        
            df[c] = df[c].apply(lambda x : int(x))
        #df['score'] = df['score'].apply(lambda x : x*100)
        
        # Renaming columns for presentation purposes
        df.columns = ['Average Score %', 'MCQ', 'Question', 'Average Attempts']  
       
        # Splicing out the selected MCQ for display
        mcq_df = df[(df['MCQ'] == mcq) & (df['Question'] <= df[(df['MCQ'] == mcq)]['Question'].nunique())].groupby(['Question']).mean().round(2).reset_index().drop('MCQ', axis=1)
        
        # Generating a png image of the table
        #title_name = f"MCQ Analysis Difficulty By MCQ Average for MCQ Test {mcq}"
        #table = render_table(mcq_df, header_columns=0, col_width=3, title_name=title_name)        
        #return table
        return mcq_df

# Tested to be working
if __name__ == '__main__':        
    with open("vmbot.json") as json_file:
            bot_info = json.load(json_file)
    client_name = bot_info['client_name']
    #client_name = 'SambaashDev'
    vmsvclib.rds_connstr = bot_info['omdb']
    vmsvclib.rdscon = None
    vmsvclib.rds_pool = 0
    vmsvclib.rdsdb = None
    mcq_analysis = MCQ_Diff()
    print(mcq_analysis)
    with open("vmbot.json") as json_file:  
        bot_info = json.load(json_file)
    #client_name = bot_info['client_name']
    course_id = "course-v1:Lithan+FOS-1219A+04Dec2019"
    client_name = 'SambaashDev'
    course_id = 'course-v1:Lithan+FOS-0820B+17Aug2020'
    
    mcq_analysis.load_mcqdata(client_name, course_id)
    options = [ 0,1 ]
    if 0 in options:
        df = mcq_analysis.top10attempts()
        #plt.savefig('attempts.png', dpi=100)
        print(df)
        print("top10attempts  completed")

    if 1 in options:
        df = mcq_analysis.top10score()
        #plt.savefig('score.png', dpi=100)
        print(df)
        print("top10score  completed")

    if 2 in options:
        df = mcq_analysis.mcq_summary(10)
        print(df.columns)
        #if photo is not None:
            #plt.savefig('summary.png', dpi=100)
        print(df)    
        print("mcq_summary  completed")

#print(f"End of unit test on MCQ Diff")
