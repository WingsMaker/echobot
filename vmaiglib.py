#  ___                  _ __  __            _
# / _ \ _ __ ___  _ __ (_)  \/  | ___ _ __ | |_ ___  _ __
#| | | | '_ ` _ \| '_ \| | |\/| |/ _ \ '_ \| __/ _ \| '__|
#| |_| | | | | | | | | | | |  | |  __/ | | | || (_) | |
# \___/|_| |_| |_|_| |_|_|_|  |_|\___|_| |_|\__\___/|_|
#
# Library functions by KH
# This is for AI Grading                                                                      
#------------------------------------------------------------------------------------------------------
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore")

import pandas as pd
from pandas_profiling import ProfileReport
import numpy as np
import pydotplus
import pickle

from sklearn.ensemble import AdaBoostRegressor
from io import StringIO
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import train_test_split
from sklearn.tree import export_graphviz
from sklearn.tree import DecisionTreeRegressor

import vmsvclib
from vmsvclib import *

# ▓▓▓▒▒▒▒▒▒▒░░░  Decision Tree ML Model  ░░░▒▒▒▒▒▒▒▓▓▓
class MLGrader():
    def __init__(self):
        self.model_name = ""
        self.model = None

    def __str__(self):
        return "AI-Grading function which uses decsion tree supervised learning model"

    def __repr__(self):
        return 'MLGrader()'

    def load_model(self, dumpfile):
        try:
        #if True:
            with open(dumpfile,"rb") as file:
                self.model = pickle.load(file)
            self.get_modelname()
        except:
            self.model = None
            self.model_name = ""
        return

    def set_modelname(self, name):
        self.model_name = name
        return

    def get_modelname(self):
        model_dict = self.__dict__
        self.model_name = str(model_dict['model']).split('(')[0]
        return self.model_name

    def predict(self, mcqscore, as_score, mcqcnt, as_cnt=0):
        df = pd.DataFrame({'mcq_avgscore':[mcqscore],'mcq_cnt':[mcqcnt],'as_avgscore':[as_score], 'as_cnt':[as_cnt]})
        return self.model.predict(df)

    def grader_df(self, client_name):
        qry = f"select `value` from params where client_name = '{client_name}' and `key`='email_filter';"
        email_filter = rds_param(qry)
        efilter = email_filter.split(',') if ',' in email_filter else []
        if efilter != []:
            cols = ['grade', 'mcq_avgscore', 'mcq_cnt', 'as_avgscore', 'as_cnt']
            qry = "SELECT a.grade,a.mcq_avgscore,a.mcq_cnt,a.as_avgscore,a.as_cnt FROM mcqas_info a "
            qry += "INNER JOIN user_master b ON a.client_name=b.client_name AND a.studentid=b.studentid "
            qry += f"WHERE a.client_name='{client_name}' "
            qry += ''.join([ " and lower(b.email) not like '%" + x + "'"  for x in efilter])
            qry += f" AND (a.mcq_cnt + a.as_cnt >0);"
            df = rds_df(qry)
            if df is None:
                return None
            df.columns = cols
            features = ['mcq_avgscore', 'mcq_cnt', 'as_avgscore', 'as_cnt']
            return df

    def train_model(self, client_name, dumpfile):
        report = ""
        modelnames = ["LinearRegression", "DecisionTreeRegressor", "AdaBoostRegressor"]
        qry = f"select `value` from params where client_name = '{client_name}' and `key`='email_filter';"
        df = self.grader_df(client_name)
        if df is not None:
            features = ['mcq_avgscore', 'mcq_cnt', 'as_avgscore', 'as_cnt']
            df.to_csv('mlgrader.csv')
            Xr = df[features]
            yr = df.grade.values
            reg_scores = cross_val_score(LinearRegression(), Xr, yr, cv=4)
            report = f"Validation Score for linear regression model: {reg_scores} {np.mean(reg_scores)}\n"
            linreg = LinearRegression().fit(Xr, yr)
            linreg_model_score = linreg.score(Xr,yr)
            report += f"LinearRegression test score : {linreg_model_score}\n"
            dtree_reg = DecisionTreeRegressor(max_depth=4)
            adaboost_reg = AdaBoostRegressor(DecisionTreeRegressor(max_depth=4),
                                n_estimators=300, random_state=8)
            dtree_reg.fit(Xr, yr)
            adaboost_reg.fit(Xr, yr)
            dtr_scores = cross_val_score(dtree_reg, Xr, yr, cv=4)
            dtr_model_score = dtree_reg.score(Xr, yr)
            adb_scores = cross_val_score(adaboost_reg, Xr, yr, cv=4)
            adb_model_score = adaboost_reg.score(Xr, yr)

            models = [ linreg , dtree_reg , adaboost_reg ]
            scoredict = {linreg_model_score:0 , dtr_model_score:1 , adb_model_score:2}

            report = report + '\n'
            report = report + 'Linear Regression      Model Score :' + str(linreg_model_score) + '\n'
            report = report + 'Decision Tree Regressor Model Score :' + str(dtr_model_score) + '\n'
            report = report + 'AdaBoost with Decision Tree  Score :' + str(adb_model_score) + '\n'
            m = scoredict[ max( list(scoredict) ) ]
            self.model_name = modelnames[m]
            self.model = models[m]
            report = report + f"\n\nConclusion: {self.model_name} model for prediction.\n"
            print(report)
            with open(dumpfile, 'wb') as file:
                pickle.dump(self.model, file)
        return report

    def profiler_report(self, client_name, output_file):
        df = self.grader_df(client_name)
        df.to_csv('mlgrader.csv')
        df = pd.read_csv('mlgrader.csv')
        pf = ProfileReport(df)
        pf.to_file(output_file)
        print(output_file," created")

if __name__ == '__main__':
    with open("vmbot.json") as json_file:
            bot_info = json.load(json_file)
    client_name = bot_info['client_name']
    vmsvclib.rds_connstr = bot_info['omdb']
    vmsvclib.rdscon = None
    vmsvclib.rds_pool = 0
    vmsvclib.rdsdb = None
    dt_model = MLGrader()
    print(dt_model)
    opts = [0, 2]
    if 0 in opts :
        dt_model.load_model("dt_model.bin")
        print(dt_model.model_name)

    if 1 in opts :
        txt = dt_model.train_model(client_name, "dt_model_2020.bin")
        print(txt)

    if 2 in opts :
        if dt_model.model_name != "":
            grad_pred = dt_model.predict(0.97 , 4, 10, 4)
            print(grad_pred)

    if 3 in opts :
        dt_model.profiler_report(client_name, "mcqas_info.html")
        print( repr(dt_model) )
