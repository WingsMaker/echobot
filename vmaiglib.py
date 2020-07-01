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
#from sklearn.externals.six import StringIO  
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

    def predict(self, mcqscore, as_score, mcqcnt):
        df = pd.DataFrame({'mcq_avgscore':[mcqscore],'mcq_cnt':[mcqcnt],'as_avgscore':[as_score]})
        return self.model.predict(df)

    def train_model(self, client_name, dumpfile):        
        modelnames = ["LinearRegression", "DecisionTreeRegressor", "AdaBoostRegressor"]
        try:
            mcqinfo = rds_df( f"SELECT * FROM mcqas_info WHERE client_name='{client_name}';")
            features = ['mcq_avgscore', 'mcq_cnt', 'as_avgscore']
            df = mcqinfo[['grade'] + features ]
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
            report = report + 'Decsion Tree Regressor Model Score :' + str(dtr_model_score) + '\n'
            report = report + 'AdaBoost with Decision Tree  Score :' + str(adb_model_score) + '\n'
            m = scoredict[ max( list(scoredict) ) ]
            self.model_name = modelnames[m]
            self.model = models[m]
            report = report + f"\n\nConclusion: {self.model_name} model for prediction.\n"
            # saving the model
            with open(dumpfile, 'wb') as file:
                pickle.dump(self.model, file)                
        except:
            pass
        return report

    def profiler_report(self, client_name, output_file):
        try:
            ok = 1
            mcqinfo = rds_df( f"SELECT * FROM mcqas_info WHERE client_name='{client_name}';")
            features = ['mcq_avgscore', 'mcq_cnt', 'as_avgscore']
            df = mcqinfo[['grade'] + features ]
            pf = ProfileReport(df)
            pf.to_file(output_file)
        except:
            ok = 0
            pass
        return ok

    def tree_graph(self, output_file):
        ok = 0
        if self.model_name=="DecisionTreeRegressor":
            dot_data = StringIO()
            features = ['mcq_avgscore', 'mcq_cnt', 'as_avgscore']
            export_graphviz(self.model, out_file=dot_data,  
                            filled=True, rounded=True,
                            special_characters=True,
                            feature_names = features)  

            graph = pydotplus.graph_from_dot_data(dot_data.getvalue()) 
            if 'write_png' in dir(graph):
                graph.write_png(output_file)
                ok = 1
        return ok

if __name__ == '__main__':
    with open("vmbot.json") as json_file:  
            bot_info = json.load(json_file)
    client_name = bot_info['client_name']
    dt_model = MLGrader()
    print(dt_model)
    opts = [0,2]
    if 0 in opts :
        dt_model.load_model("dt_model.bin")
        print(dt_model.model_name)    

    if 1 in opts :
        txt = dt_model.train_model("mcqinfo.db", "dt_model.bin")
        print(txt)
    
    if 2 in opts :
        if dt_model.model_name != "":
            grad_pred = dt_model.predict(7.9 , 6.2, 13)
            print(grad_pred)

    if 3 in opts :       
        dt_model.profiler_report(client_name, "mcqas_info.html")
        print( repr(dt_model) )

    if 4 in opts :
        dt_model.tree_graph('mcqas_info.jpg')
        print( str(dt_model) )
