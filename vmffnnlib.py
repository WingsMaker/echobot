#  ___                  _ __  __            _
# / _ \ _ __ ___  _ __ (_)  \/  | ___ _ __ | |_ ___  _ __
#| | | | '_ ` _ \| '_ \| | |\/| |/ _ \ '_ \| __/ _ \| '__|
#| |_| | | | | | | | | | | |  | |  __/ | | | || (_) | |
# \___/|_| |_| |_|_| |_|_|_|  |_|\___|_| |_|\__\___/|_|
#
# Library functions by QY
# This is for AI Grading v2
#------------------------------------------------------------------------------------------------------
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
import pickle
import vmsvclib
import os
import json

#import tensorflow.compat.v1 as tf
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.python.keras.layers import  Input, Embedding, Dot, Reshape, Dense
from tensorflow.python.keras.models import Model

#from keras import Model
#from keras.models import Sequential, load_model
#from keras.layers import Dense, Dropout

from sklearn import preprocessing
from sklearn.model_selection import train_test_split
from sklearn.tree import export_graphviz


# ▓▓▓▒▒▒▒▒▒▒░░░   Feedforward Neural Network Model  ░░░▒▒▒▒▒▒▒▓▓▓
class NNGrader():
    def __init__(self):
        self.model_name = "FeedForwardNN"        
        self.model = None
        os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
        #logging.getLogger('tensorflow').setLevel(logging.FATAL)    
        #tf.disable_v2_behavior() 
        #print(tf.__version__)  # correction is 1.2.0 ? 2.2.0 ?
        #print(keras.__version__)  # make sure using version 2.0.9 ? 2.2.5 ? 2.3.0 ? 2.4.2 ?

    def __str__(self):
        return "AI-Grading function using feedforward neural network"        

    def __repr__(self):
        return 'NNGrader()'

    def model_loader(self, dumpfile):
        #try:
            #ffnn_model = load_model("ffnn_model.hdf5")
            #self.model = ffnn_model
        #self.model = load_model(dumpfile)
        self.model = tf.keras.models.load_model(dumpfile)
        #except:
            #self.model = None
            #self.model_name = ""
        return


    def pred(self, client_name, mcq_avgscore, mcq_avgattempts, mcq_maxattempts, as_avgscore, as_avgattempts, as_maxattempts, as_cnt):
        """
        This functions predicts the grade of the student given some features. It returns a numpy array.

        For local testing, old_df will be manually inputted hence, it will not be empty
        """
        
        # Read data from database & Convert into Pandas Dataframe
        old_df = vmsvclib.rds_df(f"SELECT * FROM mcqas_info WHERE client_name='{client_name}';")
        if old_df is None:
            print("there is no data found, check if connecton or tunnel issue.")
            return 0

        new_col = {0: 'client_name', 1: 'course_id', 2: 'studentid', 
                    3: 'grade', 4: 'mcq_avgscore', 5: 'mcq_avgattempts', 
                    6: 'mcq_maxattempts', 7: 'mcq_cnt', 8: 'as_avgscore', 
                    9: 'as_avgattempts', 10: 'as_maxattempts', 11: 'as_cnt'}


        # Rename the column names
        old_df.columns = [new_col[c] for c in new_col]                        
        old_df.drop(columns=['client_name'], inplace=True)
        
        
        # Store to-be-predicted student data into dataframe
        demo_data = pd.DataFrame({'mcq_avgscore':[mcq_avgscore],'mcq_avgattempts':[mcq_avgattempts],'mcq_maxattempts':[mcq_maxattempts],
                            'as_avgattempts':[as_avgattempts],'as_avgscore':[as_avgscore], 'as_maxattempts':[as_maxattempts], 
                            'as_cnt':[as_cnt], 'mcq_cnt':[0],'client_name':[client_name],'course_id':['demo']})
        
        new_df = old_df.append(demo_data, ignore_index = True)
        #print(len(new_df))        
        
        # Data Preprocessing
        col_names = ['mcq_avgattempts', 'mcq_cnt' ,'mcq_maxattempts','as_avgattempts', 'as_maxattempts', 'as_cnt']
        
        for col in col_names:
          # Create x, where x the 'scores' column's values as floats
          temp = new_df[[col]].values.astype(float)

          # Create a minimum and maximum processor object
          min_max_scaler = preprocessing.MinMaxScaler()

          # Create an object to transform the data to fit minmax processor
          temp_scaled = min_max_scaler.fit_transform(temp)

          # Extract individual elements into a list
          temp_list = [float(element) for element in temp_scaled]
          
          new_df.drop(col, axis = 1, inplace = True)
          
          new_df[col] = temp_list
        
        # === Assign feature columns as list: feature_cols === #
        feature_cols = ['mcq_avgscore','mcq_avgattempts', 'mcq_maxattempts',
                        'as_avgscore','as_avgattempts', 
                        'as_maxattempts', 'as_cnt']

        X = new_df[feature_cols]
        to_predict = X.iloc[-1]

        predict_df = pd.DataFrame({'mcq_avgscore':[to_predict['mcq_avgscore']],'mcq_avgattempts':[to_predict['mcq_avgattempts']],
                                   'mcq_maxattempts':[to_predict['mcq_maxattempts']],'as_avgattempts':[to_predict['as_avgattempts']],
                                   'as_avgscore':[to_predict['as_avgscore']], 'as_maxattempts':[to_predict['as_maxattempts']],
                                   'as_cnt':[to_predict['as_cnt']]})

        result = None
        result = self.model.predict(predict_df)

        return result[0]

    def train_model(self, client_name, dumpfile):
        """
        This function trains the model from scratch.

        Assumptions: Data input is always the same as current data inputs, if further data preprocessing work is to be done
                     then, the code labeled below as Data Preprocessing would have to be changed accordingly
        """
        modelname = "FeedForwardNN"
        report = ""
        try:
        #if True:
            # Read data from database & Convert into Pandas Dataframe
            df = vmsvclib.rds_df(f"SELECT * FROM mcqas_info WHERE client_name='{client_name}';")
            if df is None:
                report = "there is no data found, check if connecton or tunnel issue."
                print(report)
                return report

            # Read Pandas Dataframe for local testing
            # df = pd.read_csv("mcg_info.csv")

            # Data Preprocessing
            features = ['mcq_avgattempts', 'mcq_cnt' ,'mcq_maxattempts','as_avgattempts', 'as_maxattempts', 'as_cnt']
            
            for feature in features:
                temp = df[[feature]].values.astype(float)

                # Create a minimum and maximum processor object
                min_max_scaler = preprocessing.MinMaxScaler()

                # Create an object to transform the data to fit minmax processor
                temp_scaled = min_max_scaler.fit_transform(temp)

                # Extract individual elements into a list
                temp_list = [float(element) for element in temp_scaled]
                
                df.drop(feature, axis = 1, inplace = True)
                
                df[feature] = temp_list
            
            # === Assign feature columns as list: feature_cols === #
            feature_cols = ['mcq_avgscore','mcq_avgattempts', 'mcq_maxattempts',
                            'as_avgscore','as_avgattempts', 
                            'as_maxattempts', 'as_cnt']

            X = df[feature_cols]

            # === Assign target_y the response variable as Pandas series === #
            y = df['grade']

            X_train, X_test, y_train, y_test = train_test_split(X,y,test_size=0.2)

            X_train = X_train.astype('float32')
            X_test = X_test.astype('float32')

            # FeedForward Neural Network Model
            # from keras.models import Sequential

            model = Sequential()
            model.add(Dense(1028, activation='relu',
                                    input_shape=(X_train.shape[1],)))
            model.add(Dense(512, activation='relu'))
            model.add(Dropout(0.4))
            model.add(Dense(512, activation='relu'))
            model.add(Dense(128, activation='relu'))
            model.add(Dense(32, activation='relu'))
            model.add(Dense(1))
            model.compile(loss='mse', # Cross-entropy
                        optimizer='rmsprop',
                        metrics=['mse'])

            model.fit(X_train, y_train,epochs=100, batch_size=100,verbose=0) # Verbose = 1 if you want to see the training process

            ffnn_loss, ffnn_mse = model.evaluate(X_test, y_test)

            report = f"\n\nConclusion: {self.model_name} model is used for prediction.\nMSE: {ffnn_mse}, Loss: {ffnn_loss}"

            # Saving the model to current directory
            model.save(dumpfile)
        except:
            pass
        return report

    def plot_graph(self, output_file):
        tf.keras.utils.plot_model(self.model, to_file=output_file, show_shapes=True)        
        return 

if __name__ == '__main__':
    with open("vmbot.json") as json_file:  
        bot_info = json.load(json_file)
    client_name = bot_info['client_name']
    nn_model = NNGrader()
    print(nn_model)
    dumpfile = "ffnn_model.hdf5"
    opts = [ 0 , 2]
    if 0 in opts :        
        nn_model.model_loader(dumpfile)
        print(nn_model.model_name)    

    if 1 in opts :
        txt = nn_model.train_model(client_name, dumpfile)
        print(txt)
    
    if 2 in opts :
        if nn_model.model_name != "":
            # mcq_avgscore, mcq_avgattempts, mcq_maxattempts, as_avgscore, as_avgattempts, as_maxattempts, as_cnt
            #grad_pred = nn_model.pred("Lithan", 0.93, 3.83, 4.0, 1.0, 1.0, 1.0, 3.0) # 0.62
            grad_pred = nn_model.pred("Lithan",0.08, 0.3076923076923077, 4 ,0.7 ,0.07692307692307693, 1, 1)
            print(grad_pred[0])  #   result ==> 0.010611363

    if 3 in opts :
        if nn_model.model_name != "":
            #nn_model.plot_graph("ffnn.jpg")            
            nn_model.model.summary()