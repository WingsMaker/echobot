# _   _                      _ _   _      _                      _    
# | \ | |                    | | \ | |    | |                    | |   
# |  \| | ___ _   _ _ __ __ _| |  \| | ___| |___      _____  _ __| | __
# | . ` |/ _ \ | | | '__/ _` | | . ` |/ _ \ __\ \ /\ / / _ \| '__| |/ /
# | |\  |  __/ |_| | | | (_| | | |\  |  __/ |_ \ V  V / (_) | |  |   < 
# |_| \_|\___|\__,_|_|  \__,_|_|_| \_|\___|\__| \_/\_/ \___/|_|  |_|\_\
#
# Neural Network Feed Forward with Back Propagation Model without tensorflow
#
import pandas as pd
import math
import matplotlib.pyplot as plt
import random
import pickle

class NeuralFFBP():
    def __init__(self):
        self.model_name = "AI-Grading function which uses neural network model"
        self.model = None
        self.ln_rate = 0.05 # learning rate
        self.threshold = 0.025
        self.epoch = 1
        self.datalist = []
        self.rows = 0
        self.inputcols = 0
        self.input_layer = []
        self.hidden_layer = []
        self.output_layer = []
        self.ypred_list = []
        self.yact_list = []
        self.rmse_list = []
        self.total_cost_list = []
        self.total_accuracy_list = []

    def __str__(self):
        return self.model_name

    def __repr__(self):
        return 'NeuralFFBP()'

    def readfromcsv(self, csvfname):
        try:
            df = pd.read_csv(csvfname)
            self.datalist = [list(x) for x in df.values]
            del df
        except:
            pass
        return

    def process_datalist(self):
        self.rows = len(self.datalist)
        if self.rows<=0:
            return
        self.inputcols = len(self.datalist[0])-1
        self.input_layer = [[random.random() for x in range(self.inputcols)] for y in range(self.inputcols)]
        self.hidden_layer = [[random.random() for x in range(self.inputcols)] for y in range(self.inputcols)]
        self.output_layer = [random.random() for x in range(self.inputcols)]
        self.ypred_list = [0 for n in range(self.rows)]
        self.yact_list = [self.datalist[n][0] for n in range(self.rows)]
        return
    
    def sigmoid(self, x):
        return 1 / (1 + math.exp(-x))

    def sumproduct(self, X,Y):
        return sum([X[n]*Y[n] for n in range(len(X))])

    def cost(self, y_act, y_pre):
        return -math.log(y_pre,math.exp(1))*y_act-math.log(1-y_pre,math.exp(1))*(1-y_act)

    def train_data(self, datarow):
        m = self.inputcols
        input_row = datarow[-m:]
        y_act = datarow[0]
        output_inputlayer = [self.sumproduct([ self.input_layer[n][p] for n in range(m)],input_row) for p in range(m)]
        sigmoid_inputlayer = [self.sigmoid(x) for x in output_inputlayer]   # row 27 excel
        output_hiddenlayer = [self.sumproduct([ self.hidden_layer[n][p] for n in range(m)],input_row) for p in range(m)]
        sigmoid_hiddenlayer = [self.sigmoid(x) for x in output_hiddenlayer] # row 31 excel
        sum_list = self.sumproduct(self.output_layer , input_row) # range B33:B35 excel
        y_pred = self.sigmoid(sum_list)
        o_sig_der = y_pred*(1 - y_pred)                               # sigmoid derivative on output layer
        o_err = y_act - y_pred                                        # partial errors on output layer
        o_weight = o_sig_der * o_err                                  # weight change on output layer, range H33:H35
        h_sig_der = [x*(1-x) for x in sigmoid_hiddenlayer]            # sigmoid derivative on hidden layer
        h_err = [x * o_err for x in self.output_layer ]               # partial errors on hidden layer
        h_weight = [h_sig_der[n]*h_err[n] for n in range(m)]          # weight change on hidden layer, range H39:L31
        i_sig_der = [x*(1-x) for x in sigmoid_inputlayer]             # sigmoid derivative on input layer
        i_err = [self.sumproduct(x,h_err) for x in self.hidden_layer] # partial errors on input layer
        i_weight = [i_sig_der[n]*i_err[n] for n in range(m)]          # weight change on input layer, range H25:L27
        adj_output_layer = [x*o_weight for x in sigmoid_hiddenlayer]  # range I20:I23 
        self.output_layer = [adj_output_layer[n]*self.ln_rate + self.output_layer[n]  for n in range(m)] 
        adj_hidden_layer= [[x*y for y in h_weight] for x in sigmoid_inputlayer]  # range I14:I17
        self.hidden_layer = [ [adj_hidden_layer[n][y]*self.ln_rate + self.hidden_layer[n][y] for y in range(m)] for n in range(m)]    
        adj_input_layer= [[x*w for w in i_weight] for x in input_row]            # range I8:I11
        self.input_layer = [ [adj_input_layer[n][y]*self.ln_rate + self.input_layer[n][y] for y in range(m)] for n in range(m)]    
        y_net = sum([self.output_layer[n]*input_row[n] for n in range(m)]) # cell B33
        y_pred = self.sigmoid(y_net)                                  # cell B34
        delta = (y_act - y_pred)*(y_act - y_pred)/2
        ycost = self.cost(y_act,y_pred)
        accuracy = 1 if delta <= self.threshold else 0
        rmse = math.sqrt( o_err*o_err/2 )
        #print(y_act, y_net, y_pred, delta, ycost, accuracy) # range A42:I70
        return (y_pred, ycost, accuracy, rmse)

    def train_model(self):
        self.process_datalist()
        self.total_cost_list = [0 for n in range(self.epoch)]
        self.total_accuracy_list = [0 for n in range(self.epoch)]
        for k in range(self.epoch):
            rmse_list = []            
            for r in range(self.rows):        
                datarow = self.datalist[r]
                (y_pred, ycost, accuracy, rmse) = self.train_data(datarow)
                self.ypred_list[r] = y_pred
                rmse_list.append(rmse)
                self.total_cost_list[k] += ycost
                self.total_accuracy_list[k] += accuracy
            self.total_accuracy_list[k] /= self.rows
            self.rmse_list.append(sum(rmse_list)/self.rows)
            #print(k, self.total_cost_list[k], self.total_accuracy_list[k])
        print("Final model with matrix:")
        print(self.output_layer)        
    
    def model_predict(self, inputs):
        prediction=self.sigmoid(self.sumproduct(self.output_layer, inputs))
        return (1 if (prediction>=1) else (0 if (prediction <=0) else prediction ))

    def view_rmse(self):
        x = [ n+1 for n in range(len(self.rmse_list))]
        plt.xlabel("Iterations")
        plt.ylabel("RMSE")
        plt.title("Is RMSE converging ?")
        plt.plot(x,self.rmse_list)
        plt.show()
        
    def view_costaccuracy(self):
        x = [ n+1 for n in range(self.epoch)]
        plt.xlabel("Iterations")
        plt.ylabel("Cost/Accuracy")
        plt.title("Total Cost and Accuracy")
        plt.plot(x,self.total_cost_list,label = "total cost")
        plt.plot(x,self.total_accuracy_list,label = "total accuracy")
        plt.legend()
        plt.show()

    def view_actual_pred(self):
        x = [ n+1 for n in range(self.rows)]
        plt.xlabel("# of records")
        plt.ylabel("Actual/Predicted Values")
        plt.title("Actual vs Prediction")
        plt.plot(x,self.yact_list,label = "y_act")
        plt.plot(x,self.ypred_list,label = "y_pred")
        plt.legend()
        plt.show()

if __name__ == '__main__':
    nn_model = NeuralFFBP()
    print(nn_model)
    nn_model.ln_rate = 0.05
    nn_model.epoch = 100
    #nn_model.readfromcsv('mcqas_info.csv')
    #nn_model.readfromcsv('grad_eda.csv')
    #zz = """ 
    nn_model.datalist = [[0.84, 1.0, 7.0, 0.88, 1.0],
     [0.92, 1.0, 8.0, 0.84, 1.0],
     [0.93, 1.0, 9.0, 0.87, 2.0],
     [0.94, 1.0, 8.0, 1.0, 3.0],
     [0.93, 1.0, 9.0, 0.91, 3.0],
     [0.95, 1.0, 8.0, 0.87, 4.0],
     [0.92, 0.83, 10.0, 1.0, 4.0],
     [0.92, 1.0, 6.0, 1.0, 5.0],
     [0.93, 0.84, 12.0, 1.0, 5.0],
     [0.91, 1.0, 3.0, 0.99, 6.0],
     [0.95, 1.0, 4.0, 0.96, 6.0],
     [0.82, 0.82, 5.0, 0.84, 6.0],
     [0.93, 0.86, 6.0, 0.97, 6.0],
     [0.92, 0.97, 10.0, 0.94, 7.0],
     [0.91, 1.0, 3.0, 0.94, 8.0],
     [0.91, 1.0, 8.0, 0.81, 8.0],
     [0.98, 0.95, 10.0, 1.0, 8.0],
     [0.97, 0.89, 5.0, 0.98, 9.0],
     [0.95, 1.0, 10.0, 1.0, 9.0],
     [0.97, 0.89, 5.0, 0.99, 10.0],
     [0.93, 0.86, 10.0, 0.98, 10.0],
     [0.94, 1.0, 11.0, 0.96, 10.0],
     [0.92, 0.83, 11.0, 0.99, 11.0],
     [0.91, 1.0, 12.0, 0.9, 11.0],
     [0.93, 0.94, 11.0, 1.0, 12.0],
     [0.93, 0.85, 12.0, 1.0, 12.0],
     [0.93, 1.0, 13.0, 0.92, 12.0],
     [0.93, 0.85, 13.0, 1.0, 13.0]]

    nn_model.train_model()
    with open('ffnn_model.bin', 'wb') as file:
        pickle.dump(nn_model, file)
    #"""
    
    #with open('ffnn_model.bin',"rb") as file:
    #    nn_model = pickle.load(file)

    y_act = 0.9300
    y_pred = nn_model.model_predict([0.85,13,1,13])
    print("\nprediction test using [0.85,13,1,13]: \n", y_act, y_pred)

    # uncomment it if you want to view the charts
    #nn_model.view_rmse()
    #nn_model.view_costaccuracy()
    #nn_model.view_actual_pred()

