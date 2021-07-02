import os
from flask import Response
from wsgiref import simple_server
from training_model import TrainModel
from predict_from_model import Prediction
from flask_cors import CORS, cross_origin
import flask_monitoringdashboard as dashboard
from flask import Flask, request, render_template
from training_validation_insertion import TrainValidation
from prediction_validation_insertion import PredictionValidation

os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)
dashboard.bind(app)
CORS(app)

@app.route("/", methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')

@app.route("/predict", methods=['POST'])
@cross_origin()
def predict_route_client():
    try:
        path = request.form.to_dict()
        print(path)
        pred_val = PredictionValidation(path["input"]) #object initialization
        pred_val.prediction_validation() #calling the prediction_validation function
        pred = Prediction(path["input"],path["output"]) #object initialization
        path_op = pred.prediction_from_model()
        return Response("Prediction File created at provided output path!!!")
    except ValueError:
        return Response("Error Occurred! %s" %ValueError)
    except KeyError:
        return Response("Error Occurred! %s" %KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" %e)

@app.route("/train", methods=['POST'])
@cross_origin()
def train_route_client():
    try:
        if request.json['folderPath'] is not None:
            path = request.json['folderPath']
            print(path)
            train_valObj = TrainValidation(path) #object initialization
            train_valObj.train_validation()#calling the training_validation function
            trainModelObj = TrainModel() #object initialization
            trainModelObj.training_model() #training the model for the files in the table
    except ValueError:
        return Response("Error Occurred! %s" % ValueError)
    except KeyError:
        return Response("Error Occurred! %s" % KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" % e)
    return Response("Training successful!!")

port = int(os.getenv("PORT",8001))

if __name__ == "__main__":
    host = '0.0.0.0'
    httpd = simple_server.make_server(host, port, app)
    print("Serving on %s %d" % (host, port))
    httpd.serve_forever()