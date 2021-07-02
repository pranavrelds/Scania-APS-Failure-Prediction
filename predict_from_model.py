import pandas
from application_logging import logger
from file_operations import file_methods
from data_preprocessing import preprocessing
from data_ingestion import data_loader_prediction
from prediction_raw_data_validation.predictiondatavalidation import PredictionDataValidation

class Prediction:
    def __init__(self,path, path_out):
        self.file_object = open("prediction_logs/Prediction_Log.txt", 'a+')
        self.log_writer = logger.AppLogger()
        self.pred_data_val = PredictionDataValidation(path)
        self.path_out = path_out

    def prediction_from_model(self):

        try:
            self.pred_data_val.delete_prediction_file() #deletes the existing Prediction file from last run!
            self.log_writer.log(self.file_object,'Start of Prediction')
            data_getter=data_loader_prediction.DataGetterPred(self.file_object, self.log_writer)
            data=data_getter.get_data()

            preprocessor=preprocessing.Preprocessor(self.file_object,self.log_writer)

            data = preprocessor.replace_invalid_values_with_null(data)

            is_null_present,cols_with_missing_values=preprocessor.is_null_present(data)
            if(is_null_present):
                data = preprocessor.handle_missing_values(data)  # missing value imputation by mean

            # These columns are created during training and so drop them for prediction
            cols_to_drop = ['cd_000', 'ch_000']

            # drop the columns obtained above
            X = preprocessor.remove_columns(data, cols_to_drop)

            X = preprocessor.scale_numerical_columns(X)

            X = preprocessor.pca_transformation(X)

            file_loader=file_methods.FileOperation(self.file_object, self.log_writer)

            result=[] # initialize blank list for storing predicitons

            model_name = file_loader.find_correct_model_file()
            model = file_loader.load_model(model_name)

            for val in (model.predict(X)):
                result.append(val)
            result = pandas.DataFrame(result,columns=['Predictions'])
            result['Predictions'] = result['Predictions'].map({ 0:'neg', 1 : 'pos'})
            # path= "prediction_output_file/Predictions.csv"
            path = self.path_out
            result.to_csv("prediction_output_file/Predictions.csv",header=True) #appends result to Prediction file
            self.log_writer.log(self.file_object,'End of Prediction')

        except Exception as ex:
            self.log_writer.log(self.file_object, 'Error occured while running the Prediction!! Error:: %s' % ex)
            raise ex

        return path