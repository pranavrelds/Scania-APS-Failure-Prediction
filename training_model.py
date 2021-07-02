"""
This is the Entry point for Training the Machine Learning Model.
"""

from sklearn.model_selection import train_test_split
from data_ingestion import data_loader
from data_preprocessing import preprocessing
from best_model_finder import tuner
from file_operations import file_methods
from application_logging import logger

class TrainModel:

    def __init__(self):
        self.log_writer = logger.AppLogger()
        self.file_object = open("training_logs/ModelTrainingLog.txt", 'a+')

    def training_model(self):
        # Logging the start of Training
        self.log_writer.log(self.file_object, 'Start of Training')
        try:
            # Getting the data from the source
            data_getter=data_loader.DataGetter(self.file_object, self.log_writer)
            data=data_getter.get_data()

            # data preprocessing
            preprocessor=preprocessing.Preprocessor(self.file_object,self.log_writer)
            #repalcing 'na' values with np.nan
            data = preprocessor.replace_invalid_values_with_null(data)
            # get encoded values for categorical data
            data = preprocessor.encode_categorical_values(data)
            # check if missing values are present in the dataset
            is_null_present,cols_with_missing_values=preprocessor.is_null_present(data)
            # if missing values are there, replace them appropriately.
            if(is_null_present):
                data=preprocessor.handle_missing_values(data) # missing value imputation by mean
            # Get columns with standard deviation zero

            # create separate features and labels
            X, Y = preprocessor.separate_label_feature(data, label_column_name='class')
            cols_to_drop = preprocessor.get_columns_with_zero_std_deviation(X)
            # drop the columns obtained above
            X = preprocessor.remove_columns(X, cols_to_drop)

            # Scaling and pca transform
            X = preprocessor.scale_numerical_columns(X)
            X = preprocessor.pca_transformation(X)

            #handle imbalance in label column
            X,Y = preprocessor.handle_imbalance_data(X, Y)

            # splitting the data into training and test set
            x_train, x_test, y_train, y_test = train_test_split(X, Y, test_size=1 / 3, random_state=0)

            model_finder=tuner.ModelFinder(self.file_object, self.log_writer) # object initialization
            #getting the best model for each of the clusters
            best_model_name,best_model=model_finder.get_best_model(x_train,y_train,x_test,y_test)

            #saving the best model to the directory.
            file_op = file_methods.FileOperation(self.file_object, self.log_writer)
            save_model = file_op.save_model(best_model, best_model_name)

            # logging the successful Training
            self.log_writer.log(self.file_object, 'Successful End of Training')
            self.file_object.close()

        except Exception:
            # logging the unsuccessful Training
            self.log_writer.log(self.file_object, 'Unsuccessful End of Training')
            self.file_object.close()
            raise Exception