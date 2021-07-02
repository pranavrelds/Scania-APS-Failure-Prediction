import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from imblearn.over_sampling import SMOTE
from sklearn.preprocessing import StandardScaler


class Preprocessor:
    """
    This class is used to clean and transform the data before training
    """
    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object

    def remove_columns(self,data,columns):
        """
        Method Name: remove_columns
        Description: This method removes the given columns from a pandas dataframe
        Output: A pandas DataFrame after removing the specified columns
        """
        self.logger_object.log(self.file_object, 'Entered the remove_columns method of the Preprocessor class')
        self.data=data
        self.columns=columns
        try:
            self.useful_data=self.data.drop(labels=self.columns, axis=1) # drop the labels specified in the columns
            self.logger_object.log(self.file_object,'Column removal Successful.Exited the remove_columns method of the Preprocessor class')
            return self.useful_data
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in remove_columns method of the Preprocessor class. Exception message:  '+str(e))
            self.logger_object.log(self.file_object,'Column removal Unsuccessful. Exited the remove_columns method of the Preprocessor class')
            raise Exception()

    def separate_label_feature(self, data, label_column_name):
        """
        Method Name: separate_label_feature
        Description: This method separates the features and a Label columns
        Output: Returns two separate Dataframes, one containing features and the other containing Labels
        """
        self.logger_object.log(self.file_object, 'Entered the separate_label_feature method of the Preprocessor class')
        try:
            self.X=data.drop(labels=label_column_name,axis=1) # drop the columns specified and separate the feature columns
            self.Y=data[label_column_name] # Filter the Label columns
            self.logger_object.log(self.file_object,
                                   'Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class')
            return self.X,self.Y
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in separate_label_feature method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object, 'Label Separation Unsuccessful. Exited the separate_label_feature method of the Preprocessor class')
            raise Exception()

    def replace_invalid_values_with_null(self, data):
        """
        Method Name: is_null_present
        Description: This method replaces 'na' values with np.nan
        """
        data.replace('na', np.NaN, inplace=True)
        return data

    def is_null_present(self,data):
        """
        Method Name: is_null_present
        Description: This method checks whether there are null values present in the pandas Dataframe or not.
        Output: Returns True if null values are present in the DataFrame, False if they are not present and
                returns the list of columns for which null values are present
        """
        self.logger_object.log(self.file_object, 'Entered the is_null_present method of the Preprocessor class')
        self.null_present = False
        self.cols_with_missing_values=[]
        self.cols = data.columns
        try:
            self.null_counts=data.isna().sum() # check for the count of null values per column
            for i in range(len(self.null_counts)):
                if self.null_counts[i]>0:
                    self.null_present=True
                    self.cols_with_missing_values.append(self.cols[i])

            if(self.null_present): # write the logs to see which columns have null values
                self.dataframe_with_null = pd.DataFrame()
                self.dataframe_with_null['columns'] = data.columns
                self.dataframe_with_null['missing values count'] = np.asarray(data.isna().sum())
                self.dataframe_with_null.to_csv('preprocessing_data/null_values.csv') # storing the null column information to file
            self.logger_object.log(self.file_object,'Finding missing values is a success.Data written to the null values file. Exited the is_null_present method of the Preprocessor class')
            return self.null_present, self.cols_with_missing_values

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in is_null_present method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'Finding missing values failed. Exited the is_null_present method of the Preprocessor class')
            raise Exception()

    def encode_categorical_values(self, data):
        """
        Method Name: encode_categorical_values
        Description: This method encodes all the categorical values in the training set.
        Output: A Dataframe which has all the categorical values encoded.
        """
        data['class'] = data['class'].map({'neg': 0, 'pos': 1})
        return data

    def handle_missing_values(self, data):
        """
        Method Name: handle_missing_values
        Description: This method replaces column having missing values > 0.6 with mean of the that particular column
        Output: A Dataframe which has missing value replaced with mean of the column
        """
        data = data[data.columns[data.isnull().mean() < 0.6]]

        data = data.apply(pd.to_numeric)

        for col in data.columns:
            data[col] = data[col].replace(np.NaN, data[col].mean())

        return data

    def pca_transformation(self, X_scaled_data):
        """
        Method Name: pca_transformation
        Description: This method performs pca on the given data and selects 100 initial components
        Output: A dataframe with 100 principal components
        """
        pca = PCA(n_components=100)
        new_data = pca.fit_transform(X_scaled_data)
        principal_x = pd.DataFrame(new_data,index=self.data.index)
        return principal_x

    def scale_numerical_columns(self,data):
        """
        Method Name: scale_numerical_columns
        Description: This method scales the numerical values using the Standard scaler.
        Output: A dataframe with scaled values
        """
        self.logger_object.log(self.file_object,'Entered the scale_numerical_columns method of the Preprocessor class')

        self.data=data
        try:

            self.scaler = StandardScaler()
            self.scaled_data = self.scaler.fit_transform(self.data)
            self.scaled_num_df = pd.DataFrame(data=self.scaled_data, columns=self.data.columns,index=self.data.index)
            self.logger_object.log(self.file_object, 'scaling for numerical values successful. Exited the scale_numerical_columns method of the Preprocessor class')
            return self.scaled_num_df

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in scale_numerical_columns method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object, 'scaling for numerical columns Failed. Exited the scale_numerical_columns method of the Preprocessor class')
            raise Exception()

    def get_columns_with_zero_std_deviation(self,data):
        """
        Method Name: get_columns_with_zero_std_deviation
        Description: This method finds out the columns which have a standard deviation of zero.
        Output: List of the columns with standard deviation of zero
        """
        self.logger_object.log(self.file_object, 'Entered the get_columns_with_zero_std_deviation method of the Preprocessor class')
        self.columns=data.columns
        self.data_n = data.describe()
        self.col_to_drop=[]
        try:
            for x in self.columns:
                if (self.data_n[x]['std'] == 0): # check if standard deviation is zero
                    self.col_to_drop.append(x)  # prepare the list of columns with standard deviation zero
            self.logger_object.log(self.file_object, 'Column search for Standard Deviation of Zero Successful. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')
            return self.col_to_drop

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_columns_with_zero_std_deviation method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object, 'Column search for Standard Deviation of Zero Failed. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')
            raise Exception()

    def handle_imbalance_data(self, X, Y):
        """
        Method Name: handle_imbalance_data
        Description: This method resamples minority class
        Output: balanced data
        """
        sample = SMOTE()
        X_bal, y_bal = sample.fit_resample(X, Y)
        return X_bal,y_bal