import sqlite3
from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from application_logging.logger import AppLogger

class PredictionDataValidation:
    """
    This class is used for handling all the validation done on the Raw Prediction Data!
    """
    def __init__(self,path):
        self.Batch_Directory = path
        self.schema_path = 'schema_prediction.json'
        self.logger = AppLogger()

    def values_from_schema(self):
        """
        Method Name: values_from_schema
        Description: This method extracts all the relevant information from the pre-defined "Schema" file.
        Output: length_of_date_stamp_in_file, length_of_time_stamp_in_file, column_names, Number of Columns
        """
        try:
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()
            pattern = dic['SampleFileName']
            length_of_date_stamp_in_file = dic['LengthOfDateStampInFile']
            length_of_time_stamp_in_file = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            number_of_columns = dic['NumberofColumns']
            file = open("training_logs/valuesfromSchemaValidationLog.txt", 'a+')
            message ="length_of_date_stamp_in_file:: %s" %length_of_date_stamp_in_file + "\t" + "length_of_time_stamp_in_file:: %s" % length_of_time_stamp_in_file +"\t " + "number_of_columns:: %s" % number_of_columns + "\n"
            self.logger.log(file,message)
            file.close()

        except ValueError:
            file = open("prediction_logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file,"ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("prediction_logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("prediction_logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e

        return length_of_date_stamp_in_file, length_of_time_stamp_in_file, column_names, number_of_columns

    def manual_regex_creation(self):
        """
        Method Name: manual_regex_creation
        Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                    This Regex is used to validate the filename of the Prediction data.
        Output: Regex pattern
        """
        regex = "['ApsFailure']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def create_directory_for_good_bad_raw_data(self):
        """
        Method Name: create_directory_for_good_bad_raw_data
        Description: This method creates directories to store the Good Data and Bad Data
                      after validating the Prediction data.
        Output: None
        """
        try:
            path = os.path.join("prediction_raw_files_validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("prediction_raw_files_validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("prediction_logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while creating Directory %s:" % ex)
            file.close()
            raise OSError

    def delete_existing_good_data_training_folder(self):
        """
        Method Name: delete_existing_good_data_training_folder
        Description: This method deletes the directory made to store the Good Data
                      after loading the data in the table. Once the good files are
                      loaded in the DB,deleting the directory ensures space optimization.
        Output: None
        """
        try:
            path = 'prediction_raw_files_validated/'
            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
                file = open("prediction_logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"GoodRaw directory deleted successfully!!!")
                file.close()
        except OSError as s:
            file = open("prediction_logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError

    def delete_existing_bad_data_training_folder(self):
        """
        Method Name: delete_existing_bad_data_training_folder
        Description: This method deletes the directory made to store the bad Data.
        Output: None
        """
        try:
            path = 'prediction_raw_files_validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file = open("prediction_logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"BadRaw directory deleted before starting validation!!!")
                file.close()

        except OSError as s:
            file = open("prediction_logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError

    def move_bad_files_to_archive_bad(self):
        """
        Method Name: move_bad_files_to_archive_bad
        Description: This method deletes the directory made  to store the Bad Data
                      after moving the data in an archive folder. We archive the bad
                      files to send them back to the client for invalid data issue.
        Output: None
        """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            path= "prediction_archived_bad_data"
            if not os.path.isdir(path):
                os.makedirs(path)
            source = 'prediction_raw_files_validated/Bad_Raw/'
            dest = 'prediction_archived_bad_data/BadData_' + str(date)+"_"+str(time)
            if not os.path.isdir(dest):
                os.makedirs(dest)
            files = os.listdir(source)
            for f in files:
                if f not in os.listdir(dest):
                    shutil.move(source + f, dest)
            file = open("prediction_logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Bad files moved to archive")
            path = 'prediction_raw_files_validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
            self.logger.log(file,"Bad Raw Data Folder Deleted successfully!!")
            file.close()

        except OSError as e:
            file = open("prediction_logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise OSError

    def validation_file_name_raw(self, regex, length_of_date_stamp_in_file, length_of_time_stamp_in_file):
        """
        Method Name: validation_file_name_raw
        Description: This function validates the name of the Prediction csv file as per given name in the schema!
                     Regex pattern is used to do the validation.If name format do not match the file is moved
                     to Bad Raw Data folder else in Good raw data.
        Output: None
        """
        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.delete_existing_bad_data_training_folder()
        self.delete_existing_good_data_training_folder()
        self.create_directory_for_good_bad_raw_data()
        onlyfiles = [f for f in listdir(self.Batch_Directory)]
        try:
            f = open("prediction_logs/nameValidationLog.txt", 'a+')
            for filename in onlyfiles:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == length_of_date_stamp_in_file:
                        if len(splitAtDot[2]) == length_of_time_stamp_in_file:
                            shutil.copy("prediction_batch_files/" + filename, "prediction_raw_files_validated/Good_Raw")
                            self.logger.log(f,"Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            shutil.copy("prediction_batch_files/" + filename, "prediction_raw_files_validated/Bad_Raw")
                            self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        shutil.copy("prediction_batch_files/" + filename, "prediction_raw_files_validated/Bad_Raw")
                        self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy("prediction_batch_files/" + filename, "prediction_raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

            f.close()

        except Exception as e:
            f = open("prediction_logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            raise e

    def validate_column_length(self, number_of_columns):
        """
        Method Name: validate_column_length
        Description: This function validates the number of columns in the csv files.
                     It is should be same as given in the schema file.
                     If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                     If the column number matches, file is kept in Good Raw Data for processing.
                    The csv file is missing the first column name, this function changes the missing name to "Wafer".
        Output: None
        """
        try:
            f = open("prediction_logs/columnValidationLog.txt", 'a+')
            self.logger.log(f,"Column Length Validation Started!!")
            for file in listdir('prediction_raw_files_validated/Good_Raw/'):
                csv = pd.read_csv("prediction_raw_files_validated/Good_Raw/" + file)
                if csv.shape[1] == number_of_columns:
                    csv.to_csv("prediction_raw_files_validated/Good_Raw/" + file, index=None, header=True)
                else:
                    shutil.move("prediction_raw_files_validated/Good_Raw/" + file, "prediction_raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)

            self.logger.log(f, "Column Length Validation Completed!!")
        except OSError:
            f = open("prediction_logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("prediction_logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e

        f.close()

    def delete_prediction_file(self):

        if os.path.exists('prediction_output_file/Predictions.csv'):
            os.remove('prediction_output_file/Predictions.csv')

    def validate_missing_values_in_whole_column(self):
        """
        Method Name: validate_missing_values_in_whole_column
        Description: This function validates if any column in the csv file has all values missing.
               If all the values are missing, the file is not suitable for processing.
               SUch files are moved to bad raw data.
        Output: None
        """
        try:
            f = open("prediction_logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Missing Values Validation Started!!")

            for file in listdir('prediction_raw_files_validated/Good_Raw/'):
                csv = pd.read_csv("prediction_raw_files_validated/Good_Raw/" + file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        shutil.move("prediction_raw_files_validated/Good_Raw/" + file,
                                    "prediction_raw_files_validated/Bad_Raw")
                        self.logger.log(f,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                if count==0:
                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv.to_csv("prediction_raw_files_validated/Good_Raw/" + file, index=None, header=True)
        except OSError:
            f = open("prediction_logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("prediction_logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()