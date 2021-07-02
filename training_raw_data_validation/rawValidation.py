import sqlite3
from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from application_logging.logger import AppLogger

class RawDataValidation:
    """
    This class is used for handling all the validation done on the Raw Training Data!
    """
    def __init__(self,path):
        self.Batch_Directory = path
        self.schema_path = 'schema_training.json'
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
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            file = open("training_logs/valuesfromSchemaValidationLog.txt", 'a+')
            message ="length_of_date_stamp_in_file:: %s" %LengthOfDateStampInFile + "\t" + "length_of_time_stamp_in_file:: %s" % LengthOfTimeStampInFile +"\t " + "number_of_columns:: %s" % NumberofColumns + "\n"
            self.logger.log(file,message)

            file.close()

        except ValueError:
            file = open("training_logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file,"ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("training_logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("training_logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns


    def manual_regex_creation(self):
        """
        Method Name: manual_regex_creation
        Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                    This Regex is used to validate the filename of the training data.
        Output: Regex pattern
        """
        regex = "['ApsFailure']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def create_directory_for_good_bad_raw_data(self):
        """
        Method Name: create_directory_for_good_bad_raw_data
        Description: This method creates directories to store the Good Data and Bad Data
                after validating the training data.
        Output: None
        """
        try:
            path = os.path.join("training_raw_files_validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("training_raw_files_validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("training_logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while creating Directory %s:" % ex)
            file.close()
            raise OSError

    def delete_existing_good_data_training_folder(self):
        """
        Method Name: delete_existing_good_data_training_folder
        Description: This method deletes the directory made  to store the Good Data
                      after loading the data in the table. Once the good files are
                      loaded in the DB,deleting the directory ensures space optimization.
        Output: None
        """
        try:
            path = 'training_raw_files_validated/'
            # if os.path.isdir("ids/" + userName):
            # if os.path.isdir(path + 'Bad_Raw/'):
            #     shutil.rmtree(path + 'Bad_Raw/')
            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
                file = open("training_logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"GoodRaw directory deleted successfully!!!")
                file.close()
        except OSError as s:
            file = open("training_logs/GeneralLog.txt", 'a+')
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
            path = 'training_raw_files_validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file = open("training_logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"BadRaw directory deleted before starting validation!!!")
                file.close()
        except OSError as s:
            file = open("training_logs/GeneralLog.txt", 'a+')
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
            source = 'training_raw_files_validated/Bad_Raw/'
            if os.path.isdir(source):
                path = "training_archived_bad_data"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = 'training_archived_bad_data/BadData_' + str(date)+"_"+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
                file = open("training_logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"Bad files moved to archive")
                path = 'training_raw_files_validated/'
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.logger.log(file,"Bad Raw Data Folder Deleted successfully!!")
                file.close()
        except Exception as e:
            file = open("training_logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise e

    def validation_file_name_raw(self, regex, LengthOfDateStampInFile, LengthOfTimeStampInFile):
        """
        Method Name: validation_file_name_raw
        Description: This function validates the name of the training csv files as per given name in the schema!
                     Regex pattern is used to do the validation.If name format do not match the file is moved
                     to Bad Raw Data folder else in Good raw data.
        Output: None
        """
        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.delete_existing_bad_data_training_folder()
        self.delete_existing_good_data_training_folder()

        onlyfiles = [f for f in listdir(self.Batch_Directory)]
        try:
            # create new directories
            self.create_directory_for_good_bad_raw_data()
            f = open("training_logs/nameValidationLog.txt", 'a+')
            for filename in onlyfiles:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            shutil.copy("training_batch_files/" + filename, "training_raw_files_validated/Good_Raw")
                            self.logger.log(f,"Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            shutil.copy("training_batch_files/" + filename, "training_raw_files_validated/Bad_Raw")
                            self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        shutil.copy("training_batch_files/" + filename, "training_raw_files_validated/Bad_Raw")
                        self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy("training_batch_files/" + filename, "training_raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

            f.close()

        except Exception as e:
            f = open("training_logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            raise e

    def validate_column_length(self, NumberofColumns):
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
            f = open("training_logs/columnValidationLog.txt", 'a+')
            self.logger.log(f,"Column Length Validation Started!!")
            for file in listdir('training_raw_files_validated/Good_Raw/'):
                csv = pd.read_csv("training_raw_files_validated/Good_Raw/" + file)
                if csv.shape[1] == NumberofColumns:
                    pass
                else:
                    shutil.move("training_raw_files_validated/Good_Raw/" + file, "training_raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
            self.logger.log(f, "Column Length Validation Completed!!")
        except OSError:
            f = open("training_logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("training_logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()

    def validate_missing_values_in_whole_column(self):
        """
        Method Name: validate_missing_values_in_whole_column
        Description: This function validates if any column in the csv file has all values missing.
                   If all the values are missing, the file is not suitable for processing.
                   SUch files are moved to bad raw data.
        Output: None
        """
        try:
            f = open("training_logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f,"Missing Values Validation Started!!")

            for file in listdir('training_raw_files_validated/Good_Raw/'):
                csv = pd.read_csv("training_raw_files_validated/Good_Raw/" + file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        shutil.move("training_raw_files_validated/Good_Raw/" + file,
                                    "training_raw_files_validated/Bad_Raw")
                        self.logger.log(f,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                if count==0:
                    csv.to_csv("training_raw_files_validated/Good_Raw/" + file, index=None, header=True)
        except OSError:
            f = open("training_logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("training_logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()