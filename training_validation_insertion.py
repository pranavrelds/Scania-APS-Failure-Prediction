# from datetime import datetime
from training_raw_data_validation.rawValidation import RawDataValidation
from data_type_validation_insertion_training.data_type_validation_training import DBOperation
from data_transform_training.data_transformation import DataTransform
from application_logging import logger

class TrainValidation:
    def __init__(self,path):
        self.raw_data = RawDataValidation(path)
        self.dataTransform = DataTransform()
        self.dBOperation = DBOperation()
        self.file_object = open("training_logs/Training_Main_Log.txt", 'a+')
        self.log_writer = logger.AppLogger()

    def train_validation(self):
        try:
            self.log_writer.log(self.file_object, 'Start of Validation on files for Prediction!!')
            # extracting values from Prediction schema
            length_of_date_stamp_in_file, length_of_time_stamp_in_file, column_names, no_of_columns = self.raw_data.values_from_schema()
            # getting the regex defined to validate filename
            regex = self.raw_data.manual_regex_creation()
            # validating filename of Prediction files
            self.raw_data.validation_file_name_raw(regex, length_of_date_stamp_in_file, length_of_time_stamp_in_file)
            # validating column length in the file
            self.raw_data.validate_column_length(no_of_columns)
            # validating if any column has all values missing
            self.raw_data.validate_missing_values_in_whole_column()
            self.log_writer.log(self.file_object, "Raw Data Validation Complete!!")

            self.log_writer.log(self.file_object, "Starting Data Transformation!!")
            self.dataTransform.add_quotes_to_string_values_in_column()
            self.log_writer.log(self.file_object, "DataTransformation Completed!!!")
            self.log_writer.log(self.file_object,"Creating training_database and tables on the basis of given schema!!!")
            # create database with given name, if present open the connection! Create table with columns given in schema
            self.dBOperation.create_table_db('Training', column_names)
            self.log_writer.log(self.file_object, "Table creation Completed!!")

            self.log_writer.log(self.file_object, "Insertion of Data into Table started!!!!")
            # insert csv files in the table
            self.dBOperation.insert_into_table_good_data('Training')
            self.log_writer.log(self.file_object, "Insertion in Table completed!!!")
            self.log_writer.log(self.file_object, "Deleting Good Data Folder!!!")
            # Delete the good data folder after loading files in table
            self.raw_data.delete_existing_good_data_training_folder()
            self.log_writer.log(self.file_object, "Good_Data folder deleted!!!")
            self.log_writer.log(self.file_object, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            # Move the bad files to archive folder
            self.raw_data.move_bad_files_to_archive_bad()
            self.log_writer.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            self.log_writer.log(self.file_object, "Validation Operation completed!!")
            self.log_writer.log(self.file_object, "Extracting csv file from table")
            # export data in table to csv file
            self.dBOperation.selecting_data_from_table_into_csv('Training')
            self.file_object.close()

        except Exception as e:
            raise e