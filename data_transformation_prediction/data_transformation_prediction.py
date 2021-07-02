import pandas
from os import listdir
from application_logging import logger

class DataTransformPredict:
     """
     This class is used for transforming the Good Raw Training Data before loading it in Database!
     """
     def __init__(self):
          self.goodDataPath = "prediction_raw_files_validated/Good_Raw"
          self.logger = logger.AppLogger()

     def add_quotes_to_string_values_in_column(self):
          """
          Method Name: add_quotes_to_string_values_in_column
          Description: This method replaces the missing values in columns with "NULL" to
                       store in the table. It is using substring in the first column to
                       keep only "Integer" data for ease up the loading.
                       This column will be removed during Prediction
          """
          try:
               log_file = open("prediction_logs/dataTransformLog.txt", 'a+')
               onlyfiles = [f for f in listdir(self.goodDataPath)]
               for file in onlyfiles:
                    data = pandas.read_csv(self.goodDataPath + "/" + file)
                    for column in data.columns:
                         count = data[column][data[column] == 'na'].count()
                         if count != 0:
                              data[column] = data[column].replace('na', "'na'")
                    data.to_csv(self.goodDataPath + "/" + file, index=None, header=True)
                    self.logger.log(log_file, " %s: Quotes added successfully!!" % file)

          except Exception as e:
               log_file = open("prediction_logs/dataTransformLog.txt", 'a+')
               self.logger.log(log_file, "Data Transformation failed because:: %s" % e)
               log_file.close()
               raise e
          log_file.close()