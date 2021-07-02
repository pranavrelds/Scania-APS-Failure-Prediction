from os import listdir
from application_logging import logger
import pandas as pd


class DataTransform:

     """
     This class is used for transforming the Good Raw Training Data before loading it in Database!
     """
     def __init__(self):
          self.goodDataPath = "training_raw_files_validated/Good_Raw"
          self.logger = logger.AppLogger()

     def add_quotes_to_string_values_in_column(self):
          """
          Method Name: add_quotes_to_string_values_in_column
          Description: This method converts all the columns with string datatype such that
                       each value for that column is enclosed in quotes. This is done
                       to avoid the error while inserting string values in table as varchar
          """
          log_file = open("training_logs/add_quotes_to_string_values_in_column.txt", 'a+')
          try:
               onlyfiles = [f for f in listdir(self.goodDataPath)]
               for file in onlyfiles:
                    data = pd.read_csv(self.goodDataPath+"/" + file)
                    data['class'] = data['class'].apply(lambda x: "'" + str(x) + "'")
                    for column in data.columns:
                         count = data[column][data[column] == 'na'].count()
                         if count != 0:
                              data[column] = data[column].replace('na', "'na'")
                    data.to_csv(self.goodDataPath+ "/" + file, index=None, header=True)
                    self.logger.log(log_file," %s: Quotes added successfully!!" % file)

          except Exception as e:
               self.logger.log(log_file, "Data Transformation failed because:: %s" % e)
               log_file.close()
          log_file.close()