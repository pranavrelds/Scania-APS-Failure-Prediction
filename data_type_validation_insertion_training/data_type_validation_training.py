import shutil
import sqlite3
from datetime import datetime
from os import listdir
import os
import csv
from application_logging import logger


class DBOperation:
    """
    This class is used for handling all the SQL operations
    """
    def __init__(self):
        self.path = 'training_database/'
        self.badFilePath = "training_raw_files_validated/Bad_Raw"
        self.goodFilePath = "training_raw_files_validated/Good_Raw"
        self.logger = logger.AppLogger()

    def data_base_connection(self, DatabaseName):
        """
        Method Name: database_connection
        Description: This method creates the database with the given name and if Database already exists then opens the connection to the DB.
        Output: Connection to the DB
        """
        try:
            conn = sqlite3.connect(self.path+DatabaseName+'.db')
            file = open("training_logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Opened %s database successfully" % DatabaseName)
            file.close()

        except ConnectionError:
            file = open("training_logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Error while connecting to database: %s" %ConnectionError)
            file.close()
            raise ConnectionError

        return conn

    def create_table_db(self, DatabaseName, column_names):
        """
        Method Name: create_table_db
        Description: This method creates a table in the given database which will be used to insert the Good data after raw data validation.
        Output: None
        """
        try:
            conn = self.data_base_connection(DatabaseName)
            c=conn.cursor()
            c.execute("SELECT count(name)  FROM sqlite_master WHERE type = 'table'AND name = 'Good_Raw_Data'")
            if c.fetchone()[0] ==1:
                conn.close()
                file = open("training_logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file, "Tables created successfully!!")
                file.close()

                file = open("training_logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file, "Closed %s database successfully" % DatabaseName)
                file.close()

            else:
                for key in column_names.keys():
                    type = column_names[key]

                    #in try block, checking if the table exists, if yes then add columns to the table
                    # else in except block, creating the table
                    try:
                        conn.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key,dataType=type))
                    except:
                        conn.execute('CREATE TABLE  Good_Raw_Data ({column_name} {dataType})'.format(column_name=key, dataType=type))
                conn.close()
                file = open("training_logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file, "Tables created successfully!!")
                file.close()
                file = open("training_logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file, "Closed %s database successfully" % DatabaseName)
                file.close()

        except Exception as e:
            file = open("training_logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()
            conn.close()
            file = open("training_logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DatabaseName)
            file.close()
            raise e

    def insert_into_table_good_data(self, Database):
        """
        Method Name: insert_into_table_good_data
        Description: This method inserts the Good data files from the Good_Raw folder into the
                    above created table.
        Output: None
        """
        conn = self.data_base_connection(Database)
        goodFilePath= self.goodFilePath
        badFilePath = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        log_file = open("training_logs/DbInsertLog.txt", 'a+')

        for file in onlyfiles:
            try:
                with open(goodFilePath+'/'+file, "r") as f:
                    next(f)
                    reader = csv.reader(f, delimiter="\n")
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute('INSERT INTO Good_Raw_Data values ({values})'.format(values=(list_)))
                                self.logger.log(log_file," %s: File loaded successfully!!" % file)
                                conn.commit()
                            except Exception as e:
                                raise e

            except Exception as e:

                conn.rollback()
                self.logger.log(log_file,"Error while creating table: %s " % e)
                shutil.move(goodFilePath+'/' + file, badFilePath)
                self.logger.log(log_file, "File Moved Successfully %s" % file)
                log_file.close()
                conn.close()

        conn.close()
        log_file.close()


    def selecting_data_from_table_into_csv(self, Database):
        """
        Method Name: selecting_data_from_table_into_csv
        Description: This method exports the data in GoodData table as a CSV file. in a given location.
                    above created .
        Output: None
        """
        self.fileFromDb = 'training_file_from_db/'
        self.fileName = 'InputFile.csv'
        log_file = open("training_logs/ExportToCsv.txt", 'a+')
        try:
            conn = self.data_base_connection(Database)
            sqlSelect = "SELECT *  FROM Good_Raw_Data"
            cursor = conn.cursor()

            cursor.execute(sqlSelect)

            results = cursor.fetchall()
            # Get the headers of the csv file
            headers = [i[0] for i in cursor.description]

            #Make the CSV ouput directory
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            # Open CSV file for writing.
            csvFile = csv.writer(open(self.fileFromDb + self.fileName, 'w', newline=''),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')

            # Add the headers and data to the CSV file.
            csvFile.writerow(headers)
            csvFile.writerows(results)

            self.logger.log(log_file, "File exported successfully!!!")
            log_file.close()

        except Exception as e:
            self.logger.log(log_file, "File exporting failed. Error : %s" %e)
            log_file.close()