from src.utils import dump_csv_file_to_mongodb_collection
from src.exception import SensorException
from src.logger import logging
import os,sys
def storing_record_in_mongo():
    try:
        file_path="/config/workspace/aps_failure_training_set1.csv"
        database_name = "aps_sensor"
        collection_name = "aps_sensor_readings"
        dump_csv_file_to_mongodb_collection(file_path, database_name, collection_name)
    except Exception as e:
        raise e

def test_exception_and_logger():
    try:
        x=1/0
    except Exception as e:
        raise SensorException(e, sys)

if __name__=="__main__":
    try:
        test_exception_and_logger()
    except Exception as e:
        logging.info(f"error: {e}")
        print(e)
   