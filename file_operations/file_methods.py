import os
import pickle
import shutil

class FileOperation:
    """
    This class is used to save the model after training
    and load the saved model for Prediction
    """
    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.model_directory='models/'

    def save_model(self,model,filename):
        """
        Method Name: save_model
        Description: Save the model file to directory
        Outcome: File gets saved
        """
        self.logger_object.log(self.file_object, 'Entered the save_model method of the FileOperation class')
        try:
            path = os.path.join(self.model_directory,filename) #create seperate directory
            if os.path.isdir(path): #remove previously existing models
                shutil.rmtree(self.model_directory)
                os.makedirs(path)
            else:
                os.makedirs(path)
            with open(path +'/' + filename+'.sav',
                      'wb') as f:
                pickle.dump(model, f) # save the model to file
            self.logger_object.log(self.file_object,'Model File '+filename+' saved. Exited the save_model method of the ModelFinder class')

            return 'success'
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in save_model method of the ModelFinder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'Model File '+filename+' could not be saved. Exited the save_model method of the ModelFinder class')
            raise Exception()

    def load_model(self,filename):
        """
        Method Name: load_model
        Description: load the model file to memory
        Output: The Model file loaded in memory
        """
        self.logger_object.log(self.file_object, 'Entered the load_model method of the FileOperation class')
        try:
            with open(self.model_directory + filename + '/' + filename + '.sav',
                      'rb') as f:
                self.logger_object.log(self.file_object,
                                       'Model File ' + filename + ' loaded. Exited the load_model method of the ModelFinder class')
                return pickle.load(f)
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in load_model method of the ModelFinder class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'Model File ' + filename + ' could not be saved. Exited the load_model method of the ModelFinder class')
            raise Exception()

    def find_correct_model_file(self):
        """
        Method Name: find_correct_model_file
        Description: Select the correct model
        Output: The Model file
        """
        self.logger_object.log(self.file_object, 'Entered the find_correct_model_file method of the FileOperation class')
        try:
            self.folder_name=self.model_directory
            self.list_of_model_files = []
            self.list_of_files = os.listdir(self.folder_name)
            for self.file in self.list_of_files:
                try:
                    self.model_name=self.file
                except:
                    continue
            self.model_name=self.model_name.split('.')[0]
            self.logger_object.log(self.file_object,'Exited the find_correct_model_file method of the ModelFinder class.')
            return self.model_name
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in find_correct_model_file method of the ModelFinder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'Exited the find_correct_model_file method of the ModelFinder class with Failure')
            raise Exception()