import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

#### class for analyzing data
class AnalyzeData():
    def __init__(self, dataFilePath):
        self.data = pd.read_csv(dataFilePath, delimiter=',')
    
    def getData(self):
        return self.data
    
    def showData(self):
        print(self.data)
    
    def dataShape(self):
        print("data shape: ", self.data.shape)
        
    def dropColumn(self, columnName):
        # first column( Unnamed: 0) is same as index column, drop it
        self.data.drop([columnName], axis=1, inplace=True)
        
    def checkNaNValues(self):
        # check if NaN values exist
        print(self.data.isna().sum())
        
    def checkDuplicateValues(self):
        # check if duplicate values exist
        # it will return "false" if no duplicate
        print("Duplicate values: ", self.data.duplicated())
        
    def showDataInfo(self):
        # show data info
        print(self.data.info())
        
    def checkUniqueValues(self):
        # check the number of unique values
        # it shows that the column of createdate has 6690 unique values but we have 8500 values totally.
        print(self.data.apply(lambda x: x.nunique()))
        
        # so drop the same date values in the column of createdate, just keep the first one in it
        self.data = self.data.drop_duplicates('createdate', keep='first')
        
        self.data = self.data.drop_duplicates(['x', 'y'], keep='first')
        
    def checkImbalanceData(self):
        # check imbalanced data. It prints the occurance number of each id_tag
        # and it seems that some of id_tag occurs more than other. Also the difference is huge.
        # so we can do some undersampling/oversampling or both of them.
        # but we dont need to do that in this given task.
        print("Number of values in each id_tag", self.data['id_tag'].value_counts())
        
    def ascendingSort(self, columnName1, columnName2):
        # ascending sort for id_tag and createdate columns
        self.data = self.data.sort_values(by=[columnName1, columnName2])
        
    def dropFirstRow(self):
        # the id_tag of first element in data is 626 and id_region is 210 but there is only one sample.
        # rest of id_tag=626 are in id_region=200
        # so drop it
        self.data = self.data.iloc[1:]
        
    def resetIndex(self):
        # it is detected that the data's index numbers is mixed
        # reset index
        self.data.reset_index(inplace=True)
        self.data.drop(["index"], axis=1, inplace=True)
        
    def arrangeXandYdata(self):
        first_x = self.data["x"][0]
        first_y = self.data["y"][0]
        x_array = []
        y_array = []
        
        for i in range(0, len(self.data)):
            var_id_region = self.data["id_region"][i]
            var_id_tag = self.data["id_tag"][i]
            var_createdate = self.data["createdate"][i]
            var_datacountno = self.data["datacountno"][i]
            var_x = self.data["x"][i]
            var_y = self.data["y"][i]
            var_id_location = self.data["id_location"][i]
        
            if((first_x-7 <= var_x <= first_x+7) and (first_y-7 <= var_y <= first_y+7)):
                x_array.append(var_x)
                y_array.append(var_y)
        
            else:
                if(len(x_array) >= len(y_array)):
                    # index-> gives index of first_x or first_y
                    index = i-len(x_array)
        
                    # average of elements in the x_array
                    x_avg = sum(x_array) / len(x_array)
                    #print("x average: ", x_avg)
        
                    # convert float to int and then write x_avg in column x
                    self.data.at[index, "x"] = int(x_avg)
        
                    # set "?" for the x values that will be deleted
                    self.data.at[index+1: i-1, 'x'] = '?'
        
                else:
                    # index-> gives index of first_x or first_y
                    index = i-len(y_array)
        
                    # average of elements in the x_array
                    y_avg = sum(y_array) / len(y_array)
                    #print("x average: ", y_avg)
        
                    # convert float to int and then write y_avg in column y
                    self.data.at[index, "y"] = int(y_avg)
        
                    # set "?" for the x values that will be deleted
                    self.data.at[index+1: i-1, 'y'] = '?'
        
                x_array = []
                y_array = []
        
                # initialize first_x and first_y again
                first_x = var_x
                first_y = var_y
        
                # append first_x into x_array as a first element
                # append first_y into y_array as a first element
                x_array.append(first_x)
                y_array.append(first_y)
                
    def dropQuestionMarks(self):
        # drop "?" values in column x
        self.data = self.data[self.data.x != "?"]
        
        # drop "?" values in column y
        self.data = self.data[self.data.y != "?"]
        
    def copyData(self):
        data_copy=self.data
        return data_copy
        
    def convertColumnType(self, columnName):
        # convert a column from object to int64
        self.data[columnName] = pd.to_numeric(self.data[columnName])

    def arrangeDateColumn(self):
        # createdate is like this: 2021-09-03 13:57:09.873000+00:00
        # we can
        for i in range(0, len(self.data)):
            var_createdate = self.data["createdate"][i]
            var_createdate = var_createdate[:-6]
            self.data.at[i, "createdate"] = var_createdate
        
        # convert createdate's type (object) to datetime
        self.data['createdate'] = pd.to_datetime(self.data['createdate'])
        
    def addNewColumn(self, columnName):
        # add a new column to data as "second_diff"
        self.data[columnName] = 0
     
    def arrangeFirstElementOfEachIDtag(self):
        first_elements_of_each_idTag = []
        tmp = []
        
        for i in range(0, len(self.data)):
            if(i == len(self.data)-1):
                break
        
            tmp.append(self.data.index.to_series().groupby(self.data['id_tag']).first())
            first_elements_of_each_idTag = tmp[0].tolist()
        
            time1 = self.data["createdate"][i]
            time2 = self.data["createdate"][i+1]
            time_diff = time2-time1
            self.data["second_diff"][i+1] = int(time_diff.total_seconds())
        
        # set second_diff as 0 in the first element of each id_tag
        for i in first_elements_of_each_idTag:
            self.data["second_diff"][i] = 0
            
        return first_elements_of_each_idTag


#### class for visualizing data    
class VisualizeData():
    def __init__(self, data):
        self.data=data
        
    def visualizeIDtag(self, data_copy, start, stop):
        x = self.data["x"].iloc[start:stop]
        y = self.data["y"].iloc[start:stop]
        time = self.data["second_diff"].iloc[start:stop]
        plt.plot(x, y, color='red', marker='o',
                  markersize=4, linestyle='-', linewidth=1)
        for i in range(start, stop):
            #print(time[i])
            plt.text(x[i], y[i], time[i])
        
        plt.show()
        
    def createSample(self, data_copy, first_elements_of_each_idTag):
        
        ### plot a figure for each id_tag because when i have done all of them in just one figure, it was too much complex. So i have done it seperately.
        self.visualizeIDtag(data_copy, first_elements_of_each_idTag[0], first_elements_of_each_idTag[1])
        self.visualizeIDtag(data_copy, first_elements_of_each_idTag[1], first_elements_of_each_idTag[2])
        self.visualizeIDtag(data_copy, first_elements_of_each_idTag[2], first_elements_of_each_idTag[3])
        self.visualizeIDtag(data_copy, first_elements_of_each_idTag[3], first_elements_of_each_idTag[4])



#########################################################################################################################################################╠

dataFilePath = "./interview_test_locations.csv"
analyzeObject=AnalyzeData(dataFilePath)


analyzeObject.dropColumn("Unnamed: 0")
analyzeObject.checkNaNValues()    
analyzeObject.checkDuplicateValues()
analyzeObject.showDataInfo()
analyzeObject.checkUniqueValues()
data_copy= analyzeObject.copyData()
analyzeObject.checkImbalanceData()
analyzeObject.ascendingSort("id_tag", "createdate")
analyzeObject.dropFirstRow()
analyzeObject.resetIndex()
analyzeObject.arrangeXandYdata()
analyzeObject.dropQuestionMarks()
analyzeObject.resetIndex()
analyzeObject.showDataInfo()
analyzeObject.convertColumnType("x")
analyzeObject.arrangeDateColumn()
analyzeObject.showDataInfo()
analyzeObject.addNewColumn("second_diff")
first_elements_of_each_idTag=analyzeObject.arrangeFirstElementOfEachIDtag()    
data=analyzeObject.getData()

visualizeObject=VisualizeData(data)  
visualizeObject.createSample(data_copy, first_elements_of_each_idTag)  
    
    
    
        
        
        
        
        
        
        

        
        
        
        
        
        
        
        
        
        

        