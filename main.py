from DataProcessor import DataProcessor
from DataInput import DataInput

di = DataInput()
di.getTBAData('2020vahay')
di.getSheetData('2020vahay')
dp = DataProcessor()
dp.checkData()