import pandas as pd
import csv as csv
import numpy as np
import sys
import os
arguments = len(sys.argv) - 1
if arguments < 1:
	print ("you must provide the file url")
else:
	file_path = sys.argv[1]
	file_name = os.path.basename(file_path).split(".")[0]
data=pd.read_csv(file_path,delimiter=",", dtype={"newId":int ,"anno_nascita":object,"comune_residenza": object, "sesso":object})
dataProvincia=pd.read_csv("C:\\Users\\matte\\Downloads\\PiemonteComuni.csv",delimiter=",", encoding="ISO-8859-1")
merged = data.merge(dataProvincia, on='comune_residenza')
merged = merged.drop(columns=['comune_residenza'])
merged = merged.rename(columns={"provincia": "comune_residenza"})
merged.to_csv(file_name+"ProvinceAll.csv", index=False)
