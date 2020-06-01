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
data=pd.read_csv(file_path ,delimiter=",", dtype={"newId":int ,"anno_nascita":object,"comune_residenza": object, "sesso":object})
dataProvincia=pd.read_csv("C:\\Users\\matte\\Downloads\\valle_daosta_provinces_municipalities.csv" , delimiter=",", )
#data2=pd.read_csv("/home/matpas/Scrivania/TesiGennaio/SetIndexCsv/patenti_newIndex.csv",delimiter=",", dtype={"newId":int ,"anno_nascita":float,"comune_residenza": object, "sesso":object})
data.dropna(inplace=True)
k=4
i=0
city=""
dataComune=data["comune_residenza"].value_counts()
#dataComune=dataComune.to_frame()
dataCount = pd.DataFrame(dataComune).reset_index()
dataCount.columns = ['comune_residenza', 'count']

#dataComune.columns=["city", "count"]
#print(dataCount)

while i<len(dataCount):
	
	if int(dataCount.iloc[i]["count"])<k:

		city=dataCount.iloc[i]["comune_residenza"]
		provincia=dataProvincia.loc[dataProvincia['comune_residenza'] == city]
		nameProvincia=provincia.iloc[0]["provincia_residenza"]
		data['comune_residenza'] = data['comune_residenza'].replace(city, nameProvincia )
		#print(data.loc[data["comune_residenza"]==city])
		#print(city)
		#print(data.loc[city])
		#print(provincia)
	i=i+1


data.to_csv(file_name+"ProvinceRari.csv", index=False)
