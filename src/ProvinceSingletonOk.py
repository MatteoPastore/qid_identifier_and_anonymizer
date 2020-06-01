import pandas as pd
import csv as csv
import numpy as np
import os
import sys
arguments = len(sys.argv) - 1
if arguments < 1:
  print ("you must provide the file url")
else:
  file_path = sys.argv[1]
  file_name = os.path.basename(file_path).split(".")[0]
dataProvincia=pd.read_csv("C:\\Users\\matte\\Downloads\\PiemonteComuni.csv", delimiter=",", encoding="ISO-8859-1")
columns=os.path.splitext("sesso,anno_nascita,comune_residenza.csv")[0].split(",")
#print(columns)
colnames=[]
for name in columns:
 colnames.append(name)
colnames.append("n")
#print (colnames)
output=pd.read_csv("sesso,anno_nascita,comune_residenza.csv", header=0, names=colnames, delimiter=",",  dtype={"newId":int ,"anno_nascita":object,"comune_residenza": object, "sesso":object})


data_df=pd.read_csv(file_path , dtype={"newId":int ,"anno_nascita":object,"comune_residenza": object, "sesso":object})

i=0

for index, row in output.iterrows():
  #if i%100==0:
  #  print(i)
  anno = row["anno_nascita"]
  comune = row["comune_residenza"]
  sesso = row["sesso"]
 
  data_df.loc[(data_df.anno_nascita==anno) &
    (data_df.comune_residenza==comune) &
    (data_df.sesso==sesso), ["comune_residenza"]] = dataProvincia.loc[dataProvincia.comune_residenza==comune,["provincia"]].values[0]

  
  
  i=i+1

data_df.to_csv(file_name+"ProvinceSingleton.csv", index=False)
