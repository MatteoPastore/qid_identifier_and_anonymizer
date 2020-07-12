import pandas as pd
import csv as csv
import numpy as np
import sys
import os

class ProvinceAllClass():
	def anonymizeProvinceAll(file_path, field):

		file_name = os.path.basename(file_path).split(".")[0]
		data=pd.read_csv(file_path,delimiter=",", dtype={"newId":int ,"anno_nascita":object,"comune_residenza": object, "sesso":object})
		dataProvince=pd.read_csv("provinces.csv",delimiter=",") #encoding="ISO-8859-1"
		merged = data.merge(dataProvince, on=field)
		merged = merged.drop(columns=[field])
		merged = merged.rename(columns={"provincia": field})
		merged.to_csv(file_name+"ProvinceAll.csv", index=False)
