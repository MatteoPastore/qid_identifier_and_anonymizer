import pandas as pd
import csv as csv
import numpy as np
import sys
import os


class GenderAllClass():
	def anonymizeGenderAll(file_path, field):

		
		
		file_name = os.path.basename(file_path).split(".")[0]
		data=pd.read_csv(file_path, delimiter=",")


		data[field]=" "

		data.to_csv(file_name+"GenderAll.csv", index=False)
