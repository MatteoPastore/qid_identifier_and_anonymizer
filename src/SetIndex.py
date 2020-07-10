import pandas as pd
import random
import numpy as np
import sys
import os
class SetIndexClass():
	def setIndex(file_path):

		file_name = os.path.basename(file_path).split(".")[0]
		a=pd.read_csv(file_path , delimiter=",")
		a=a.dropna()
		a.insert(0,"newId", 1)
		a['newId'] = random.sample(range(len(a)), len(a))

		
		a.to_csv("dataset_newIndex.csv", index=False)
	
