import pandas as pd
import random
import numpy as np
import sys
import os
arguments = len(sys.argv) - 1
if arguments < 1:
	print ("you must provide the file url")
else:
	file_path = sys.argv[1]
	file_name = os.path.basename(file_path).split(".")[0]
a=pd.read_csv(file_path , delimiter=",", dtype={"anno_nascita":float,"comune_residenza": object, "sesso":object})
a=a.dropna()
a.insert(0,"newId", 1)
a['newId'] = random.sample(range(len(a)), len(a))

#a.ix(i, "newId")
a.to_csv("dataset_newIndex.csv", index=False)
#print(len(a))


#b.rename(columns={a.columns[0]:"newId"}, inplace=True)

#print(a)
