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
df = pd.read_csv(file_path)


df.drop("newId", axis=1, inplace=True)

df.to_csv(file_name+"NoNewId.csv", index=False)