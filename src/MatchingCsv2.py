import pandas as pd
import sys
import os
arguments = len(sys.argv) - 1
if arguments < 1:
	print ("you must provide the file url")
else:
	file_path = sys.argv[1]
	file_name = os.path.basename(file_path).split(".")[0]
a = pd.read_csv("C:\\Users\\matte\\Desktop\\Framework\\dataset_newIndex.csv")
b = pd.read_csv(file_path)
#b = pd.read_csv("C:\\Users\\matte\\Desktop\\TesiGennaio\\quasi_identifier_hunter-master\\patenti__dataset\\ValleAosta\\GenereSingletonValleAosta.csv")
merged = a.merge(b, on='newId')
merged.to_csv("merged.csv", index=False)

deletedLines=0
modifiedLines=0

i=0
while i<len(merged):
	if i%10000==0:
		print(i)
	if merged.iloc[i]['anno_nascita_x']!= merged.iloc[i]["anno_nascita_y"] or merged.iloc[i]['comune_residenza_x']!= merged.iloc[i]["comune_residenza_y"]  or  merged.iloc[i]['sesso_x']!= merged.iloc[i]["sesso_y"]:
		#print(merged.iloc[i])
		modifiedLines=modifiedLines+1
	if merged.iloc[i]['anno_nascita_x'] and pd.isnull(merged.iloc[i]["anno_nascita_y"]) or merged.iloc[i]['comune_residenza_x'] and pd.isnull(merged.iloc[i]["comune_residenza_y"]) or merged.iloc[i]['sesso_x'] and pd.isnull(merged.iloc[i]["sesso_y"]):
		deletedLines=deletedLines+1
	

	i=i+1
		
		

results_directory = os.path.dirname(file_path)
RESULT_PATH = "resultsQuality/"+results_directory

if not os.path.exists(RESULT_PATH):
	os.mkdir(RESULT_PATH)

RESULT_PATH += "/"+file_name+"_qualityresults.txt"
output_file = open(RESULT_PATH, "w")
output_file.write("Deleted Lines = " + str(deletedLines) + "\n modified Lines = "+ str(modifiedLines) )
output_file.close()

	
print("Deleted Lines = " , deletedLines, "\n modified Lines = ", modifiedLines)
   




	