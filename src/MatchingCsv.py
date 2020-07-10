import pandas as pd
import sys
import os
from abstract_MatchingCsv import AbstractMatching

class MatchingClass(AbstractMatching):
	def matching(file1_path, file2_path):
		file_name = os.path.basename(file2_path).split(".")[0]
		a = pd.read_csv(file1_path)
		b = pd.read_csv(file2_path)
		merged = a.merge(b, on='newId')
		merged.to_csv("merged.csv", index=False)

		deletedLines=0
		modifiedLines=0

		i=0
		while i<len(merged):
			
			if merged.iloc[i]['anno_nascita_x']!= merged.iloc[i]["anno_nascita_y"] or merged.iloc[i]['comune_residenza_x']!= merged.iloc[i]["comune_residenza_y"]  or  merged.iloc[i]['sesso_x']!= merged.iloc[i]["sesso_y"]:
				modifiedLines=modifiedLines+1
			if merged.iloc[i]['anno_nascita_x'] and pd.isnull(merged.iloc[i]["anno_nascita_y"]) or merged.iloc[i]['comune_residenza_x'] and pd.isnull(merged.iloc[i]["comune_residenza_y"]) or merged.iloc[i]['sesso_x'] and pd.isnull(merged.iloc[i]["sesso_y"]):
				deletedLines=deletedLines+1
			

			i=i+1
				
				

		results_directory = os.path.dirname(file_name)
		RESULT_PATH = "resultsQuality/"+results_directory

		if not os.path.exists(RESULT_PATH):
			os.mkdir(RESULT_PATH)

		RESULT_PATH += "/"+file_name+"_qualityresults.txt"
		output_file = open(RESULT_PATH, "w")
		output_file.write("Deleted Lines = " + str(deletedLines) + "\nModified Lines = "+ str(modifiedLines) )
		output_file.close()

			

		   




			