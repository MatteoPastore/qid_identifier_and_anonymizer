import pandas as pd
import random
import numpy as np
import os
import subprocess
import sys
import csv

class BestFinder():

	def finder(folder1_path, folder2_path):
		Results = open("Ranking.txt", "w")
		Lines=[]
		Values=[]
		Results.write("AnonymizationTechnique,Deleted Rows,Modified Rows,Singletons"+"\n")
		for filename in os.listdir(folder1_path):
	   		with open(os.path.join(folder1_path, filename), 'r') as f:
	   			for line in f.readlines():
	   				words=line.split()
	   				Lines.append(words[3])
	   			Results.write( f.name[+65:-19]+","+Lines[0]+","+ Lines[1])
	   			Lines.clear()
	   		with open(os.path.join(folder2_path+"\\"+filename[:-19]+"NoNewId_results.txt"), 'r') as g:
	   			for line in g.readlines():
	   				valueswords=line.split()
	   				if(len(valueswords)>3):
	   					Values.append(valueswords[3])
	   			Results.write(","+Values[1]+"\n")
	   			Values.clear()
		Results.close()
		'''txt_file = r"Ranking.txt"
		csv_file = r"RankingCsv.csv"
		in_txt = csv.reader(open(txt_file, "rt"), delimiter = ',')
		out_csv = csv.writer(open(csv_file, 'wt'))
		out_csv.writerows(in_txt)'''
		dftxt = pd.read_csv('Ranking.txt',sep=",")
		dftxt.columns = ["AnonymizationTechnique", "Deleted Rows", "Modified Rows", "Singletons"]
		dftxt.to_csv("RankingCsv.csv", index=False)
		df = pd.read_csv('RankingCsv.csv')
		df = df.sort_values("Singletons")
		df.to_csv('RankingSingletons.csv', index=False)
		df = df.sort_values("Modified Rows")
		df.to_csv("RankingModifiedRows.csv", index=False)


