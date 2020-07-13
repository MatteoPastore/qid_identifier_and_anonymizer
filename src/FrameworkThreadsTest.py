import pandas as pd
import random
import numpy as np
import os
import subprocess
import sys
from Anonymizer import AnonymizerClass as Anonymizer
from MatchingCsv import MatchingClass as MatchingCsv
from privacy_checker import PrivacyChecker as PrivacyChecker
from SetIndex import SetIndexClass as SetIndex
from DropNewId import DropNewIdClass as DropNewId
from BestAnonymization import BestFinder as BestFinder
import threading
import time



class AnonymizerThread (threading.Thread):
   def __init__(self, name, path, field, type):
      threading.Thread.__init__(self)
      self.name= name
      self.path = path
      self.field = field
      self.type = type
   def run(self):
      print ("Starting " + self.name)
      anonymizeThread(self.path, self.field, self.type)
      print ("Exiting " + self.name)

def anonymizeThread(path, field, type):
	Anonymizer.anonymize(path, field, type)	
	if type=="Year":
		MatchingCsv.matching("src\\dataset_newIndex.csv", path[:-4]+"Range.csv")
		MatchingCsv.matching("src\\dataset_newIndex.csv", path[:-4]+"Centroid.csv")	
		DropNewId.dropNewId(path[:-4]+"Range.csv")
		PrivacyChecker.privacychecker(path[:-4]+"RangeNoNewId.csv")
		DropNewId.dropNewId(path[:-4]+"Centroid.csv")
		PrivacyChecker.privacychecker(path[:-4]+"CentroidNoNewId.csv")
	if type=="Gender":
		MatchingCsv.matching("src\\dataset_newIndex.csv", path[:-4]+"GenderAll.csv")
		MatchingCsv.matching("src\\dataset_newIndex.csv", path[:-4]+"GenderSingleton.csv")	
		DropNewId.dropNewId(path[:-4]+"GenderAll.csv")
		PrivacyChecker.privacychecker(path[:-4]+"GenderAllNoNewId.csv")
		DropNewId.dropNewId(path[:-4]+"GenderSingleton.csv")
		PrivacyChecker.privacychecker(path[:-4]+"GenderSingletonNoNewId.csv")
	if type=="Municipality":
		MatchingCsv.matching("src\\dataset_newIndex.csv", path[:-4]+"ProvinceAll.csv")
		MatchingCsv.matching("src\\dataset_newIndex.csv", path[:-4]+"ProvinceSingleton.csv")	
		DropNewId.dropNewId(path[:-4]+"ProvinceAll.csv")
		PrivacyChecker.privacychecker(path[:-4]+"ProvinceAllNoNewId.csv")
		DropNewId.dropNewId(path[:-4]+"ProvinceSingleton.csv")
		PrivacyChecker.privacychecker(path[:-4]+"ProvinceSingletonNoNewId.csv")






os.system("src\\python Singleton.py dataset.csv")
if os.path.isfile("src\\anno_nascita,comune_residenza,sesso.csv"):
	os.rename(r'src\\anno_nascita,comune_residenza,sesso.csv',r'src\\sesso,anno_nascita,comune_residenza.csv')
if os.path.isfile("src\\comune_residenza,anno_nascita,sesso.csv"):
	os.rename(r'src\\comune_residenza,anno_nascita,sesso.csv',r'src\\sesso,anno_nascita,comune_residenza.csv')
if os.path.isfile("src\\anno_nascita,sesso,comune_residenza.csv"):
	os.rename(r'src\\anno_nascita,sesso,comune_residenza.csv',r'src\\sesso,anno_nascita,comune_residenza.csv')
if os.path.isfile("src\\comune_residenza,sesso,anno_nascita.csv"):
	os.rename(r'src\\comune_residenza,sesso,anno_nascita.csv',r'src\\sesso,anno_nascita,comune_residenza.csv')
if os.path.isfile("src\\sesso,comune_residenza,anno_nascita.csv"):
	os.rename(r'src\\sesso,comune_residenza,anno_nascita.csv',r'src\\sesso,anno_nascita,comune_residenza.csv')								#GenereSingleton


#SetIndex e PrivacyChecker
print("Running SetIndex and PrivacyChecker")
SetIndex.setIndex("src\\dataset.csv")	
PrivacyChecker.privacychecker("src\\dataset.csv")

#YearRange + YearCentroid
thread1=AnonymizerThread("YearRange + YearCentroid","src\\dataset_newIndex.csv", "anno_nascita", "Year")
thread1.start()


#GenderAll and GenderSingleton

thread2=AnonymizerThread("Gender All + Gender Singleton","src\\dataset_newIndex.csv", "sesso", "Gender")
thread2.start()


#ProvinceAll and ProvinceSingleton	

thread3=AnonymizerThread("Province All + Province Singleton","src\\dataset_newIndex.csv", "comune_residenza", "Municipality")
thread3.start()


thread1.join()
thread2.join()
thread3.join()
#YearRange + Province

thread4=AnonymizerThread("Year Range + Province","src\\dataset_newIndexRange.csv", "comune_residenza", "Municipality")
thread4.start()


#YearCentroid + Province

thread5=AnonymizerThread("Year Centroid + Province","src\\dataset_newIndexCentroid.csv", "comune_residenza", "Municipality")
thread5.start()

#GenderAll + Province

threadGenderAllProvince=AnonymizerThread("Gender All + Province","src\\dataset_newIndexGenderAll.csv", "comune_residenza", "Municipality")
threadGenderAllProvince.start()


#GenderAll + Year
thread7=AnonymizerThread("Gender All + Year","src\\dataset_newIndexGenderAll.csv", "anno_nascita", "Year")
thread7.start()


threadGenderAllProvince.join()
#GenderAll + Province + Year

thread8=AnonymizerThread("Gender All + Province + Year ","src\\dataset_newIndexGenderAllProvinceAll.csv", "anno_nascita", "Year")
thread8.start()

#Ranking
BestFinder.finder("src\\resultsQuality", "src\\results")
