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

from apscheduler.scheduler import Scheduler


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
		MatchingCsv.matching("dataset_newIndex.csv", path[:-4]+"Range.csv")
		MatchingCsv.matching("dataset_newIndex.csv", path[:-4]+"Centroid.csv")	
		DropNewId.dropNewId(path[:-4]+"Range.csv")
		PrivacyChecker.privacychecker(path[:-4]+"RangeNoNewId.csv")
		DropNewId.dropNewId(path[:-4]+"Centroid.csv")
		PrivacyChecker.privacychecker(path[:-4]+"CentroidNoNewId.csv")
	if type=="Gender":
		MatchingCsv.matching("dataset_newIndex.csv", path[:-4]+"GenderAll.csv")
		MatchingCsv.matching("dataset_newIndex.csv", path[:-4]+"GenderSingleton.csv")	
		DropNewId.dropNewId(path[:-4]+"GenderAll.csv")
		PrivacyChecker.privacychecker(path[:-4]+"GenderAllNoNewId.csv")
		DropNewId.dropNewId(path[:-4]+"GenderSingleton.csv")
		PrivacyChecker.privacychecker(path[:-4]+"GenderSingletonNoNewId.csv")
	if type=="Municipality":
		MatchingCsv.matching("dataset_newIndex.csv", path[:-4]+"ProvinceAll.csv")
		MatchingCsv.matching("dataset_newIndex.csv", path[:-4]+"ProvinceSingleton.csv")	
		DropNewId.dropNewId(path[:-4]+"ProvinceAll.csv")
		PrivacyChecker.privacychecker(path[:-4]+"ProvinceAllNoNewId.csv")
		DropNewId.dropNewId(path[:-4]+"ProvinceSingleton.csv")
		PrivacyChecker.privacychecker(path[:-4]+"ProvinceSingletonNoNewId.csv")





os.system("python Singleton.py dataset.csv")
if os.path.isfile("anno_nascita,comune_residenza,sesso.csv"):
	os.rename(r'anno_nascita,comune_residenza,sesso.csv',r'sesso,anno_nascita,comune_residenza.csv')
if os.path.isfile("comune_residenza,anno_nascita,sesso.csv"):
	os.rename(r'comune_residenza,anno_nascita,sesso.csv',r'sesso,anno_nascita,comune_residenza.csv')
if os.path.isfile("anno_nascita,sesso,comune_residenza.csv"):
	os.rename(r'anno_nascita,sesso,comune_residenza.csv',r'sesso,anno_nascita,comune_residenza.csv')
if os.path.isfile("comune_residenza,sesso,anno_nascita.csv"):
	os.rename(r'comune_residenza,sesso,anno_nascita.csv',r'sesso,anno_nascita,comune_residenza.csv')
if os.path.isfile("sesso,comune_residenza,anno_nascita.csv"):
	os.rename(r'sesso,comune_residenza,anno_nascita.csv',r'sesso,anno_nascita,comune_residenza.csv')								#GenereSingleton


sched = Scheduler()
sched.start()

def timer():
	print ("Timer")

sched.add_interval_job(timer, seconds = 300)


#SetIndex e PrivacyChecker
print("Running SetIndex and PrivacyChecker")
SetIndex.setIndex("dataset.csv")	
PrivacyChecker.privacychecker("dataset.csv")

#YearRange + YearCentroid
thread1=AnonymizerThread("YearRange + YearCentroid","dataset_newIndex.csv", "anno_nascita", "Year")
thread1.start()


#GenderAll and GenderSingleton

thread2=AnonymizerThread("Gender All + Gender Singleton","dataset_newIndex.csv", "sesso", "Gender")
thread2.start()


#ProvinceAll and ProvinceSingleton	

thread3=AnonymizerThread("Province All + Province Singleton","dataset_newIndex.csv", "comune_residenza", "Municipality")
thread3.start()



thread1.join()
thread2.join()
thread3.join()
sched.shutdown()


