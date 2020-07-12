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
''' 
The framework deletes the files related to previous executions, to avoid that the analyzes are wrong

'''
if os.path.isfile("sesso,anno_nascita,comune_residenza.csv"):
	os.remove("sesso,anno_nascita,comune_residenza.csv")
if os.path.isfile("sesso,anno_nascita.csv"):
	os.remove("sesso,anno_nascita.csv")
if os.path.isfile("sesso,comune_residenza.csv"):
	os.remove("sesso,comune_residenza.csv")
if os.path.isfile("sesso.csv"):
	os.remove("sesso.csv")
if os.path.isfile("anno_nascita,comune_residenza.csv"):
	os.remove("anno_nascita,comune_residenza.csv")
if os.path.isfile("anno_nascita.csv"):
	os.remove("anno_nascita.csv")
if os.path.isfile("anno_nascita,sesso.csv"):
	os.remove("anno_nascita,sesso.csv")
if os.path.isfile("comune_residenza.csv"):
	os.remove("comune_residenza.csv")
if os.path.isfile("comune_residenza,sesso.csv"):
	os.remove("comune_residenza,sesso.csv")
if os.path.isfile("comune_residenza,anno_nascita.csv"):
	os.remove("comune_residenza,anno_nascita.csv")

#SetIndex e PrivacyChecker

SetIndex.setIndex("C:\\Users\\matte\\Desktop\\src\\dataset.csv")	
PrivacyChecker.privacychecker("C:\\Users\\matte\\Desktop\\src\\dataset.csv")

#YearRange + YearCentroid

Anonymizer.anonymize("C:\\Users\\matte\\Desktop\\src\\dataset_newIndex.csv", "anno_nascita", "Year")		
MatchingCsv.matching("C:\\Users\\matte\\Desktop\\src\\dataset_newIndex.csv", "C:\\Users\\matte\\Desktop\\src\\dataset_newIndexRange.csv")	
MatchingCsv.matching("C:\\Users\\matte\\Desktop\\src\\dataset_newIndex.csv", "C:\\Users\\matte\\Desktop\\src\\dataset_newIndexCentroid.csv")				#YearRange and YearCentroid#YearRange and YearCentroid
DropNewId.dropNewId("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexRange.csv")
PrivacyChecker.privacychecker("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexRangeNoNewId.csv")
DropNewId.dropNewId("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexCentroid.csv")
PrivacyChecker.privacychecker("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexCentroidNoNewId.csv")

#Find Singletons

os.system("python Singleton.py dataset.csv")
if os.path.isfile("anno_nascita,comune_residenza,sesso.csv"):
	os.rename(r'C:\\Users\\matte\\Desktop\\src\\anno_nascita,comune_residenza,sesso.csv',r'C:\\Users\\matte\\Desktop\\src\\sesso,anno_nascita,comune_residenza.csv')
if os.path.isfile("comune_residenza,anno_nascita,sesso.csv"):
	os.rename(r'C:\\Users\\matte\\Desktop\\src\\comune_residenza,anno_nascita,sesso.csv',r'C:\\Users\\matte\\Desktop\\src\\sesso,anno_nascita,comune_residenza.csv')
if os.path.isfile("anno_nascita,sesso,comune_residenza.csv"):
	os.rename(r'C:\\Users\\matte\\Desktop\\src\\anno_nascita,sesso,comune_residenza.csv',r'C:\\Users\\matte\\Desktop\\src\\sesso,anno_nascita,comune_residenza.csv')
if os.path.isfile("comune_residenza,sesso,anno_nascita.csv"):
	os.rename(r'C:\\Users\\matte\\Desktop\\src\\comune_residenza,sesso,anno_nascita.csv',r'C:\\Users\\matte\\Desktop\\src\\sesso,anno_nascita,comune_residenza.csv')
if os.path.isfile("sesso,comune_residenza,anno_nascita.csv"):
	os.rename(r'C:\\Users\\matte\\Desktop\\src\\sesso,comune_residenza,anno_nascita.csv',r'C:\\Users\\matte\\Desktop\\src\\sesso,anno_nascita,comune_residenza.csv')								#GenereSingleton

#GenderAll and GenderSingleton

Anonymizer.anonymize("C:\\Users\\matte\\Desktop\\src\\dataset_newIndex.csv", "sesso", "Gender")						
MatchingCsv.matching("C:\\Users\\matte\\Desktop\\src\\dataset_newIndex.csv", "C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAll.csv")	
MatchingCsv.matching("C:\\Users\\matte\\Desktop\\src\\dataset_newIndex.csv", "C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderSingleton.csv")
DropNewId.dropNewId("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAll.csv")
PrivacyChecker.privacychecker("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAllNoNewId.csv")		
DropNewId.dropNewId("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderSingleton.csv")
PrivacyChecker.privacychecker("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderSingletonNoNewId.csv")

#ProvinceAll and ProvinceSingleton	

Anonymizer.anonymize("C:\\Users\\matte\\Desktop\\src\\dataset_newIndex.csv", "comune_residenza", "Municipality")														#ProvinceAll and ProvinceSingleton
MatchingCsv.matching("C:\\Users\\matte\\Desktop\\src\\dataset_newIndex.csv", "C:\\Users\\matte\\Desktop\\src\\dataset_newIndexProvinceAll.csv")	
MatchingCsv.matching("C:\\Users\\matte\\Desktop\\src\\dataset_newIndex.csv", "C:\\Users\\matte\\Desktop\\src\\dataset_newIndexProvinceSingleton.csv")
DropNewId.dropNewId("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexProvinceAll.csv")
PrivacyChecker.privacychecker("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexProvinceAllNoNewId.csv")
DropNewId.dropNewId("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexProvinceSingleton.csv")
PrivacyChecker.privacychecker("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexProvinceSingletonNoNewId.csv")

#YearRange + Province

Anonymizer.anonymize("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexRange.csv", "comune_residenza", "Municipality")		
MatchingCsv.matching("C:\\Users\\matte\\Desktop\\src\\dataset_newIndex.csv", "C:\\Users\\matte\\Desktop\\src\\dataset_newIndexRangeProvinceAll.csv")	
MatchingCsv.matching("C:\\Users\\matte\\Desktop\\src\\dataset_newIndex.csv", "C:\\Users\\matte\\Desktop\\src\\dataset_newIndexRangeProvinceSingleton.csv")				#YearRange and YearCentroid#YearRange and YearCentroid
DropNewId.dropNewId("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexRangeProvinceAll.csv")
PrivacyChecker.privacychecker("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexRangeProvinceAllNoNewId.csv")
DropNewId.dropNewId("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexRangeProvinceSingleton.csv")
PrivacyChecker.privacychecker("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexRangeProvinceSingletonNoNewId.csv")

#YearCentroid + Province

Anonymizer.anonymize("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexCentroid.csv", "comune_residenza", "Municipality")		
MatchingCsv.matching("C:\\Users\\matte\\Desktop\\src\\dataset_newIndex.csv", "C:\\Users\\matte\\Desktop\\src\\dataset_newIndexCentroidProvinceAll.csv")	
MatchingCsv.matching("C:\\Users\\matte\\Desktop\\src\\dataset_newIndex.csv", "C:\\Users\\matte\\Desktop\\src\\dataset_newIndexCentroidProvinceSingleton.csv")				#YearRange and YearCentroid#YearRange and YearCentroid
DropNewId.dropNewId("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexCentroidProvinceAll.csv")
PrivacyChecker.privacychecker("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexCentroidProvinceAllNoNewId.csv")
DropNewId.dropNewId("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexCentroidProvinceSingleton.csv")
PrivacyChecker.privacychecker("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexCentroidProvinceSingletonNoNewId.csv")

#GenderAll + Province

Anonymizer.anonymize("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAll.csv", "comune_residenza", "Municipality")		
MatchingCsv.matching("C:\\Users\\matte\\Desktop\\src\\dataset_newIndex.csv", "C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAllProvinceAll.csv")	
MatchingCsv.matching("C:\\Users\\matte\\Desktop\\src\\dataset_newIndex.csv", "C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAllProvinceSingleton.csv")				#YearRange and YearCentroid#YearRange and YearCentroid
DropNewId.dropNewId("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAllProvinceAll.csv")
PrivacyChecker.privacychecker("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAllProvinceAllNoNewId.csv")
DropNewId.dropNewId("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAllProvinceSingleton.csv")
PrivacyChecker.privacychecker("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAllProvinceSingletonNoNewId.csv")

#GenderAll + Year

Anonymizer.anonymize("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAll.csv", "anno_nascita", "Year")		
MatchingCsv.matching("C:\\Users\\matte\\Desktop\\src\\dataset_newIndex.csv", "C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAllRange.csv")	
MatchingCsv.matching("C:\\Users\\matte\\Desktop\\src\\dataset_newIndex.csv", "C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAllCentroid.csv")				#YearRange and YearCentroid#YearRange and YearCentroid
DropNewId.dropNewId("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAllRange.csv")
PrivacyChecker.privacychecker("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAllRangeNoNewId.csv")
DropNewId.dropNewId("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAllCentroid.csv")
PrivacyChecker.privacychecker("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAllCentroidNoNewId.csv")

#GenderAll + Province + Year

Anonymizer.anonymize("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAllProvinceAll.csv", "anno_nascita", "Year")		
MatchingCsv.matching("C:\\Users\\matte\\Desktop\\src\\dataset_newIndex.csv", "C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAllProvinceAllRange.csv")	
MatchingCsv.matching("C:\\Users\\matte\\Desktop\\src\\dataset_newIndex.csv", "C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAllProvinceAllCentroid.csv")				#YearRange and YearCentroid#YearRange and YearCentroid
DropNewId.dropNewId("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAllProvinceAllRange.csv")
PrivacyChecker.privacychecker("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAllProvinceAllRangeNoNewId.csv")
DropNewId.dropNewId("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAllProvinceAllCentroid.csv")
PrivacyChecker.privacychecker("C:\\Users\\matte\\Desktop\\src\\dataset_newIndexGenderAllProvinceAllCentroidNoNewId.csv")

BestFinder.finder("C:\\Users\\matte\\Desktop\\src\\resultsQuality", "C:\\Users\\matte\\Desktop\\src\\results")
