import pandas as pd
import random
import numpy as np
import os
import subprocess
import sys

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

a = pd.read_csv("C:\\Users\\matte\\Desktop\\Framework\\dataset.csv" , delimiter=",", dtype={"anno_nascita":float,"comune_residenza": object, "sesso":object})
g=input("Do you want run only privacy_checker? : ")
if g=="yes":
	print("Running privacy_checker")
	os.system('python privacy_checker.py dataset.csv')
	print("Completed privacy_checker")
	sys.exit()


print("Running SetIndex")
os.system("python SetIndex.py dataset.csv")
print("Completed SetIndex")																#SetIndex e PrivacyChecker
print("Running privacy_checker")
os.system('python privacy_checker.py dataset.csv')
print("Completed privacy_checker")

print("Running Range")
os.system("python IntervalliGruppiDaK.py dataset_newIndex.csv")							#AnnoRange
print("Completed Range")
os.system("python MatchingCsv2.py dataset_newIndexRange.csv")
os.system('python DropNewId.py dataset_newIndexRange.csv')			
os.system('python privacy_checker.py dataset_newIndexRangeNoNewId.csv')

print("Running Centroide")
os.system("python GruppiDaK2.py dataset_newIndex.csv")									#AnnoCentroide
print("Completed Centroide")
os.system("python MatchingCsv2.py dataset_newIndexCentroide.csv")
os.system('python DropNewId.py dataset_newIndexCentroide.csv')			
os.system('python privacy_checker.py dataset_newIndexCentroideNoNewId.csv')

print("Running GenereAll")
os.system("python GenereAll.py dataset_newIndex.csv")									#GenereAll
print("Completed GenereAll")
os.system("python MatchingCsv2.py dataset_newIndexGenereAll.csv")
os.system('python DropNewId.py dataset_newIndexGenereAll.csv')			
os.system('python privacy_checker.py dataset_newIndexGenereAllNoNewId.csv')


print("Running GenereSingleton")
os.system("python Singleton.py dataset.csv")
if os.path.isfile("anno_nascita,comune_residenza,sesso.csv"):
	os.rename(r'C:\\Users\\matte\\Desktop\\Framework\\anno_nascita,comune_residenza,sesso.csv',r'C:\\Users\\matte\\Desktop\\Framework\\sesso,anno_nascita,comune_residenza.csv')
if os.path.isfile("comune_residenza,anno_nascita,sesso.csv"):
	os.rename(r'C:\\Users\\matte\\Desktop\\Framework\\comune_residenza,anno_nascita,sesso.csv',r'C:\\Users\\matte\\Desktop\\Framework\\sesso,anno_nascita,comune_residenza.csv')
if os.path.isfile("anno_nascita,sesso,comune_residenza.csv"):
	os.rename(r'C:\\Users\\matte\\Desktop\\Framework\\anno_nascita,sesso,comune_residenza.csv',r'C:\\Users\\matte\\Desktop\\Framework\\sesso,anno_nascita,comune_residenza.csv')
if os.path.isfile("comune_residenza,sesso,anno_nascita.csv"):
	os.rename(r'C:\\Users\\matte\\Desktop\\Framework\\comune_residenza,sesso,anno_nascita.csv',r'C:\\Users\\matte\\Desktop\\Framework\\sesso,anno_nascita,comune_residenza.csv')
if os.path.isfile("sesso,comune_residenza,anno_nascita.csv"):
	os.rename(r'C:\\Users\\matte\\Desktop\\Framework\\sesso,comune_residenza,anno_nascita.csv',r'C:\\Users\\matte\\Desktop\\Framework\\sesso,anno_nascita,comune_residenza.csv')								#GenereSingleton
os.system("python GenereSingletonOk.py dataset_newIndex.csv")
print("Completed GenereSingleton")
os.system("python MatchingCsv2.py dataset_newIndexGenereSingleton.csv")
os.system('python DropNewId.py dataset_newIndexGenereSingleton.csv')			
os.system('python privacy_checker.py dataset_newIndexGenereSingletonNoNewId.csv')

print("Running ProvinceAll")
os.system("python ProvinceAllOk.py dataset_newIndex.csv")									#ProvinceAll
print("Completed ProvinceAll")
os.system("python MatchingCsv2.py dataset_newIndexProvinceAll.csv")
os.system('python DropNewId.py dataset_newIndexProvinceAll.csv')			
os.system('python privacy_checker.py dataset_newIndexProvinceAllNoNewId.csv')

print("Running ProvinceSingleton")															#ProvinceSingleton
os.system("python ProvinceSingletonOk.py dataset_newIndex.csv")
print("Completed ProvinceSingleton")
os.system("python MatchingCsv2.py dataset_newIndexProvinceSingleton.csv")
os.system('python DropNewId.py dataset_newIndexProvinceSingleton.csv')			
os.system('python privacy_checker.py dataset_newIndexProvinceSingletonNoNewId.csv')

print("Running ProvinceAll on AnnoRange")
os.system("python ProvinceAllOk.py dataset_newIndexRange.csv")								#ProvinceAll on AnnoRange
print("Completed ProvinceAll on AnnoRange")
os.system("python MatchingCsv2.py dataset_newIndexRangeProvinceAll.csv")
os.system('python DropNewId.py dataset_newIndexRangeProvinceAll.csv')			
os.system('python privacy_checker.py dataset_newIndexRangeProvinceAllNoNewId.csv')

print("Running ProvinceAll on AnnoCentroide")
os.system("python ProvinceAllOk.py dataset_newIndexCentroide.csv")							#ProvinceAll on AnnoCentroide
print("Completed ProvinceAll on AnnoCentroide")
os.system("python MatchingCsv2.py dataset_newIndexCentroideProvinceAll.csv")
os.system('python DropNewId.py dataset_newIndexCentroideProvinceAll.csv')			
os.system('python privacy_checker.py dataset_newIndexCentroideProvinceAllNoNewId.csv')

print("Running ProvinceAll on GenereAll")
os.system("python ProvinceAllOk.py dataset_newIndexGenereAll.csv")							#ProvinceAll on GenereAll
print("Completed ProvinceAll on GenereAll")
os.system("python MatchingCsv2.py dataset_newIndexGenereAllProvinceAll.csv")
os.system('python DropNewId.py dataset_newIndexGenereAllProvinceAll.csv')			
os.system('python privacy_checker.py dataset_newIndexGenereAllProvinceAllNoNewId.csv')

'''
print("Running ProvinceSingleton on GenereSingleton")										#ProvinceSingleton on GenereSingleton
os.system("python ProvinceSingleton+GenereOk.py dataset_newIndex.csv")
print("Completed ProvinceSingleton on GenereSingleton")
#os.system("python MatchingCsv2.py dataset_newIndexGenereSingletonProvinceSingleton.csv")
os.system('python DropNewId.py dataset_newIndexProvinceSingletonGenereSingleton.csv')			
os.system('python privacy_checker.py dataset_newIndexProvinceSingletonGenereSingleton.csv')
'''

print("Running GenereAll on YearRange")
os.system("python GenereAllYearRange.py dataset_newIndexRange.csv")							#GenereAll on YearRange
print("Completed GenereAll on YearRange")
os.system("python MatchingCsv2.py dataset_newIndexRangeGenereAll.csv")
os.system('python DropNewId.py dataset_newIndexRangeGenereAll.csv')			
os.system('python privacy_checker.py dataset_newIndexRangeGenereAllNoNewId.csv')

print("Running GenereAll on YearCentroide")
os.system("python GenereAll.py dataset_newIndexCentroide.csv")								#GenereAll on YearCentroide
print("Completed GenereAll on YearCentroide")
os.system("python MatchingCsv2.py dataset_newIndexCentroideGenereAll.csv")
os.system('python DropNewId.py dataset_newIndexCentroideGenereAll.csv')			
os.system('python privacy_checker.py dataset_newIndexCentroideGenereAllNoNewId.csv')

print("Running Range on GenereAllProvinciaAll")
os.system("python IntervalliGruppiDaK.py dataset_newIndexGenereAllProvinceAll.csv")			#AnnoRange on GenereAll ProvinciaAll
print("Completed Range on GenereAllProvinciaAll")
os.system("python MatchingCsv2.py dataset_newIndexGenereAllProvinceAllRange.csv")
os.system('python DropNewId.py dataset_newIndexGenereAllProvinceAllRange.csv')			
os.system('python privacy_checker.py dataset_newIndexGenereAllProvinceAllRangeNoNewId.csv')

print("Running Centroide on GenereAllProvinciaAll")
os.system("python GruppiDaK2.py dataset_newIndexGenereAllProvinceAll.csv")					#AnnoCentroide on GenereAll ProvinciaAll
print("Completed Centroide on GenereAllProvinciaAll")
os.system("python MatchingCsv2.py dataset_newIndexGenereAllProvinceAllCentroide.csv")
os.system('python DropNewId.py dataset_newIndexGenereAllProvinceAllCentroide.csv')			
os.system('python privacy_checker.py dataset_newIndexGenereAllProvinceAllCentroideNoNewId.csv')





