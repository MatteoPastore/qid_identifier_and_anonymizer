import pandas as pd
import random
import numpy as np
import os
import subprocess
import sys
from abstract_Anonymizer import AbstractAnonymizer
from GenderAll import GenderAllClass as GenderAll
from GenderSingleton import GenderSingletonClass as GenderSingleton
from YearRange import YearRangeClass as YearRange
from YearCentroid import YearCentroidClass as YearCentroid
from ProvinceAll import ProvinceAllClass as ProvinceAll
from ProvinceSingleton import ProvinceSingletonClass as ProvinceSingleton

class AnonymizerClass(AbstractAnonymizer):
	def anonymize(file_path, field, type):
		if type=="Gender":
			GenderAll.anonymizeGenderAll(file_path, field)
			GenderSingleton.anonymizeGenderSingleton(file_path, field)

		if type=="Year":
			YearRange.anonymizeYearRange(file_path, field)
			YearCentroid.anonymizeYearCentroid(file_path, field)

		if type=="Municipality":
			ProvinceAll.anonymizeProvinceAll(file_path, field)
			ProvinceSingleton.anonymizeProvinceSingleton(file_path, field)

		if type != "Gender" and type != "Year" and type != "Municipality" :
			print("Type not supported")



