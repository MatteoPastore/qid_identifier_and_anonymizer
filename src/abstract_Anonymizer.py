import pandas as pd
import csv as csv
import numpy as np
import sys
import os
from abc import abstractmethod

"""
It abstracts the behavior of an Anonymizer. It should be extended by each anonymizer.
"""

class AbstractAnonymizer():


	@abstractmethod
	def anonymizer(dataset_path, field, type):
		pass
