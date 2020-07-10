import pandas as pd
import sys
import os
from abc import abstractmethod

"""
It abstracts the behavior of a script to check the quality. It should be extended by each quality checker.
"""

class AbstractMatching():


	@abstractmethod
	def matching(original_dataset_path, anonymized_dataset_path):
		pass