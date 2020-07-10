import pandas as pd
import csv as csv
import numpy as np
import sys
import os
from abc import abstractmethod

"""
It abstracts the behavior of a PrivacyChecker. It should be extended by each privacy checker technique.
"""

class AbstractPrivacyChecker():


	@abstractmethod
	def privacychecker(dataset_path):
		pass
