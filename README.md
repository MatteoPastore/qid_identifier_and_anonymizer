
[![Build Status](https://travis-ci.com/MatteoPastore/qid_identifier_and_anonymizer.svg?branch=master)](https://travis-ci.com/MatteoPastore/qid_identifier_and_anonymizer)
# qid_identifier_and_anonymizer
This repository contains a (software) Anonymization Framework to perform singletons analysis and anonymization of OpenData’s datasets. OpenData must guarantee consistent information but at the same time must not harm the privacy of individuals. Analyzing open datasets containing information relating to licenses and published by the Italian government, the well-known quasi-identifier (date of birth, sex, municipality of residence) exhibits up to 2% of singleton, i.e. uniquely determined tuples, in already anonymized datasets. Our goal is to find the best tradeoff between consistent information (Fewer modified or deleted tuples) and maintaining privacy (Fewer number of singletons). The Framework select the best QID by the number of disclosed singletons and it provides an anonymization mechanism (based on generalization and suppression) to anonymize the well-known QID (year_of_birth, sex, sex) by minimizing the number of singletons while minimizing the number of affected rows. 
# Framework structure
It is a diagrammatic representation of the involved actors in the framework and their interactions. The blue boxes represent abstract classes, while the white boxes represent concrete classes.



The starting point of the anonymization is the Anonymizer which is the orchestrator of the whole anonymization and it is in charge of:
  1. verifying the correctness of the parameters set by the user,
  2. veryfing the type of the attribute to start the process,
  3. call the right script to anonymize according to the type of the attribute provided by the user,
  4. forward the right field to anonymize
  
Depending on the type of the attribute, the corresponding script decides how to anonymize the field. Three are the field-type supported by the framework.
  1. Year, that can be anonymized by Range ( YearRange ) or by Centroid ( YearCentroid ),
  2. Gender, that can be anonymized by All or Singletons,
  3. Municipality, that can be anonymized by All or Singletons.

Once anonymized, two are the following steps.
  1. MatchingCsv provide to match the modified lines and the deleted lines between the original dataset and the new one.
  2. PrivacyChecker provide to analyze the new dataset and discover how many singletons ( and wich percentage of them ) it has.
  
The framework can run in debug or no-debug mode, further more it can run in parallel mode.
In parallel mode it creates a thread for each anonymization technique.
