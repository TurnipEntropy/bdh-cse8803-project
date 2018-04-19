# -*- coding: utf-8 -*-
"""
Created on Wed Apr 18 20:52:47 2018

@author: Entropic Turnips
"""

from sklearn.linear_model import SGDClassifier
from DataLoader import DataLoader

path = "I:\\EnTur\\Documents\\Big_Data_Health\\project\\newly_labeled_dataset\\"
data_loader = DataLoader(path)
data = data_loader.csv_to_2d_ndarray()
labels = data_loader.read_labels()

model = SGDClassifier(loss="log", penalty="elasticnet", max_iter=20)
model.fit(data, labels)
preds = model.predict_proba(data)