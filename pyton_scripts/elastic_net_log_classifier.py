# -*- coding: utf-8 -*-
"""
Created on Wed Apr 18 20:52:47 2018

@author: Entropic Turnips
"""

from sklearn.linear_model import SGDClassifier
from DataLoader import DataLoader
import numpy as np

path = "I:\\EnTur\\Documents\\Big_Data_Health\\project\\newly_labeled_dataset\\"
data_loader = DataLoader(path)
data = data_loader.csv_to_2d_ndarray(start=7).astype(np.float)
labels = data_loader.csv_to_2d_ndarray(start=5, end=6).astype(np.int)
print('creating model')
model = SGDClassifier(loss="log", penalty="elasticnet", max_iter=2)
print('training model')
model.fit(data, np.ravel(labels))
print('predicting values')
preds = model.predict_proba(data)