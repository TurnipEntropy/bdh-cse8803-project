# -*- coding: utf-8 -*-
"""
Created on Sun Apr 22 19:07:57 2018

@author: Entropic Turnips
"""

from DataLoader import DataLoader
from sklearn.metrics import roc_curve, auc
import numpy as np
import matplotlib.pyplot as plt

path = "I:\\EnTur\\Documents\\Big_Data_Health\\project\\predictions\\"
labels = []
preds = []
for i in range(1,5):
    data_loader = DataLoader(path + str(i) + "\\")
    labels.extend(data_loader.read_DataFrame_labels())
    preds.extend(data_loader.read_DataFrame_pos_probabilities())
    
print(len(labels))
print(len(preds))
fpr, tpr, thresholds = roc_curve(np.array(labels).astype(np.int), preds)
plt.plot([0, 1], [0, 1], linestyle ='--', lw = 2, color='r')
plt.plot(fpr, tpr, lw=2, alpha=1.0)
plt.show()
print(auc(fpr, tpr))