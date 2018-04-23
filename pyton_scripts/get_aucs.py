# -*- coding: utf-8 -*-
"""
Created on Sun Apr 22 19:07:57 2018

@author: Entropic Turnips
"""

from DataLoader import DataLoader
from sklearn.metrics import roc_curve, auc
import numpy as np
import matplotlib.pyplot as plt

path = "I:\\EnTur\\Documents\\Big_Data_Health\\project\\predictions_round1_0713945\\"
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
ppv = tpr / (fpr + tpr)

def get_ppv(label, pred_prob, threshold):
    tp = 0
    fp = 0
    tn = 0
    fn = 0
    
    for i in range(len(pred_prob)):
        pred = 0 if pred_prob[i] < threshold else 1
        if label[i] == pred == 1:
            tp += 1
        elif label[i] == pred == 0 :
            tn += 1
        elif label[i] == 0:
            fp += 1
        else:
            fn += 1
    return (tp / float(tp + fp), tp + fp)


max_ppv = 0
max_total_positive = 0
max_thresh = 0
np_labels = np.array(labels).astype(np.int)
for i in range(775, 785, 1):
    ppv, total_pos = get_ppv(labels, preds, thresholds[i])
    print(total_pos)
    if ppv > max_ppv:
        max_ppv = ppv
        max_thresh = thresholds[i]
        max_total_positive = total_pos
    