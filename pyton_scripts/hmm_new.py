# -*- coding: utf-8 -*-
"""
Created on Fri Apr 13 23:08:28 2018

@author: Entropic Turnips
"""

import numpy as np
from matplotlib import cm, pyplot as plt
from matplotlib.dates import YearLocator, MonthLocator
from hmmlearn import hmm
import matplotlib.pyplot as plt
from sklearn.metrics import roc_curve, auc
from scipy import interp
import random
import csv
import warnings
from DataLoader import DataLoader

warnings.filterwarnings('ignore')

path = "newly_labeled_dataset/"
prefix = "part-000"
counts_0 = 0
counts_1 = 0
data_loader = DataLoader('I:\\EnTur\\Documents\\Big_Data_Health\\project\\newly_labeled_dataset\\')
labels = data_loader.read_labels()
features = data_loader.read_features()
    
#for i in range(8):
#    print(i)
#    with open(path + prefix + str(i).zfill(2)) as data:
#        content = data.readlines()
#    
#        for i in range(len(content)):
#            seq = content[i].split(",")
#            #print(seq)
#            icu_id = seq[5]
#            data_str = seq[7:13]
#            data_str.append(seq[14].replace(")", ""))
#            data_arr = np.array(list(float(x.strip()) for x in data_str))
#            if (icu_id in hash_table):
#                hash_table[icu_id].append(data_arr)
#            else:
#                hash_table[icu_id] = []
#                hash_table[icu_id].append(data_arr)
#    
#        for k, v in hash_table.items():
#            arrs = np.array(v)
#            Xs.append(arrs)
#            #print(arrs.shape)
#            sizes.append((arrs.shape)[0])
    


X = np.concatenate(features[0])
sizes = features[1]
model = hmm.GaussianHMM(n_components = 5, n_iter = 80).fit(X, sizes)
pred = model.predict_proba(X, sizes)
group0 = pred[:, 0]
group1 =np.sum(pred[:, 1:5], axis=1)
preds = np.concatenate([[group0], [group1]], axis=0).transpose()
fpr, tpr, thresholds = roc_curve(np.array(labels).astype(np.int), preds[:,1])
plt.plot(fpr, tpr, lw=1, alpha=0.3)
plt.show()