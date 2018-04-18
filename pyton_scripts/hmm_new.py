# -*- coding: utf-8 -*-
"""
Created on Fri Apr 13 23:08:28 2018

@author: Entropic Turnips
"""

import numpy as np
from matplotlib import cm, pyplot as plt
from matplotlib.dates import YearLocator, MonthLocator
from hmmlearn import hmm
import random
import csv
import warnings

warnings.filterwarnings('ignore')

path = "newly_labeled_dataset/"
prefix = "part-000"
Xs = []
sizes = []
hash_table = {}
labels = {}
counts_0 = 0
counts_1 = 0
for i in range(4):
    with open(path + prefix + str(i).zfill(2)) as data:
        content = data.readlines()
        for i in range(len(content)):
            seq = content[i].split(",")
            icu_id = seq[5]
            if (icu_id in labels):
                if (labels[icu_id] == 1):
                    if (seq[3] == '0'):
                        counts_0 += 1
                else:
                    counts_1 += 1
                continue
            
            labels[icu_id] = seq[3]
            if (seq[3] == '0'):
                counts_0 += 1
            else:
                counts_1 += 1



    
for i in range(4):
    print(i)
    with open(path + prefix + str(i).zfill(2)) as data:
        content = data.readlines()
    
        for i in range(len(content)):
            seq = content[i].split(",")
            #print(seq)
            icu_id = seq[5]
            data_str = seq[7:13]
            data_str.append(seq[14].replace(")", ""))
            data_arr = np.array(list(float(x.strip()) for x in data_str))
            if (icu_id in hash_table):
                hash_table[icu_id].append(data_arr)
            else:
                hash_table[icu_id] = []
                hash_table[icu_id].append(data_arr)
    
        for k, v in hash_table.items():
            arrs = np.array(v)
            Xs.append(arrs)
            #print(arrs.shape)
            sizes.append((arrs.shape)[0])
    


X = np.concatenate(Xs)
model = hmm.GaussianHMM(n_components = 2, n_iter = 50).fit(X, sizes)
pred = model.predict_proba(X, sizes)
group0 = (sum(pred[:, 0]))
group1 = (sum(pred[:, 1]))
    