# -*- coding: utf-8 -*-
"""
Created on Sun Apr  1 11:24:26 2018

@author: Entropic Turnips
"""
import numpy as np
from matplotlib import cm, pyplot as plt
from matplotlib.dates import YearLocator, MonthLocator
from hmmlearn import hmm
import random
try:
    from matplotlib.finance import quotes_historical_yahoo_ochl
except ImportError:
    from matplotlib.finance import(
        quotes_historical_yahoo as quotes_historical_yahoo_ochl        
    )
import csv

path = "sequences/"
prefix = "part-000"
resultMap1 = []
resultMap2 = []
careMap = []
careVues = []
careSizes = []
careIds = []
with open('sampled_carevue/part-00000') as cdata:
    content = cdata.readlines()
    for i in range(len(content)):
        seqs = content[i].split(",")
        patientId = seqs[0]
        seqs = seqs[1:]
        seqs = list(x.replace(")", "").replace("CompactBuffer(", "") for x in seqs)
        seqs = list(x.strip() for x in seqs)
        arrs = np.array(list(np.array(list(float(y) for y in x.split(" "))) for x in seqs))
        if (len(arrs.shape) > 1):
            if (arrs.shape[1] == 8):
                careVues.append(arrs)
                careSizes.append(arrs.shape)
                for x in list(enumerate([patientId] * arrs.shape[0])):
                    careIds.append(x)
                
with open('sampled_carevue/part-00001') as cdata:
    content = cdata.readlines()
    for i in range(len(content)):
        seqs = content[i].split(",")
        patientId = seqs[0]
        seqs = seqs[1:]
        seqs = list(x.replace(")", "").replace("CompactBuffer(", "") for x in seqs)
        seqs = list(x.strip() for x in seqs)
        arrs = np.array(list(np.array(list(float(y) for y in x.split(" "))) for x in seqs))
        if (len(arrs.shape) > 1):
            if (arrs.shape[1] == 8):
                careVues.append(arrs)
                careSizes.append(arrs.shape)
                for x in list(enumerate([patientId] * arrs.shape[0])):
                    careIds.append(x)

careX = np.concatenate(careVues)
careSizes = [x[0] for x in careSizes]
                
for i in range(1, 5):
    print(i)
    Xs = []
    trainIds = []
    sizes = []
    ids = []
    testXs = []
    testIds = []
    testSizes = []
    for suffix in range(0, 33):
        filename = path + prefix + str(suffix).zfill(2)
        with open(filename) as data:
            content = data.readlines()
            indices = list(range(len(content)))
            rand_ind = [indices[i] for i in sorted(random.sample(range(len(indices)), round(len(indices) * 0.75)))]
            for i in range(len(content)):
                seqs = content[i].split(",")
                patientId = seqs[0]
                seqs = seqs[2:]
                seqs = list(x.replace(")", "") for x in seqs)
                seqs = list(x.strip() for x in seqs)
                arrs = np.array(list(np.array(list(float(y) for y in x.split(" ")[1:])) for x in seqs))
                if (len(arrs.shape) > 1):
                    if (arrs.shape[1] == 8 and arrs.shape[0] >= 7):
                        if (i in rand_ind):
                            Xs.append(arrs)
                            sizes.append(arrs.shape)
                            for x in list(enumerate([patientId] * arrs.shape[0])):
                                trainIds.append(x)
                        else:
                            testXs.append(arrs)
                            testSizes.append(arrs.shape)
                            for x in list(enumerate([patientId] * arrs.shape[0])):
                                testIds.append(x)

    
    sizes = [x[0] for x in sizes]
    testSizes = [x[0] for x in testSizes]
    X = np.concatenate(Xs)
    testX = np.concatenate(testXs)
    model = hmm.GaussianHMM(n_components = 2, n_iter = 25).fit(X, sizes)
    model2 = hmm.GaussianHMM(n_components = 2, n_iter = 25)
    model2startprob_ = np.array([1.0, 0.0])
    model2.fit(X, testSizes)
    pred2 = model2.predict_proba(testX, testSizes)
    pred = model.predict_proba(testX, testSizes)
    carePred = model.predict_proba(careX, careSizes)
    careMap.append(list(zip(careIds, carePred.tolist())))
    resultMap1.append(list(zip(testIds, pred.tolist())))
    resultMap2.append(list(zip(testIds, pred2.tolist())))
    
