# -*- coding: utf-8 -*-
"""
Created on Tue Apr 17 23:56:47 2018

@author: Entropic Turnips
"""

import os
import re
import np

class DataLoader:
    def __init__(self, directory):
        self.directory = directory
        
    def __get_files__(self):
        files = os.listdir(self.directory)
        pattern = re.compile('part-[0-9]{0,5}?$')
        data_files = list(filter(lambda x: re.match(pattern, x) != None, files))
        return data_files

    
    def clean_csvs(self):
        data_files = self.__get_files__()
        pattern = re.compile('[^0-9\.,:]')
        csv = []
        for file in data_files:
            with open(self.directory + file) as data:
                content = data.readlines()
                cleaned = [re.sub(pattern, "", x) for x in content]
                csv.append(np.array(cleaned))
        return np.array(csv)
        
        
    def read_labels(self):
        data_files = self.__get_files__()
        labels = {}
        for file in data_files:
            with open(self.directory + file) as data:
                content = data.readlines()
                for i in range(len(content)):
                    seq = content[i].split(",")
                    icu_id = seq[5]
                    labels[icu_id] = seq[3]
        return labels
    
    def read_data(self):
        data_files = self.__get_files__()
        hash_table = {}
        Xs = []
        sizes = []
        for file in data_files:
            with open(self.directory + file) as data:
                content = data.readlines()
                for i in range(len(content)):
                    seq = content[i].split(",")
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
            sizes.append((arrs.shape)[0])
        return (Xs, sizes)
    
    def read_labels_and_data(self):
        data_files = self.__get_files__()
        hash_table = {}
        Xs = []
        sizes = []
        labels = {}
        for file in data_files:
            with open(self.directory + file) as data:
                content = data.readlines()
                for i in range(len(content)):
                    seq = content[i].split(",")
                    icu_id = seq[5]
                    data_str = seq[7:13]
                    data_str.append(seq[14].replace(")", ""))
                    data_arr = np.array(list(float(x.strip()) for x in data_str))
                    labels[icu_id] = seq[3]
                    if (icu_id in hash_table):
                        hash_table[icu_id].append(data_arr)
                    else:
                        hash_table[icu_id] = []
                        hash_table[icu_id].append(data_arr)
                        
        for k, v in hash_table.items():
            arrs = np.array(v)
            Xs.append(arrs)
            sizes.append((arrs.shape)[0])
            
        return (Xs, sizes, labels)