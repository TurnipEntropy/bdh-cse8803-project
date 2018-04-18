# -*- coding: utf-8 -*-
"""
Created on Tue Apr 17 23:56:47 2018

@author: Entropic Turnips
"""

import os
import re

class DataLoader:
    def __init__(self, directory):
        self.directory = directory
        
    def read_labels(self):
        files = os.listdir(self.directory)
        pattern = re.compile('part-[0-9]{0,5}?$')
        data_files = list(filter(lambda x: re.match(pattern, x) != None, files))
        labels = {}
        for file in data_files:
            with open(self.directory + file) as data:
                content = data.readlines()
                print(len(content))
                for i in range(len(content)):
                    seq = content[i].split(",")
                    icu_id = seq[5]
                    labels[icu_id] = seq[3]
        return labels