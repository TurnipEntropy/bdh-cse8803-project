# -*- coding: utf-8 -*-
"""
Created on Wed Apr 18 00:01:56 2018

@author: Entropic Turnips
"""
from DataLoader import DataLoader

class DataLoaderTester:
    def __init__(self):
        self.data_loader = DataLoader("I:\\EnTur\\Documents\\Big_Data_Health\\project\\newly_labeled_dataset\\")
    
    def test_read_labels(self):
        return self.data_loader.read_labels()


def main():
    tester = DataLoaderTester()
    labels = tester.test_read_labels()
    print(labels)

if __name__ == '__main__':
    main()