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

    def test_csv_to_2d_ndarray(self):
        return self.data_loader.csv_to_2d_ndarray()
    
def main():
    tester = DataLoaderTester()
    data = tester.data_loader.read_features()
    print("data sent")

if __name__ == '__main__':
    main()