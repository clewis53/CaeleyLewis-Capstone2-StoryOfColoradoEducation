# -*- coding: utf-8 -*-
"""
Created on Mon Jul 10 19:36:49 2023

@author: caeley
"""
import pandas as pd


class IDDatasetBuilder:
    # The columns that are pertinent to the ID Dataset
    keep_cols = []
    # Save the id_cols to search duplicates for
    id_cols = []
    
    def __init__(self, kaggle_datasets):
        # Save only pertinent information of datasets
        self.datasets = [dataset[self.keep_cols] for dataset in kaggle_datasets]
        # Initialize id_dataset
        self.id_dataset = pd.DataFrame()
        
    def build(self):
        # Create a long list of id_datasets
        self.id_dataset = self._concatenate()
        
        # Remove duplicates
        self.id_dataset = self.id_dataset.drop_duplicates(subset=self.id_cols)
        
    def _concatenate(self):
        self.id_dataset = self.datasets[0]
        
        for i in range(1, len(self.datasets)):
            self.id_dataset = pd.concat((self.id_dataset, self.datasets[i]))
            
    
class SchoolIDBuilder(IDDatasetBuilder):
    keep_cols = ['school_id', 'school', 'emh', 'district_id']
    id_cols = ['school_id', 'emh']
    

class DistrictIDBuilder(IDDatasetBuilder):
    keep_cols = ['district_id' 'district_name']
    id_cols = ['district_id']
    
    changes = {'SCHOOL ': '',
               'DISTRICT ': '',
               '-': ' ',
               ':': ' ',
               'S/D ': '',
               '/': ' ',
               r'[^\w\s]+': '',
               'RURAL ': '',
               'NO2': '2',
               '29J': '29 J',
               '49JT':'49 JT',
               'RE1J':'RE 1J',
               'MILIKEN': 'MILLIKEN',
               'NO 1': '1',
               'PARK ESTES PARK': 'PARK',
               'PARK R 3': 'ESTES PARK R 3',
               'WELD RE 1': 'WELD COUNTY RE 1',
               'MOFFAT 2': 'MOFFAT COUNTY 2',
               '10 JT R': 'R 10 JT',
               'GARFIELD 16': 'GARFIELD COUNTY 16',
               'MOFFAT CONSOLIDATED': 'MOFFAT COUNTY',
               'Ñ': 'N',
               'NORTHGLENN THORNTON 12': 'ADAMS 12 FIVE STAR',
               'CONSOLIDATED C 1': 'CUSTER COUNTY C 1'
              }
    
    def build(self):
        super().build()
        self._transform_district_name()
        
        
    def _transform_district_name(self):
        # Uppercase the district_names
        self.id_dataset['district_name'] = self.id_dataset['district_name'].str.upper()
        
        # Apply all changes
        for original, replacement in self.changes.enumerate():
            self.id_dataset['district_name'] = self.id_dataset['district_name'].str.replace(original, replacement)
    
    
