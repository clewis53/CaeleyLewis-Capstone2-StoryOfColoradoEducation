# -*- coding: utf-8 -*-
"""
Created on Sun Jul  9 10:29:27 2023

@author: caeley
"""
import numpy as np
import pandas as pd


class DataFrameSet:
    """ Class to get transform and save sets of dataframes """
    
    def __init__(self, input_filenames, output_filenames, maker):
        assert len(input_filenames) == len(output_filenames)
        
        self.input_filenames = input_filenames
        self.output_filenames = output_filenames
        self.maker = maker
        
        self.dataframes = [pd.DataFrame([])] * len(input_filenames)
    
    def make_dataframes(self):
        self._get_dataframes()
        self._transform_dataframes()
        self._save_dataframes()
    
    def _get_dataframes(self):
        for i in range(len(self.input_filenames)):
            self.dataframes[i] = pd.read_csv(self.input_filenames[i])
    
    def _transform_dataframes(self):
        for i in range(len(self.dataframes)):
            df_maker = self.maker(self.dataframes[i])
            df_maker.transform()
            
            self.dataframes[i] = df_maker.df
    
    def _save_dataframes(self):
        for i in range(len(self.output_filenames)):
            self.dataframes[i].to_csv(self.output_filenames[i], index=False)
    
    def make_tall(self):
        pass


class Maker:
    
    drop_rows = []
    drop_cols = []
    col_map = {}
        
    def __init__(self, dataframe):
        self.df = dataframe
    
    def transform(self):
        self._transform_rows()
        self._transform_cols()
        
    def _transform_rows(self):
        # Rows
        # Drop specified rows
        self.df = self.df.drop(self.drop_rows)
        # and any completely empty rows
        self.df.dropna(how='all')
        # Then reset the index
        self.df = self.df.reset_index(drop=True)
        
    def _transform_cols(self):
        # Columns
        # Lowercase column names
        self.df.columns = self.df.columns.str.lower()
        # Rename columns
        self.df = self.df.rename(columns=self.col_map)
        # Drop columns
        self.df = self.df.drop(self.drop_cols, axis=1, errors='ignore')
        # Infer Column types
        self.df = self.df.convert_dtypes()
        
        

class CensusMaker(Maker):
    
    col_map = {'sd_name': 'district_name',
               'saepov5_17rv_pt': 'est_child_poverty',
               'saepov5_17v_pt': 'est_total_child',
               'saepovall_pt': 'est_total_pop',
               'time': 'year'}
    drop_cols = ['state', 'school district (unified)']
    
    
    def transform(self):
        self.df = self.df.set_index(self.df.columns[0])
        super().transform()
    
