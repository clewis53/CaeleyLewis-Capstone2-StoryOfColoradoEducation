# -*- coding: utf-8 -*-
"""
Created on Mon Jul 10 18:35:03 2023

@author: caeley
"""
import pandas as pd
from pathlib import Path
import builders

def combine_datasets(input_filepath, output_filepath, census, exp, kaggle):
    # Extract kaggle datasets
    change, coact, enroll, final, frl, remediation, address = kaggle
    
    # Build district dataset
    district = create_district_dataset(input_filepath, output_filepath,
                                       change, coact, enroll, final, frl)
    # Build school dataset
    school = create_school_dataset(input_filepath, output_filepath,
                                   change, final)
    
    # Update datasets by removing district and school information
    updated_datasets = remove_district_and_school_info(census, 
                                                       exp, 
                                                       change, coact, enroll, final, frl, remediation,
                                                       district, school)
    # Extract updated datasets
    census, exp, change, coact, enroll, final, frl, remediation = updated_datasets
      
    # Build all data
    all_data = create_all_data(input_filepath, output_filepath, census, exp, change, enroll, final, frl)
    
    # Build high school data
    high_school = create_high_school(input_filepath, output_filepath, coact, remediation, all_data)
    
    return district, school, all_data, high_school
    
    
def create_district_dataset(input_filepath, output_filepath, 
                            change, coact, enroll, final, frl):
    
    district_builder = builders.DistrictIDBuilder((change, coact, enroll, final, frl))
    district_builder.build()
    
    return district_builder.id_dataset
    

def create_school_dataset(input_filepath, output_filepath, 
                          change, final):
    kaggle_datasets = change, final
    
    school_builder = builders.SchoolIDBuilder(kaggle_datasets)
    school_builder.build()
    
    return school_builder.id_dataset


def create_all_data(input_filepath, output_filepath, 
                    census, 
                    exp, 
                    change, enroll, final, frl):
    census_exp_df = combine_census_exp(census, exp)
    change_final_df = combine_change_final(change, final)
     
    
def create_high_school(input_filepath, output_filepath,
                       coact, remediation,
                       all_data):
    coact_remediation = combine_coact_remediation(coact, remediation)


def remove_district_and_school_info(census, 
                                    exp, 
                                    change, coact, enroll, final, frl, remediation, 
                                    district, school):
    pass


def add_district_id(dataset):
    if 'district_name' not in dataset.columns:
        raise ValueError('district_name must be a column in the dataset')


def combine_census_exp(census, exp):
    pass


def combine_change_final(change, final):
    pass


def combine_coact_remediation(coact, remediation):
    pass


def main(input_filepath, output_filepath):
    census = pd.read_csv('data\\interim\\census\\tall_saipe.csv')
    exp = pd.read_csv('data\\interim\\expenditures\\tall_expenditures.csv')
    change = pd.read_csv('data\\interim\\kaggle\\1YR_3YR_change_tall.csv')
    coact = pd.read_csv('data\\interim\\kaggle\\1YR_3YR_change_tall.csv')
    enroll = pd.read_csv('data\\interim\\kaggle\\enrl_working_tall.csv')
    final = pd.read_csv('data\\interim\\kaggle\\final_grade_tall.csv')
    frl = pd.read_csv('data\\interim\\kaggle\\FRL_tall.csv')
    remediation = pd.read_csv('data\\interim\\kaggle\\remediation_tall.csv')
    address = pd.DataFrame()


if __name__ == '__main__':
    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]    
    input_filepath = project_dir.joinpath("data/interim")
    output_filepath = project_dir.joinpath("data")
    
    main(input_filepath, output_filepath)
