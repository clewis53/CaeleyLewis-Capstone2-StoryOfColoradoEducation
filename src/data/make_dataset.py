# -*- coding: utf-8 -*-
"""
Created on Thu Feb 16 13:02:25 2023

@author: caeley
"""
import pandas as pd
from pathlib import Path

# Each year that should be analyzed
YEARS = 2010, 2011, 2012

# CENSUS dataframe information
# specifies readable column names
CENSUS_COL_MAP = {'SD_NAME': 'district_name',
               'SAEPOV5_17RV_PT': 'est_child_poverty',
               'SAEPOV5_17V_PT': 'est_total_child',
               'SAEPOVALL_PT': 'est_total_pop',
               'time': 'year'
               }
# specifies columns to drop
CENSUS_DROP_COLS = ['state', 'school district (unified)']


def get_dataframes(filenames=[], read_method=pd.read_csv, col_map={}, drop_cols=[]):
    """
    Takes in a list of filenames to create DataFrames from,
    changes the column names using the column map,
    and drops irrelevant columns.

    Parameters
    ----------
    filenames : TYPE, optional
        DESCRIPTION. The default is [].
    col_map : TYPE, optional
        DESCRIPTION. The default is {}.
    drop_cols : TYPE, optional
        DESCRIPTION. The default is [].

    Returns
    -------
    datasets : TYPE
        DESCRIPTION.

    """
    datasets = []
    for file in filenames:
        # Instructs pandas not to recreate the index column,
        # to use the first row as column names,
        # and to drop the specifiec columns
        df = read_method(file, index_col=0, header=0).drop(drop_cols, axis=1)
        df.columns = df.columns.map(col_map) 
        
        datasets.append(df)
    
    return datasets


def make_tall(datasets=[], output_filename='df.csv'):
    """
    Transforms a list of .csv datasets into a single tall .csv dataset
    and saves it.

    Parameters
    ----------
    input_filenames : list(String), optional
        A list of .csv filenames. The default is [].
    output_filename : String, optional
        The name of the file to save to
    col_names : list(String), optional
        A list of strings to transform column names to, optional

    Returns
    -------
    None.

    """
    tall_df = pd.DataFrame(data=[], columns=datasets[0].columns)
    
    for df in datasets:
        tall_df = pd.concat((tall_df, df))
        
    tall_df.to_csv(output_filename)
        


def make_tall_census(input_filepath, output_filepath):
    """
    Transforms raw census data into usable tall interim data 
    
    Parameters
    ----------
    input_filepath : str, Path
        the directory to obtain files from
    output_filepath : str, Path
        the directory to save files in

    Returns
    -------
    None.
    """
    # make DataFrames from all saipe files
    filenames = [input_filepath.joinpath(f'saipe{time}.csv') for time in YEARS]
    datasets = get_dataframes(filenames=filenames, col_map=CENSUS_COL_MAP, drop_cols=CENSUS_DROP_COLS)
    
    # combine DataFrames and save
    output_filename = output_filepath.joinpath('saipe_tall.csv')
    make_tall(datasets, output_filename)
    

def make_tall_expenditures(input_filepath, output_filepath):
    """
    Transforms all expenditures raw data into usable tall interim data
    
    Parameters
    ----------
    input_filepath : str, Path
        the directory to obtain files from
    output_filepath : str, Path
        the directory to save files in

    Returns
    -------
    None.

    """
    # make DataFrames from all saipe files
    filenames = [input_filepath.joinpath(f'expenditures{time}.xls') for time in YEARS]
    datasets = get_dataframes(filenames, pd.read_excel)
    
    # combine DataFrames and save
    output_filename = output_filepath.joinpath('expenditures_tall.csv')
    make_tall(datasets, output_filename)


def make_tall_kaggle(input_filepath, output_filepath):
    """
    Transforms each kaggle raw dataset into individual usable tall interim data
    
    Parameters
    ----------
    input_filepath : str, Path
        the directory to obtain files from
    output_filepath : str, Path
        the directory to save files in

    Returns
    -------
    None.

    """


def make_1yr_3yr_change(datasets=[]):
    pass


def make_coact(datasets=[]):
    pass


def make_enrl_working(datasets=[]):
    pass


def make_final_grade(datasets=[]):
    pass


def make_k_12_flr(datasets=[]):
    pass


def make_remediation(datasets=[]):
    pass


def make_school_address(datasets=[]):
    pass

    
    
    

def main(input_filepath, output_filepath):
    """
    Transforms raw data into usable data saved as interim

    Parameters
    ----------
    output_filepath : str, Path, optional
        The directory to save files in. The default is './'.

    Returns
    -------
    None.

    """
    make_tall_census(input_filepath.joinpath('census'), output_filepath)
    make_tall_expenditures(input_filepath.joinpath('expenditures'), output_filepath)
    make_tall_kaggle(input_filepath.joinpath('kaggle'), output_filepath)


if __name__ == '__main__':
    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]    
    input_filepath = project_dir.joinpath("data/raw")
    output_filepath = project_dir.joinpath("data/interim")
    
    main(input_filepath, output_filepath)
