# -*- coding: utf-8 -*-
"""
Created on Thu Feb 16 13:02:25 2023

@author: caeley
"""
import numpy as np
import pandas as pd
from pathlib import Path


# CENSUS dataframe information
# specifies readable column names
CENSUS_COL_MAP = {'SD_NAME': 'district_name',
                 'SAEPOV5_17RV_PT': 'est_child_poverty',
                 'SAEPOV5_17V_PT': 'est_total_child',
                 'SAEPOVALL_PT': 'est_total_pop',
                 'time': 'year'}
# specifies columns to drop
CENSUS_DROP_COLS = ['state', 'school district (unified)']

# Expenditures dataframe information
# Readable column names
EXP_COL_MAP = {'Unnamed: 1': 'county',
               'DISTRICT/': 'district_name',
               'Unnamed: 3': 'instruction',
               'Total': 'support',
               'Unnamed: 5': 'community',
               'Unnamed: 6': 'other',
               'Unnamed: 7': 'sum'}
# Columns that won't be used
EXP_DROP_COLS = ['Unnamed: 0']


def append_path(path, addition):
    """
    Appends the path to a string based on type.

    Parameters
    ----------
    path : str, Path
        The original path
    addition : String
        The string to append to the path

    Returns
    -------
    str, Path
        The original path with the addition

    """
    if type(path) == str:
        return path + '/' + addition
    return path.joinpath(addition)




def get_dataframes(filenames, index_col=None,
                   drop_rows=[], drop_cols=[], col_map={}):
    """
    Takes in a list of filenames to create DataFrames from,
    changes the column names using the column map,
    and drops irrelevant columns. Files must be in .csv format

    Parameters
    ----------
    filenames : list(String), optional
        The names of the files to get names from. The default is [].
    index_col: int
        Specifies where the index column is if at all. The default is none
    drop_rows: list, optional
        Which rows are not necessary. The default is []
    drop_cols : list, optional
        Which columns are not necessary. The default is [].
    col_map : dict, optional
        How to rename the columns. The default is None.
        
    Returns
    -------
    datasets : list(DataFrame)
        a list of the loaded dataframes

    """
    datasets = []
    for file in filenames:
        # Instructs pandas not to recreate the index column,
        # to use the first row as column names,
        # and to drop the specifiec columns
        df = pd.read_csv(file, index_col=index_col, header=0).drop(drop_cols, axis=1).drop(drop_rows)
        df.columns = df.columns.map(col_map) 
        
        datasets.append(df)
    
    return datasets

def save_dataframes(datasets=[], filenames=[]):
    assert len(datasets) == len(filenames)
    
    for i in range(len(datasets)):
        datasets[i].to_csv(filenames[i])
    


def make_tall(datasets, id_col=[], id_name='df_id'):
    """
    Transforms a list of .csv datasets into a single tall .csv dataset

    Parameters
    ----------
    datasets : list(DataFrame)
        A non-empty list of DataFrames to combine.
    id_col: list(int), optional
        A list of values that should identify each dataframe before concatenating them.
        The default is [] or don't add id_col.
    id_name: String, optional
        The name for the id column to be used when id_cols is not empty.
        The default is 'df_id'.

    Returns
    -------
    None.

    """
    # Initialize DataFrame
    tall_df = datasets[0]   
    # Make changes if id_col have been added
    if id_col:
        # The length of id_col must be equal to the number of datasets provided
        assert len(id_col) == len(datasets)
        # Their must be an id_name given when an id_col is specified
        assert id_name != None
        # Add the id column
        tall_df[id_name] = id_col[0]
    
    # Iterate over all datasets and id_col
    for i in range(1, len(datasets)):
        # Add the id column if necessary
        if id_col:
            datasets[i][id_name] = id_col[i]

        tall_df = pd.concat((tall_df, datasets[i]))
        
    return tall_df
        


def make_tall_census(input_filepath, output_filepath, years=(2010, 2011, 2012)):
    """
    Transforms raw census data into usable tall interim data.
    The input filepath must contain saipe datasets that
    that were downloaded using get_raw_data module.
    Finally, it saves the tall_df in the output_filepath
    
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
    input_filenames = [append_path(input_filepath, f'saipe{time}.csv') for time in years]
    datasets = get_dataframes(input_filenames, 
                              col_map=CENSUS_COL_MAP, 
                              drop_cols=CENSUS_DROP_COLS, 
                              index_col=0)
    
    # save the DataFrames from all saipe files
    output_filenames = [append_path(output_filepath, f'census/saipe{time}.csv') for time in years]
    save_dataframes(datasets, output_filenames)
    
    # combine DataFrames into a single tall DataFrame
    tall_df = make_tall(datasets)
    
    # Save the tall DataFrame
    tall_df.to_csv(append_path(output_filepath, 'saipe_tall.csv'))


def make_tall_expenditures(input_filepath, output_filepath, years=(2010, 2011, 2012)):
    """
    Transforms all expenditures datasets that must be Comparison of All 
    Program Expenditures (All Funds) directly downloaded from
    https://www.cde.state.co.us/cdefinance/RevExp and then saved as 
    expenditures{year}.csv
    
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
    # make DataFrames from all expenditures files
    input_filenames = [append_path(input_filepath, f'expenditures{time}.csv') for time in years]            
    datasets = get_dataframes(input_filenames,
                              col_map=EXP_COL_MAP,
                              drop_rows=[0,1],
                              drop_cols=EXP_DROP_COLS)
    
    # Transform each dataset
    transformed_datasets = [transform_expenditure_df(df) for df in datasets]
    
    # Save all transformed datasets
    output_filenames = [append_path(output_filepath, f'expenditures/expenditures{time}.csv') for time in years]
    save_dataframes(transformed_datasets, output_filenames)
    
    # Combine transformed DataFrames into a tall dataframe
    tall_df = make_tall(transformed_datasets, id_col=years, id_name='year')

    # Save tall DataFrame
    tall_df.to_csv(append_path(output_filepath, 'expenditures_tall.csv'))
    
    
def transform_expenditure_df(df):
    df = df.dropna(how='all')
    
    # All numbers have commas in them that need to be removed
    df = df.replace(',','', regex=True)
    
    # The district_name column has numbers that were relevant to the BOCES funding but not our project.
    # We want to be able to identify each of those and remove them.
    def remove_floats(entry):
        try:
            float(entry)
            return np.nan
        except:
            return entry
        
    df['district_name'] = df['district_name'].apply(remove_floats)
    
    # Now that they are removed, lets forward fill the district_name,
    # so that we can extract the total amount for each category
    # and the per pupil amount for each category
    df['district_name'] = df['district_name'].fillna(method='ffill')
    
    # Remove all BOCES funding entries because they are irrelevant
    df = df[~(df['district_name'].str.lower().str.contains('boces'))]
    
    # Now we can extract the total amounts
    totals = df[df['county'].str.lower() == 'amount'].drop('county', axis=1)
    # the per pupil amounts 
    per_pupils = df[df['county'].str.lower() == 'per pupil'].drop('county', axis=1)
    # and the county names
    counties = df.loc[~(df['county'].str.lower().isin(('amount', 'per pupil', 'all funds'))), ['district_name', 'county']]
    
    # Now we can merge them
    merged_df = pd.merge(left=totals, right=per_pupils, on='district_name', suffixes=('_total', '_per_pupil'))
    final_df = pd.merge(left=counties, right=merged_df, on='district_name')
    
    return final_df


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
    # make_tall_census(append_path(input_filepath, 'census'), 
    #                  output_filepath)
    make_tall_expenditures(append_path(input_filepath, 'expenditures'), 
                           output_filepath)
    # make_tall_kaggle(input_filepath.joinpath('kaggle'), output_filepath)


if __name__ == '__main__':
    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]    
    input_filepath = project_dir.joinpath("data/raw")
    output_filepath = project_dir.joinpath("data/interim")
    
    main(input_filepath, output_filepath)
