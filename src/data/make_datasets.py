# -*- coding: utf-8 -*-
"""
Created on Thu Feb 16 13:02:25 2023

@author: caeley
"""
import pandas as pd
from pathlib import Path
from makers import DataFrameSet
import makers

#
NO_FILL = ['school', 'school_id', 'district_name', 'district_id']

# Kaggle dataframes information
# Standardized column names 
KAGGLE_COL_MAP = {'emh-combined': 'emh_combined',
                  'emh_combined': 'emh_combined',
                  'spf_emh_code': 'emh',
                  'spf_included_emh_for_a': 'emh_combined',
                  'school_name': 'school',
                  'schoolname': 'school',
                  'school name': 'school',
                  '2010 school name': 'school',
                  'spf_school_name': 'school',
                  'district code': 'district_id',
                  'districtnumber': 'district_id',
                  'district no': 'district_id',
                  'district number': 'district_id',
                  'districtnumber': 'district_id',
                  'spf_dist_number': 'district_id',
                  'org. code': 'district_id',
                  'organization code': 'district_id',
                  'districtname': 'district_name',
                  'district name': 'district_name',
                  'spf_district_name': 'district_name',
                  'organization name': 'district_name',
                  'school_district': 'district_name',
                  'school_districte': 'district_name',
                  'schoolnumber': 'school_id',
                  'school code': 'school_id',
                  'school number': 'school_id',
                  'school no': 'school_id',
                  'spf_school_number': 'school_id'}

# COACT DataFrames
COACT_COL_DROP = ['district_name']
# Enrl_working DataFrames
ENRL_COL_DROP = ['unnamed: 12', 'unnamed: 13',	'unnamed: 14']
# Final_Grade Dataframes
FINAL_COL_DROP = ['emh_2lvl', 
                  'LT100pnts', 
                  'aec10',
                  'alternative_school',
                  'charter',
                  'charteroronline',
                  'ell_growth_grade',
                  'emh_2lvl',
                  'final_plan',
                  'highestgrade',
                  'initial_plan',
                  'lowestgrade',
                  'lt100pnts',
                  'notes',
                  'online',
                  'overall_ach_grade',
                  'overall_achievement',
                  'record_no',
                  'spf_ps_ell_grad_rate']
FINAL_COL_MAP = {'aec_10': 'alternative_school',
                 'initial_plantype': 'initial_plan',
                 'final_plantype': 'final_plan', 
                 'rank_tot': 'rank',
                 'overall_ach_grade': 'overall_achievement',
                 'read_ach_grade': 'read_achievement',
                 'read_ach_grade': 'read_achievement',
                 'math_ach_grade': 'math_achievement',
                 'write_ach_grade': 'write_achievement',
                 'sci_ach_grade': 'science_achievment',
                 'overall_weighted_growth_grade': 'overall_weighted_growth',
                 'read_growth_grade': 'read_growth',
                 'math_growth_grade': 'math_growth',
                 'write_growth_grade': 'write_growth',
                 'spf_ps_ind_grad_rate': 'graduation_rate'}
# FRL Dataframes
FRL_COL_DROP = ['unnamed: 5', 'unnamed: 6', 'unnamed: 7']
FRL_COL_MAP = {'% free and reduced': 'pct_fr'}
# Remediation DataFrames
REM_COL_DROP = ['unnamed: 5', 'this is 2011 data: created nov 7, 2012', 'public_private']
REM_COL_MAP = {'remediation_atleastone_pct2010': 'pct_remediation',
               'remediation_at_leastone_pct2010': 'pct_remediation'}
# Address DataFrames
ADDRESS_COL_DROP = ['phone', 'physical address']
ADDRESS_COL_MAP = {'physical city': 'city',
                  'physical state': 'state',
                  'physical zipcode': 'zipcode'}


def append_path(path, addition):
    """
    Appends the path to a string based on type.

    Parameters
    ----------
    path : str, Path
        The original path
    addition : String
        The string to append to the path
        
    Raises
    ------
    TypeError
        The file_extension must contain {year}.

    Returns
    -------
    str, Path
        The original path with the addition

    """
    if type(addition) != str:
        raise TypeError('The addition must be of type string')
    
    if type(path) == str:
        return path + '/' + addition
    elif Path in type(path).mro():
        return path.joinpath(addition)
    else:
        raise TypeError('Path must be of type string or Path')
        

def create_filenames(filepath, file_extension='{year}', years=(2010,2011,2012)):
    """
    A function to append a year file_extension to each final path

    Parameters
    ----------
    filepath : String, Path
        The filepath
    file_extension : String, optional
        The extension to add to each file location. Must_containe {years}.
        The default is '{year}'.
    years : , optional
        The years to add. The default is (2010,2011,2012).

    Raises
    ------
    ValueError
        The file_extension must contain {year}.

    Returns
    -------
    list
        A list of filepaths.

    """
    if '{year}' not in file_extension:
        raise ValueError('{year} must be in the file_extension')
    
    return [append_path(filepath, file_extension.format(year=year)) for year in years]


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
    tall_df: DataFrame
        The dataframe of the tall version of the given datasets

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



def make_census(input_filepath, output_filepath, years=(2010, 2011, 2012), save_tall=True):
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
    # Input and output locations
    input_filenames = create_filenames(input_filepath, 'saipe{year}.csv')
    output_filenames = create_filenames(output_filepath, 'saipe{year}.csv')
    
    # MakeDatasets
    dataframes = DataFrameSet(input_filenames, output_filenames, makers.CensusMaker)
    dataframes.make_dataframes()
    
    # if save_tall:
    #     tall_df = make_tall(datasets, id_col=years, id_name='year')
    #     tall_df.to_csv(append_path(output_filepath, 'saipe_tall.csv'))



def make_expenditures(input_filepath, output_filepath, years=(2010, 2011, 2012), save_tall=True):
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
    # Input and output locations    
    input_filenames = create_filenames(input_filepath, 'expenditures{year}.csv')
    output_filenames = create_filenames(output_filepath, 'expenditures{year}.csv')
    
    # Make datasets
    datasets = DataFrameSet(input_filenames, output_filenames, makers.ExpenditureMaker)
    datasets.make_dataframes()
    
    # if save_tall:
    #     tall_df = make_tall(datasets, id_col=years, id_name='year')
    #     tall_df.to_csv(append_path(output_filepath, 'expenditures_tall.csv'))
    



def make_kaggle(input_filepath, output_filepath):
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
    
    # make_1yr_3yr_change(input_filepath, output_filepath)
    # make_coact(input_filepath, output_filepath)
    # make_enrl_working(input_filepath, output_filepath)
    # make_final_grade(input_filepath, output_filepath)
    make_k_12_frl(input_filepath, output_filepath)
    # make_remediation(input_filepath, output_filepath)
    # make_school_address(input_filepath, output_filepath)
    

def make_1yr_3yr_change(input_filepath, output_filepath):
    """
    Transforms 1yr_3yr_change datasets downloaded from the kaggle competition

    Parameters
    ----------
    input_filepath : String, Path
        The input filepath base to extract data from
    output_filepath : String, Path
        The output filepath base to save data to

    Returns
    -------
    None.

    """
    
    input_filenames = create_filenames(input_filepath, '{year}_1YR_3YR_change.csv')
    output_filenames = create_filenames(output_filepath, '1YR_3YR_change{year}.csv')
    
    datasets = DataFrameSet(input_filenames, output_filenames, makers.ChangeMaker)
    datasets.make_dataframes()
    

def make_coact(input_filepath, output_filepath):
    input_filenames = create_filenames(input_filepath, '{year}_COACT.csv')
    
    # datasets = get_dataframes(input_filenames, col_map=KAGGLE_COL_MAP, drop_cols=COACT_COL_DROP)
    
    # # A map to apply to each column that makes more sense than 1,2
    # readiness_map = {1: 1,
    #                  2: 0,
    #                  0: 0}
    # # The column to apply the map to
    # direction_cols = ['eng_yn','math_yn','read_yn','sci_yn']
    
            
    
    # datasets = fill_back_forward(datasets, ['school_id'], direction_cols)
    
    # for df in datasets:
    #     # Find entries with district results
    #     dist_res = df['school']=='DISTRICT RESULTS'
    #     # Find entries with state results
    #     state_res = df['district_id'] == 0
    #     # Remove them both
    #     df.drop(df[dist_res | state_res].index, inplace=True)
        
    #     # Apply the college readiness map
    #     for col in direction_cols:
    #         df[col] = df[col].map(readiness_map)
    
    output_filenames = create_filenames(output_filepath, 'COACT{year}.csv')
    
    datasets = DataFrameSet(input_filenames, output_filenames, makers.CoactMaker)
    datasets.make_dataframes()
    
    # save_dataframes(datasets, output_filenames)
    
    
def make_enrl_working(input_filepath, output_filepath):
    input_filenames = create_filenames(input_filepath, '{year}_enrl_working.csv')
    
    # datasets = get_dataframes(input_filenames, col_map=KAGGLE_COL_MAP, drop_cols=ENRL_COL_DROP)
    
    # for df in datasets:
    #     remove_boces(df)
    
    output_filenames = create_filenames(output_filepath, 'enrl_working{year}.csv')
    
    # save_dataframes(datasets, output_filenames)
    
    datasets = DataFrameSet(input_filenames, output_filenames, makers.EnrollMaker)
    datasets.make_dataframes()
    

def make_final_grade(input_filepath, output_filepath):
    input_filenames = create_filenames(input_filepath, '{year}_final_grade.csv')    # Append standard col changes to specific ones
    # FINAL_COL_MAP.update(KAGGLE_COL_MAP)    
    # datasets = get_dataframes(input_filenames, 
    #                           index_col=None, 
    #                           col_map=FINAL_COL_MAP,
    #                           drop_cols=FINAL_COL_DROP)     
    
    # for df in datasets:
    #     df.drop(['emh_combined'], axis=1, inplace=True)
    #     remove_boces(df)
        
    output_filenames = create_filenames(output_filepath, 'final_grade{year}.csv')    
    # save_dataframes(datasets, output_filenames)
    datasets = DataFrameSet(input_filenames, output_filenames, makers.FinalMaker)
    datasets.make_dataframes()

def make_k_12_frl(input_filepath, output_filepath):
    input_filenames = create_filenames(input_filepath, '{year}_k_12_FRL.csv')
    
    # FRL_COL_MAP.update(KAGGLE_COL_MAP)
    
    # datasets = get_dataframes(input_filenames, 
    #                           col_map=FRL_COL_MAP,
    #                           drop_cols=FRL_COL_DROP)
    
    # for df in datasets:
    #     df.drop([len(df)-2, len(df)-1], inplace=True)
    #     df['pct_fr'] = df['pct_fr'].str.replace('%','').astype('float') / 100
        
    #     remove_boces(df)
        
    output_filenames = create_filenames(output_filepath, 'FRL{year}.csv')
    
    # save_dataframes(datasets, output_filenames)
    
    datasets = DataFrameSet(input_filenames, output_filenames, makers.FrlMaker)
    datasets.make_dataframes()

def make_remediation(input_filepath, output_filepath):
    input_filenames = create_filenames(input_filepath, '{year}_remediation_HS.csv')
    
    # REM_COL_MAP.update(KAGGLE_COL_MAP)
    
    # datasets = get_dataframes(input_filenames, 
    #                           col_map=REM_COL_MAP,
    #                           drop_cols=REM_COL_DROP)
    
    
    # for df in datasets:
    #     # Remove percentage signs only where applicable
    #     try:
    #         df['pct_remediation'] = df['pct_remediation'].str.replace('%', '').astype('float') / 100
    #     except AttributeError:
    #         pass
    #     remove_boces(df)
        
    
    output_filenames = create_filenames(output_filepath, 'remediation{year}.csv')
    
    # save_dataframes(datasets, output_filenames)
    
    datasets = DataFrameSet(input_filenames, output_filenames, makers.RemediationMaker)
    datasets.make_dataframes()


def make_school_address(input_filepath, output_filepath):
    input_filenames = create_filenames(input_filepath, '{year}_school_address.csv')
    
    # ADDRESS_COL_MAP.update(KAGGLE_COL_MAP)
    
    # datasets = get_dataframes(input_filenames, 
    #                           col_map=ADDRESS_COL_MAP, 
    #                           drop_cols=ADDRESS_COL_DROP)
    
    output_filenames = create_filenames(output_filepath, 'address{year}.csv')
    
    # save_dataframes(datasets, output_filenames)
    
    datasets = DataFrameSet(input_filenames, output_filenames, makers.AddressMaker)
    datasets.make_dataframes()
    
    
    
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
    make_census(append_path(input_filepath, 'census'), 
                      append_path(output_filepath, 'census'))
    make_expenditures(append_path(input_filepath, 'expenditures'), 
                            append_path(output_filepath, 'expenditures'))
    make_kaggle(append_path(input_filepath,'kaggle'), 
                     append_path(output_filepath,'kaggle'))


if __name__ == '__main__':
    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]    
    input_filepath = project_dir.joinpath("data/raw")
    output_filepath = project_dir.joinpath("data/interim")
    
    main(input_filepath, output_filepath)
