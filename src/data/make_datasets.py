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
CENSUS_COL_MAP = {'sd_name': 'district_name',
                 'saepov5_17rv_pt': 'est_child_poverty',
                 'saepov5_17v_pt': 'est_total_child',
                 'saepovall_pt': 'est_total_pop',
                 'time': 'year'}
# specifies columns to drop
CENSUS_DROP_COLS = ['state', 'school district (unified)']

# Expenditures dataframe information
# Readable column names
EXP_COL_MAP = {'unnamed: 1': 'county',
               'district/': 'district_name',
               'unnamed: 3': 'instruction',
               'total': 'support',
               'unnamed: 5': 'community',
               'unnamed: 6': 'other',
               'unnamed: 7': 'sum'}
# Columns that won't be used
EXP_DROP_COLS = ['unnamed: 0']

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
# 1YR_3YR_change Dataframes
CHANGE_COL_MAP = {'rate_at.5_chng_ach': 'achievement_dir',
                  'rate_at.5_chng_gro': 'growth_dir',
                  'rate_at.5_chng_growth': 'growth_dir',
                  'pct_pts_chng_.5': 'overall_dir',
                  'pct_pts_chnge_.5': 'overall_dir'}
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

    Returns
    -------
    str, Path
        The original path with the addition

    """
    if type(path) == str:
        return path + '/' + addition
    return path.joinpath(addition)


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

    Returns
    -------
    list
        A list of filepaths.

    """
    return [append_path(filepath, file_extension.format(year=year)) for year in years]


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
        df = pd.read_csv(file, index_col=index_col, header=0)
        # Lower all column names to make renaming and dropping easier
        df.columns = df.columns.str.lower()
        # Drop specified Columns, sometimes they won't exist, and we don't want to raise errors
        # Drop specified rows
        df = df.drop(drop_rows)
        # Drop any rows that contain no data
        df = df.dropna(how='all')
        # Reset the index
        df = df.reset_index(drop=True)
        # Apply column map        
        df = df.rename(columns=col_map)
        # Drop columns
        df = df.drop(drop_cols, axis=1, errors='ignore')
        # Infer dtypes
        df = df.convert_dtypes()
        
        datasets.append(df)
    
    return datasets


def save_dataframes(datasets=[], filenames=[]):
    """
    Saves a set of data to the specified location

    Parameters
    ----------
    datasets : list(DatFrame), optional
        The DataFrames to save. The default is [].
    filenames : list(String), optional
        A list of filenames. The default is [].

    Returns
    -------
    None.

    """
    # Ensure the length of datasets and filenames is the same
    # before continuing
    try: 
        assert len(datasets) == len(filenames)
    
        for i in range(len(datasets)):
            datasets[i].to_csv(filenames[i], index=False)
    except AssertionError:
        print('The datasets could not be saved because', 
              f'the length of datasets was {len(datasets)}',
              f'and length of filenames was {len(filenames)}', 
              sep='\n')



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


def fill_back_forward(datasets, merge_on, columns):
    """
    Fills na values in datasets by backfilling from the most recent observations
    then forward filling from the first observations.

    Parameters
    ----------
    datasets : list(DataFrame)
        A list of datasets to fill. Must be at least 2, and they must have the same column names
    merge_on : String
        A string to indicate the shared column to merge_on
    columns : list(String)
        A list of strings indicating the columns to fill na values in

    Returns
    -------
    list(DataFrame)
        A list of the modified DataFrames.

    """
    def extract_datasets(merged_df, num_datasets, merged_on, all_columns):
        """
        A helper function that extracts original datasets from their merged form

        Parameters
        ----------
        merged_df : DataFrame 
            A DataFrame that is the result of outer joins, and merged in a format where the
            suffixes where _i for each i in range(num_datasets)
        num_datasets : int 
            The number of DataFrames originally merged
        merged_on : string
            The column the DataFrames were merged_on.
        all_columns : list[string]
            The original column names before the merge
            
        Returns
        -------
        datasets : list(DataFrame)
            The list of the extracted DataFrames

        """
        datasets = []
        
        for i in range(num_datasets):
            # determine column names of the dataset to extract
            column_names = all_columns + f'_{i}'
            # extract them
            selected_df = merged_df[column_names]
            # rename the columns to their original title
            selected_df.columns = all_columns
            # Determine what rows the original DataFrame existed
            cond = selected_df[merged_on].notna()
            # Select the correct rows
            datasets.append(selected_df[cond])
        
        return datasets
        
    def fill_in_direction(merged_df, merged_on, fill_suffix, val_suffix, columns):
        """
        Fills the na values in the columns with the fill_suffix using the values from
        the columns with the val_suffix

        Parameters
        ----------
        merged_df : DataFrame
            A DataFrame that is the result of outer joins, and merged in a format where the
            suffixes where _i for each i in range(num_datasets)
        merged_on : String
            The column the DataFrames were merged_on.
        fill_suffix : String
            The suffix to fill on
        val_suffix : String
            The suffix for values to fill with
        columns : list(String)
            The columns to fill values in

        Returns
        -------
        None.

        """
        # Only fill values in columns where the the original fill data existed
        condition = merged_df[merged_on + fill_suffix].notna()
        
        # Apply this method to each column
        for col in columns:
            # The column name to fill in
            fill_column = col + fill_suffix
            # The column name to fill with
            val_column = col + val_suffix
            # Update the values
            merged_df.loc[condition, [fill_column, val_column]] = merged_df.loc[condition, [fill_column, val_column]].fillna(method='bfill', axis=1)

        
    def merge_datasets(datasets, merge_on):
        """
        Merges a list of datasets on merge_on. Adds suffixes as _i for each i
        in range(len(datasets))

        Parameters
        ----------
        datasets : list(DataFrame)
            A list of datasets to fill. Must be at least 2, and they must have the same column names
        merge_on : String
            A string to indicate the shared column to merge_on

        Raises
        ------
        ValueError
            datasets must contain at least two items.

        Returns
        -------
        merged_df : DataFrame
            The merged DataFrame.

        """
        # Check datasets length
        if len(datasets) < 2:
            raise ValueError("datasets must contain at least two DataFrames")

        # Initialize the merged_df with the first dataframe
        # Add a suffix to the merge_on column
        merged_df = datasets[0].rename(columns={col: col+'_0' for col in datasets[0].columns})
        
        
        for i in range(1, len(datasets)):
            # Add suffix to column
            datasets[i].rename(columns={col: col+f'_{i}' for col in datasets[i].columns}, inplace=True)
          
            left_on = merge_on + f'_{i-1}'
            right_on = merge_on + f'_{i}'
            
            # Perform outer merges with remaining datasets continuing to add suffixes
            merged_df = pd.merge(merged_df, datasets[i], 
                                 left_on=left_on, right_on=right_on, how='outer')
        
        return merged_df
        
    
    # Merge datasets
    merged_df = merge_datasets(datasets, merge_on)
        
    # bfill values
    for i in range(len(datasets)-2, -1, -1):
        fill_suffix = f'_{i}'
        val_suffix = f'_{i+1}'
        fill_in_direction(merged_df, merge_on, fill_suffix, val_suffix, columns)
            
    # ffill values
    for i in range(0, len(datasets)-1):
        fill_suffix = f'_{i+1}'
        val_suffix = f'_{i}'
        fill_in_direction(merged_df, merge_on, fill_suffix, val_suffix, columns)
    
    # return extracted datasets
    return extract_datasets(merged_df, merged_on=merge_on,
                            num_datasets = len(datasets), 
                            all_columns = datasets[0].columns)



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
    # make DataFrames from all saipe files
    input_filenames = [append_path(input_filepath, f'saipe{time}.csv') for time in years]
    datasets = get_dataframes(input_filenames, 
                              col_map=CENSUS_COL_MAP, 
                              drop_cols=CENSUS_DROP_COLS, 
                              index_col=0)
    
    # save the DataFrames from all saipe files
    output_filenames = [append_path(output_filepath, f'saipe{time}.csv') for time in years]
    save_dataframes(datasets, output_filenames)
    
    if save_tall:
        tall_df = make_tall(datasets, id_col=years, id_name='year')
        tall_df.to_csv(append_path(output_filepath, 'saipe_tall.csv'))



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
    # make DataFrames from all expenditures files
    input_filenames = [append_path(input_filepath, f'expenditures{time}.csv') for time in years]            
    datasets = get_dataframes(input_filenames,
                              col_map=EXP_COL_MAP,
                              drop_rows=[0,1],
                              drop_cols=EXP_DROP_COLS)
    
    # Transform each dataset
    transformed_datasets = [transform_expenditure_df(df) for df in datasets]
    
    # Save all transformed datasets
    output_filenames = [append_path(output_filepath, f'expenditures{time}.csv') for time in years]
    save_dataframes(transformed_datasets, output_filenames)
    
    if save_tall:
        tall_df = make_tall(datasets, id_col=years, id_name='year')
        tall_df.to_csv(append_path(output_filepath, 'expenditures_tall.csv'))
    
    
def transform_expenditure_df(df):
    """
    A method that transforms expenditures datasets from a report style format
    to a usable format

    Parameters
    ----------
    df : DataFrame
        The original DataFrame that is in a report format.

    Returns
    -------
    DataFrame
        The transformed DataFrame in a usable format.

    """
    
    # All numbers have commas in them that need to be removed
    df = df.replace([',', '\(', '\)'], '', regex=True)
    for col in df.columns:
        if df[col].dtype == 'string':
            df[col] = df[col].str.replace(',','', regex=True)
            df[col] = df[col].str.replace('\(','', regex=True)
            df[col] = df[col].str.replace('\)','', regex=True)
    
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
    
    make_1yr_3yr_change(input_filepath, output_filepath)
    make_coact(input_filepath, output_filepath)
    make_enrl_working(input_filepath, output_filepath)
    make_final_grade(input_filepath, output_filepath)
    make_k_12_frl(input_filepath, output_filepath)
    make_remediation(input_filepath, output_filepath)
    make_school_address(input_filepath, output_filepath)


def remove_boces(data):
    """
    Removes any rows from a dataset where the district_name contains BOCES.

    Parameters
    ----------
    data : DataFrame
        The DataFrame to remove BOCES from

    Raises
    ------
    ValueError
        district_name must be a column that is present in the data.

    Returns
    -------
    None.

    """
    if 'district_name' not in data.columns:
        raise ValueError('The dataframe must contain district_name as one of its columns.')
    
    boces_loc = (data['district_name'].str.upper().str.contains('BOCES'))
    data.drop(data[boces_loc].index, inplace=True)
    

def make_1yr_3yr_change(input_filepath, output_filepath):
    years = 2010, 2011, 2012
    # Load DataFrames
    # The names of the raw files 
    raw_filenames = [append_path(input_filepath, f'{year}_1YR_3YR_change.csv') for year in years]
    # Append standard col changes to specific ones
    CHANGE_COL_MAP.update(KAGGLE_COL_MAP)    
    datasets = get_dataframes(raw_filenames, 
                              index_col=None, 
                              col_map=CHANGE_COL_MAP)     

    # A map to apply to each column that makes more sense than 1,2,3
    trend_arrow_map = {1: '-1',
                       2: '0',
                       3: '1'}
    # The column to apply the map to
    direction_cols = ['achievement_dir','growth_dir','overall_dir']
    
    for df in datasets:
        for col in direction_cols:
            df[col] = df[col].map(trend_arrow_map)
        df.drop(['emh_combined'], axis=1, inplace=True)
        remove_boces(df)
        
      
    output_filenames = [append_path(output_filepath, f'1YR_3YR_change{year}.csv') for year in years]
    save_dataframes(datasets, output_filenames)


def make_coact(input_filepath, output_filepath):
    raw_filenames = create_filenames(input_filepath, '{year}_COACT.csv')
    
    datasets = get_dataframes(raw_filenames, col_map=KAGGLE_COL_MAP, drop_cols=COACT_COL_DROP)
    
    # A map to apply to each column that makes more sense than 1,2
    readiness_map = {1: 1,
                     2: 0,
                     0: 0}
    # The column to apply the map to
    direction_cols = ['eng_yn','math_yn','read_yn','sci_yn']
    
    for df in datasets:
        # Find entries with district results
        dist_res = df['school']=='DISTRICT RESULTS'
        # Find entries with state results
        state_res = df['district_id'] == 0
        # Remove them both
        df.drop(df[dist_res | state_res].index, inplace=True)
        
        # Apply the college readiness map
        for col in direction_cols:
            df[col] = df[col].map(readiness_map)
    
    
    output_filenames = create_filenames(output_filepath, 'COACT{year}.csv')
    
    save_dataframes(datasets, output_filenames)
    
    
def make_enrl_working(input_filepath, output_filepath):
    raw_filenames = create_filenames(input_filepath, '{year}_enrl_working.csv')
    
    datasets = get_dataframes(raw_filenames, col_map=KAGGLE_COL_MAP, drop_cols=ENRL_COL_DROP)
    
    for df in datasets:
        remove_boces(df)
    
    output_filenames = create_filenames(output_filepath, 'enrl_working{year}.csv')
    
    save_dataframes(datasets, output_filenames)


def make_final_grade(input_filepath, output_filepath):
    raw_filenames = create_filenames(input_filepath, '{year}_final_grade.csv')    # Append standard col changes to specific ones
    FINAL_COL_MAP.update(KAGGLE_COL_MAP)    
    datasets = get_dataframes(raw_filenames, 
                              index_col=None, 
                              col_map=FINAL_COL_MAP,
                              drop_cols=FINAL_COL_DROP)     
    
    for df in datasets:
        df.drop(['emh_combined'], axis=1, inplace=True)
        remove_boces(df)
        
    output_filenames = create_filenames(output_filepath, 'final_grade{year}.csv')    
    save_dataframes(datasets, output_filenames)


def make_k_12_frl(input_filepath, output_filepath):
    raw_filenames = create_filenames(input_filepath, '{year}_k_12_FRL.csv')
    
    FRL_COL_MAP.update(KAGGLE_COL_MAP)
    
    datasets = get_dataframes(raw_filenames, 
                              col_map=FRL_COL_MAP,
                              drop_cols=FRL_COL_DROP)
    
    for df in datasets:
        df.drop([len(df)-2, len(df)-1], inplace=True)
        df['pct_fr'] = df['pct_fr'].str.replace('%','').astype('float') / 100
        
        remove_boces(df)
        
    output_filenames = create_filenames(output_filepath, 'FRL{year}.csv')
    
    save_dataframes(datasets, output_filenames)


def make_remediation(input_filepath, output_filepath):
    raw_filenames = create_filenames(input_filepath, '{year}_remediation_HS.csv')
    
    REM_COL_MAP.update(KAGGLE_COL_MAP)
    
    datasets = get_dataframes(raw_filenames, 
                              col_map=REM_COL_MAP,
                              drop_cols=REM_COL_DROP)
    
    
    for df in datasets:
        # Remove percentage signs only where applicable
        try:
            df['pct_remediation'] = df['pct_remediation'].str.replace('%', '').astype('float') / 100
        except AttributeError:
            pass
        remove_boces(df)
        
    
    output_filenames = create_filenames(output_filepath, 'remediation{year}.csv')
    
    save_dataframes(datasets, output_filenames)


def make_school_address(input_filepath, output_filepath):
    raw_filenames = create_filenames(input_filepath, '{year}_school_address.csv')
    
    ADDRESS_COL_MAP.update(KAGGLE_COL_MAP)
    
    datasets = get_dataframes(raw_filenames, 
                              col_map=ADDRESS_COL_MAP, 
                              drop_cols=ADDRESS_COL_DROP)
    
    output_filenames = create_filenames(output_filepath, 'address{year}.csv')
    
    save_dataframes(datasets, output_filenames)

    
    
    
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
