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
            cond = selected_df[merged_on[0]].notna()
            # Select the correct rows and drop_duplicates
            datasets.append(selected_df[cond].drop_duplicates(subset=merged_on))
        
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
        condition = merged_df[merged_on[0] + fill_suffix].notna()
        
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
          
            left_on = [on + f'_{i-1}' for on in merge_on]
            right_on = [on + f'_{i}' for on in merge_on]
            
            # Perform outer merges with remaining datasets continuing to add suffixes
            merged_df = pd.merge(merged_df, datasets[i], 
                                 left_on=left_on, right_on=right_on, how='outer')
        
        return merged_df
        
    
    if type(merge_on) == 'string':
        merge_on = [merge_on]
    
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
