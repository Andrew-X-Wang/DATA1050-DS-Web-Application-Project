import pandas as pd


MISS_P_THRESH = 0.5
DB_LIMIT = 8000 # max num of rows*0.9

tablename_to_csv_urls = {
    'covid': 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv',
    'covidhistorical':'https://covid.ourworldindata.org/data/owid-covid-data.csv'
}


def drop_cols_na(df, cols_missing_count, num_rows, miss_p_thresh, verbose=False):
    cols_to_keep = []
    for c in df.columns:
        if cols_missing_count[c]/num_rows < miss_p_thresh:
            cols_to_keep.append(c)

    num_cols_kept = len(cols_to_keep)
    if verbose:
        print(f"Keeping {num_cols_kept} columns out of {cols_missing_count.shape[0]} w over {miss_p_thresh} values non-missing")
        print(cols_to_keep)

    df_col_lim = df[cols_to_keep]
    return df_col_lim, num_cols_kept


def drop_rows_na(df, rows_missing_count, num_cols, miss_p_thresh, verbose=False):
    rows_to_keep = []
    for idx, row in df.iterrows():
        if rows_missing_count[idx]/num_cols < miss_p_thresh:
            rows_to_keep.append(idx)

    num_rows_kept = len(rows_to_keep)
    if verbose:
        print(f"Keeping {num_rows_kept} rows out of {rows_missing_count.shape[0]} w over {MISS_P_THRESH} values non-missing")
    
    df_row_lim = df.iloc[rows_to_keep]
    return df_row_lim, num_rows_kept


def clean_null_data(df, miss_p_thresh=MISS_P_THRESH, verbose=False):
    num_rows, num_cols = df.shape
    rows_missing_count = df.isna().sum(axis=1)
    cols_missing_count = df.isna().sum(axis=0)

    # Remove columns
    df, num_cols_kept = drop_cols_na(df, cols_missing_count, num_rows, miss_p_thresh, verbose)
    if verbose:
        print("Removed columns with too many null values.")

    # Remove rows
    df, num_rows_kept = drop_rows_na(df, rows_missing_count, num_cols_kept, miss_p_thresh, verbose)
    if verbose:
        print("Removed rows with too many null values.")
    
    return df


def read_csv(dataset):
    if dataset not in tablename_to_csv_urls.keys():
        print(f"'{dataset}' is not a valid dataset")
        return -1
    else:
        df = pd.read_csv(tablename_to_csv_urls[dataset])
        return df
