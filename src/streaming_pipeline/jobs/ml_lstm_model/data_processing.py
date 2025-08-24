import pandas as pd
import numpy as np
import logging

def load_and_process_data(file_path=None, df=None):
    """Load data from file or DataFrame, preprocess, and apply feature engineering."""
    if file_path:
        try:
            df = pd.read_csv(file_path).dropna()
        except Exception as e:
            logging.error(f"Error reading file {file_path}: {e}")
            raise
    if df is None:
        raise ValueError("No data provided for processing.")

    assert df['Timestamp'].is_monotonic_increasing, "Timestamp is not sorted"
    
    df['aid_int'] = df['Arbitration_ID'].apply(lambda x: int(x, 16))
    df['time_interval'] = df.groupby('Arbitration_ID')['Timestamp'].diff().fillna(0)

    # Calculate rolling entropy for Arbitration IDs
    df['entropy'] = df.rolling(window=5, min_periods=1)['aid_int'].apply(lambda x: get_H(x))
    df['entropy'] = df['entropy'].ffill()

    data_field_values = [
        [int(i, 16) for i in field.split(' ')] + [-1] * (8 - len(field.split(' ')))
        for field in df['Data']
    ]
    data_fields = pd.DataFrame(data_field_values, columns=[f'datafield{i}' for i in range(8)])

    features_df = pd.concat([df.reset_index(drop=True), data_fields.reset_index(drop=True)], axis=1)
    features = features_df[['aid_int', 'time_interval', 'entropy'] + [f'datafield{i}' for i in range(8)]]

    print("\nFinal features DataFrame:")
    print(features.head(10))
    
    print("\nFeatures Statistics:")
    print(features.describe())
    
    logging.info("Data processed successfully.")
    return features

def get_H(series_aid):
    count = series_aid.value_counts()
    p_i = count / series_aid.shape[0]
    return -(p_i * np.log(p_i)).sum()
