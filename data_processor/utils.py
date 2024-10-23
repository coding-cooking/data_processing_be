import pandas as pd
import json

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()  # Convert Timestamps to string
        elif isinstance(obj, pd.Timedelta):
            return str(obj)  # Convert Timedeltas to string
        return super(NumpyEncoder, self).default(obj)

def infer_and_convert_data_types(df):
    for col in df.columns:
        # Check for all null values
        if df[col].isnull().all():
            continue  # Keep as is if all values are null

        #如果有空行空列要删除，合并单元格的内容咋处理，再看看处理数据别人咋考虑的

        # Attempt to convert to numeric
        numeric_data = pd.to_numeric(df[col], errors='coerce')
        if not numeric_data.isnull().all():
            if (numeric_data % 1 == 0).all():
                df[col] = numeric_data.astype('Int64')  # Use Int64 to allow for NaN values
            else:
                df[col] = numeric_data
            continue

        # Attempt to convert to datetime
        if is_datetime(df[col]):
            df[col] = pd.to_datetime(df[col], errors='coerce')
            continue

        # Check for boolean data
        if is_boolean(df[col]):
            df[col] = df[col].map({'True': True, 'False': False, 'true': True, 'false': False}).astype('boolean')
            continue

        # Check if the column should be categorical
        if should_be_categorical(df[col]):
            df[col] = pd.Categorical(df[col])
            continue

        # If none of the above, keep as object (string) type
        df[col] = df[col].astype('object')

    return df

def is_datetime(series):
    # Check if the series contains datetime strings
    non_null = series.dropna()
    if len(non_null) == 0:
        return False
    
    try:
        pd.to_datetime(non_null, errors='raise', utc=True)
        return True
    except ValueError:
        return False

def is_boolean(series):
    # Check if the series contains only boolean-like values
    unique_values = series.dropna().unique()
    boolean_values = {'True', 'False', 'true', 'false', True, False}
    return set(unique_values).issubset(boolean_values)

def should_be_categorical(series):
    # Determine if a series should be treated as categorical
    unique_ratio = len(series.unique()) / len(series)
    return unique_ratio < 0.5 and len(series.unique()) <= 100  # Adjust thresholds as needed

def analyze_column_types(df):
    column_analysis = {}
    for col in df.columns:
        column_analysis[col] = {
            'inferred_type': str(df[col].dtype),
            'unique_values': int(len(df[col].unique())),
            'null_count': int(df[col].isnull().sum()),
            'sample_values': df[col].dropna().head(5).tolist()
        }
    return column_analysis