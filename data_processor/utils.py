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
        if is_numeric(df[col]):
            df[col] = pd.to_numeric(df[col], errors='coerce')
            if not df[col].isnull().all():
                if (df[col] % 1 == 0).all():
                    df[col] = df[col].astype('Int64')  # Use Int64 to allow for NaN values
                else:
                    df[col] = df[col]
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
        if is_categorical(df[col]):
            df[col] = pd.Categorical(df[col])
            continue

        # If none of the above, keep as object (string) type
        df[col] = df[col].astype('object')

    return df

def is_numeric(series, threshold=0.5):
    non_null = series.dropna()
    if len(non_null) == 0:
        return False
    valid_count = pd.to_numeric(non_null, errors='coerce').notna().mean()
    return valid_count > threshold


def is_datetime(series, threshold=0.5):
    non_null = series.dropna()
    if len(non_null) == 0:
        return False
    date_formats = [
        '%d/%m/%Y',
        '%Y-%m-%d',
        '%d-%m-%Y',
        '%m/%d/%Y',
        '%Y/%m/%d',
        '%b %d, %Y',
        '%B %d, %Y',
        '%d %b %Y',
        '%d %B %Y'
    ]
    for fmt in date_formats:
        valid_count = pd.to_datetime(non_null, format=fmt, errors='coerce').notna().mean()
        if valid_count > threshold:
            return True  
    return False

def is_boolean(series, threshold=0.5):
    non_null = series.dropna().astype(str).str.upper()
    bool_values = {'TRUE', 'FALSE', '1', '0', 'T', 'F', 'YES', 'NO'}
    return non_null.isin(bool_values).mean() > threshold

def is_categorical(series, max_unique=10, unique_ratio_threshold=0.8, tolerance=0.2):
    non_null = series.dropna()
    if len(non_null) == 0:
        return False

    if is_numeric(series) or is_datetime(series) or is_boolean(series):
        return False

    is_likely_categorical = non_null.astype(str).str.match(r'^[A-Za-z\s-]+$')
    categorical_ratio = is_likely_categorical.mean()

    if (categorical_ratio >= (1 - tolerance) and
            series.nunique(dropna=True) <= max_unique and
            (series.nunique(dropna=True) / len(series)) <= unique_ratio_threshold):
        return True
    return False

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