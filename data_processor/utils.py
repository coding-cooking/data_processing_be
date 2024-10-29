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
            return obj.isoformat() 
        elif isinstance(obj, pd.Timedelta):
            return str(obj)  
        return super(NumpyEncoder, self).default(obj)

def infer_and_convert_data_types(df):
    result_df = df.copy()
    for col in df.columns:
        series = df[col]

        if is_boolean(series):
            result_df[col] = series.map({'TRUE': True, 'FALSE': False,
                                    '1': True, '0': False,
                                    'T': True, 'F': False,
                                    'YES': True, 'NO': False}).astype('boolean')

        elif is_complex(series):
            result_df[col] = series.apply(lambda x: complex(x) if isinstance(x, str)
                                                                  and 'j' in x else x).astype('string')
            
        elif is_numeric(series):
            clean_series = (series.astype(str)
                    .str.replace(r'(?<=[0-9])[^0-9]', '', regex=True)
                    .str.strip())
            result_df[col] = pd.to_numeric(clean_series, errors='coerce')
            if result_df[col].dropna().mod(1).eq(0).all():
                result_df[col] = result_df[col].astype('Int64')

        elif is_datetime(series):
            for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']:
                try:
                    result_df[col] = pd.to_datetime(series, format=fmt, errors='coerce')
                    if not result_df[col].isna().all():
                        break
                except:
                    continue

        elif is_categorical(series):
            result_df[col] = series.astype('category')

        else:
            result_df[col] = series.astype('object')
    result_df = result_df.select_dtypes(exclude=['complex'])
    return result_df

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

def is_complex(series):
    try:
        return series.dropna().apply(lambda x: isinstance(x, complex) or
                                               (isinstance(x, str) and 'j' in x)).mean() > 0.5
    except:
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