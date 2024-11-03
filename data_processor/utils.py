import numpy as np
import pandas as pd

def infer_and_convert_data_types(df):
    result_df = df.copy()
    for col in df.columns:
        series = df[col]

        if is_boolean(series):
            cleaned_series = series.astype(str).str.strip().str.upper()
            mapping = {
                'TRUE': True,
                'FALSE': False,
                '1': True,
                '0': False,
                'T': True,
                'F': False,
                'YES': True,
                'NO': False
            }
            result_df[col] = cleaned_series.map(mapping).astype('boolean')

        elif is_complex(series):
            cleaned_series = (series.astype(str)
                                  .str.strip()
                                  .str.lower()
                                  .str.replace('i', 'j', regex=False)) 
            result_df[col] = cleaned_series.astype('string')

        elif is_numeric(series):
            clean_series = (series.astype(str)
                    .str.strip()
                    .str.replace(r'(?<!^)[^\d.-]', '', regex=True)  
                    .str.replace(r'(?<!^)-', '', regex=True) 
                    .str.replace(r'\.(?=.*\.)', '', regex=True))
            result_df[col] = pd.to_numeric(clean_series, errors='coerce')
            if result_df[col].dropna().mod(1).eq(0).all():
                result_df[col] = result_df[col].astype('Int64')
            else:
                result_df[col] = result_df[col].astype('float64')

        elif is_datetime(series):
            # for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']:
            for fmt in ['%d/%m/%Y','%Y-%m-%d', '%d-%m-%Y','%m/%d/%Y','%Y/%m/%d','%b %d, %Y','%B %d, %Y','%d %b %Y','%d %B %Y']:
                try:
                    result_df[col] = pd.to_datetime(series, format=fmt, errors='coerce')
                    if not result_df[col].isna().all():
                        break
                except:
                    continue

        elif is_categorical(series):
            cleaned_series = (series.astype(str)
                                  .str.strip()
                                  .str.upper())
            result_df[col] = cleaned_series.astype('category')

        else:
            result_df[col] = series.astype('object')

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

def is_complex(series, threshold=0.5):
    try:
        non_null_series = series.dropna()
        count_complex = (
            (non_null_series.apply(lambda x: isinstance(x, complex) or (isinstance(x, str) and 'j' in x)))
        ).sum()
        if(len(non_null_series) > 0):
            return count_complex / len(non_null_series) > threshold
        else:
            return False
    except AttributeError:
        return False

def analyze_column_types(df):
    column_analysis = {}
    type_mapping = {
        'object': 'Text',
        'boolean': 'Boolean',
        'Int64': 'Integer',
        'float64': 'Float',
        'datetime64[ns]': 'Date',
        'category': 'Category',
        'complex': 'Complex'
    }
    for col in df.columns:
        inferred_type = str(df[col].dtype)
        if is_complex(df[col]):
            inferred_type = 'complex'
        column_analysis[col] = {
            'be_type': str(df[col].dtype),
            'fe_type': type_mapping.get(inferred_type, 'Unknown'),
            'unique_values': int(len(df[col].unique())),
            'null_count': int(df[col].isnull().sum()),
            'sample_values': df[col].dropna().head(5).tolist()
        }
    return column_analysis

def process_large_csv(file, chunksize=10000):
    processed_chunks = []
    for chunk in pd.read_csv(file, chunksize=chunksize, skip_blank_lines=True):
        processed_chunk = infer_and_convert_data_types(chunk)
        processed_chunks.append(processed_chunk)
    return pd.concat(processed_chunks, ignore_index=True)