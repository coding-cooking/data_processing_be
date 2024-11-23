import numpy as np
import pandas as pd
from dateutil.parser import parse

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
            result_df[col] = series.apply(parse_date)

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
    cleaned_series = non_null.astype(str).str.replace(',', '', regex=True)
    valid_count = pd.to_numeric(cleaned_series, errors='coerce').notna().mean()
    return valid_count > threshold


def is_datetime(series, threshold=0.5):
    non_null = series.dropna().astype(str).str.strip()
    if len(non_null) == 0:
        return False

    parsed_dates = non_null.apply(parse_date)
    valid_count = parsed_dates.notna().mean()

    return valid_count >= threshold

def is_boolean(series, threshold=0.5):
    non_null = series.dropna().astype(str).str.upper()
    bool_values = {'TRUE', 'FALSE', '1', '0', 'T', 'F', 'YES', 'NO'}
    return non_null.isin(bool_values).mean() > threshold

def is_categorical(series, max_unique=10, unique_ratio_threshold=0.8, tolerance=0.2):
    if is_numeric(series) or is_datetime(series) or is_boolean(series):
        return False
    non_null = series.dropna()
    if len(non_null) == 0:
        return False
    is_likely_categorical = non_null.astype(str).str.match(r'^[A-Za-z\s-]+$')
    categorical_ratio = is_likely_categorical.mean()

    if (categorical_ratio >= (1 - tolerance) and
            series.nunique(dropna=True) <= max_unique and
            (series.nunique(dropna=True) / len(series)) <= unique_ratio_threshold):
        return True
    return False

def is_complex(series, threshold=0.5):
    def check_complex(x):
        return isinstance(x, complex) or (
                isinstance(x, str) and
                any(c in x for c in ['+', '-']) and
                'j' in x
        )
    try:
        non_null_series = series.dropna()
        if len(non_null_series) == 0:
            return False
        count_complex = sum(check_complex(x) for x in non_null_series)
        return count_complex / len(non_null_series) >= threshold
    except AttributeError:
        return False

def parse_date(val):
    try:
        # Reject complex numbers, booleans
        if isinstance(val, (bool, complex)):
            return pd.NaT

        # Handle pure numeric strings
        val_str = str(val).strip()
        if val_str.isdigit():
            if len(val_str) == 8:
                try:
                    parsed_date = pd.to_datetime(val_str, format='%Y%m%d')
                    if parsed_date.year >= 1900 and parsed_date.year <= 2100:
                        return parsed_date
                    else:
                        return pd.to_datetime(val_str, unit='s')
                except Exception:
                    return pd.to_datetime(val_str, unit='s')
            else:
                num = int(val_str)
                if 0 <= num <= 2147483647:
                    return pd.to_datetime(val_str, unit='s')
                elif 0 <= num <= 2147483647000:
                    return pd.to_datetime(val_str, unit='ms')

        # Use dateutil for general parsing
        parsed = parse(str(val), fuzzy=False)

        # Additional validation: year should be within reasonable range
        if parsed.year < 1000 or parsed.year > 2100:
            return pd.NaT

        return parsed

    except (ValueError, OverflowError, TypeError):
        return pd.NaT

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