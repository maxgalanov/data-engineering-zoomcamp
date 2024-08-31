import pandas as pd
import re

def transform_data(input_file: str, output_file: str):
    if input_file.endswith(".csv.gz"):
        df = pd.read_csv(input_file, compression='gzip')
    elif input_file.endswith(".csv"):
        df = pd.read_csv(input_file)
    else:
        raise Exception("Only .csv files could be proccesed")
    
    df = df[(df['passenger_count'] > 0) & (df['trip_distance'] > 0)]

    df.loc[:, 'lpep_pickup_date'] = pd.to_datetime(df['lpep_pickup_datetime']).dt.date

    df.rename(columns=lambda x: re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', x).lower(), inplace=True)

    assert 'vendor_id' in df.columns, "Column 'vendor_id' does not exist in the DataFrame."
    assert (df['passenger_count'] > 0).all(), "There are entries with passenger_count <= 0."
    assert (df['trip_distance'] > 0).all(), "There are entries with trip_distance <= 0."

    print("All assertions passed successfully!")

    df.to_csv(output_file)

    print(f"Transformed data saved to {output_file}")

    return

    