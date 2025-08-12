import pandas as pd

def prepare_data(df, cols_to_convert) -> pd.DataFrame:

    for col in cols_to_convert:
        df[col] = pd.to_numeric(df[col], errors='coerce')  # 'coerce' will turn invalid parsing into NaN
        
        
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    # Keep the most recent recorords by resturant and respondent name
    df = df.sort_values(by=['Restaurant', 'Respondent Name', 'Timestamp'], ascending=[True, True, False])
    df = df.drop_duplicates(subset=['Restaurant', 'Respondent Name'], keep='first')
    # Reset index after dropping duplicates
    df = df.reset_index(drop=True)
    
    return df