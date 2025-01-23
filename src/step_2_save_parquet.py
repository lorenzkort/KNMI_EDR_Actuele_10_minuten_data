import pandas as pd
import xarray as xr
import numpy as np
import os
from src.logging_config import setup_logging
import pyarrow

logger = setup_logging(__name__)

def convert_nc_to_df(nc_file_path):
    """
    Converts a .nc file to a Pandas DataFrame, auto-determining column names based on variables.
    
    Parameters:
        nc_file_path (str): Path to the .nc file.

    Returns:
        pd.DataFrame: DataFrame containing all variables as columns.
    """
    # Load the .nc file using xarray
    ds = xr.open_dataset(nc_file_path, engine="netcdf4")
    
    # Convert all data variables to a Pandas DataFrame
    df = ds.to_dataframe().reset_index()

    # Ensure column names are descriptive
    df.columns = [col if col else f"unnamed_{i}" for i, col in enumerate(df.columns)]

    # Handle binary string fields by decoding them or setting to empty string
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)
            # Replace empty strings with None
            df[col] = df[col].replace('', None)

    # Close the dataset
    ds.close()

    return df

def main(input_dir="./dataset-download/", output_dir="./parquet_files/"):
    """
    Converts all .nc files in input directory to parquet files in output directory.
    
    Parameters:
        input_dir (str): Directory containing .nc files
        output_dir (str): Directory where parquet files will be saved
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Process each .nc file in the input directory
    for filename in os.listdir(input_dir):
        if filename.endswith('.nc'):
            logger.info(f"Processing file: {filename}")
            
            # Construct full file paths
            input_path = os.path.join(input_dir, filename)
            output_path = os.path.join(output_dir, filename.replace('.nc', '.parquet'))
            
            try:
                # Convert nc to DataFrame
                df = convert_nc_to_df(input_path)
                
                # Save as parquet using pyarrow engine
                df.to_parquet(output_path, index=False, engine='pyarrow')
                logger.info(f"Successfully converted {filename} to parquet")
                
            except Exception as e:
                logger.error(f"Error processing {filename}: {str(e)}")
                continue
