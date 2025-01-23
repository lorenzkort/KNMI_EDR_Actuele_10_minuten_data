import pandas as pd
import os
from src.logging_config import setup_logging
from src.column_metadata import column_mapping, column_renaming

logger = setup_logging(__name__)

def validate_and_rename_parquet(input_dir="./parquet_files/", output_dir="./parquet_files_validated/"):
    """
    Validates column datatypes and renames columns for all parquet files in input directory.
    
    Parameters:
        input_dir (str): Directory containing parquet files to validate
        output_dir (str): Directory to save validated parquet files
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Process each parquet file
    for filename in os.listdir(input_dir):
        if filename.endswith('.parquet'):
            logger.info(f"Processing file: {filename}")
            input_path = os.path.join(input_dir, filename)
            output_path = os.path.join(output_dir, filename)
            
            try:
                # Read parquet file
                df = pd.read_parquet(input_path)
                
                # Validate and convert datatypes
                for col in df.columns:
                    if col in column_mapping:
                        df[col] = df[col].astype(column_mapping[col])
                
                # Rename columns
                df = df.rename(columns=column_renaming)
                
                # Save validated and renamed DataFrame
                df.to_parquet(output_path, index=False)
                logger.info(f"Successfully validated and renamed {filename}")
                
            except Exception as e:
                logger.error(f"Error processing {filename}: {str(e)}")
                continue

def main():
    validate_and_rename_parquet()

if __name__ == "__main__":
    main()
