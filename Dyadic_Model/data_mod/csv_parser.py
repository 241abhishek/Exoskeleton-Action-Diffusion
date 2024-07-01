"""
Utility functions for parsing csv files.
"""

import csv
import pandas as pd 
from tqdm import tqdm

def extract_columns(input_file, output_file, columns_to_extract, chunksize=10000):
    """
    Extract specific columns from a CSV file and write them to a new CSV file.

    Args:
        input_file (csv): Input CSV file
        output_file (csv): Output CSV file
        columns_to_extract (list[strings]): List of column names to extract
        chunksize (int, optional): Size of data that loads at once. Defaults to 10000.
    """
    # Open the output CSV file
    with open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        
        # Write the header
        writer.writerow(columns_to_extract)
        
        # Get the total number of rows
        total_rows = get_file_length(input_file)
        
        # Use tqdm to create a progress bar
        with tqdm(total=total_rows, desc="Extracting columns") as pbar:
            # Read and process the CSV file in chunks
            for chunk in pd.read_csv(input_file, chunksize=chunksize, usecols=columns_to_extract):
                for _, row in chunk.iterrows():
                    # Extract the specified columns
                    extracted_row = [row[col] for col in columns_to_extract]
                    writer.writerow(extracted_row)
                    pbar.update(1)

def get_file_length(input_file) -> int:
    """
    Get the number of rows in a CSV file.

    Args:
        input_file (csv): Input CSV file

    Returns:
        integer: Number of rows in the CSV file
    """
    return sum(1 for _ in open(input_file)) - 1  # Subtract 1 for the header

def list_columns(input_file):
    """
    List the columns in a CSV file.

    Args:
        input_file (csv): Input CSV file

    Returns:
        list[strings]: List of column names
    """
    df = pd.read_csv(input_file, nrows=0)
    return df.columns.tolist()

if __name__ == "__main__":
    # Example usage
    input_file = "/home/abhi2001/SRA/Data/Data/logCoppy/Subj1_May72024/X2_SRA_A_07-05-2024_10-39-10-005.csv"
    output_file = "/home/abhi2001/SRA/Dyadic_Model/data/X2_SRA_A_07-05-2024_10-39-10-mod.csv"
    columns_to_extract = [' JointPositions_1', ' JointPositions_2', ' JointPositions_3', ' JointPositions_4']
    # print(get_file_length(input_file))
    extract_columns(input_file, output_file, columns_to_extract)
    # print(list_columns(input_file))
    # print(list_columns(input_file))