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

def show_preview(input_file, num_rows=5):
    """
    Show a preview of the first few rows of a CSV file.

    Args:
        input_file (csv): Input CSV file
        num_rows (int, optional): Number of rows to preview. Defaults to 5.
    """
    df = pd.read_csv(input_file, nrows=num_rows)
    print(df)

def find_sync_point(file_1, file_2, sync_column):
    """
    Find the sync point between two CSV files based on a common column.

    Args:
        file_1 (csv): First CSV file
        file_2 (csv): Second CSV file
        sync_column (string): Column to use for synchronization

    Returns:
        int: Index of the sync point in the first CSV file
    """
    # Read the sync value from the sync column of the second file
    sync_value = pd.read_csv(file_2, usecols=[sync_column]).iloc[0][sync_column]

    # Find the index of the sync value in the first file
    for chunk in pd.read_csv(file_1, chunksize=10000):
        for index, row in chunk.iterrows():
            if row[sync_column] == sync_value:
                return index
    # Return -1 if the sync value is not found
    print(f"Sync value {sync_value} not found in {file_1}")
    return -1

if __name__ == "__main__":
    # Example usage
    input_file = "/home/abhi2001/SRA/Data/Data/logCoppy/Subj1_May72024/X2_SRA_B_07-05-2024_10-41-46.csv"
    output_file = "/home/abhi2001/SRA/Dyadic_Model/data/X2_SRA_B_07-05-2024_10-41-46-mod.csv"
    columns_to_extract = [' TimeInteractionSubscription', ' JointPositions_1', ' JointPositions_2', ' JointPositions_3', ' JointPositions_4']
    # print(get_file_length(input_file))
    # extract_columns(input_file, output_file, columns_to_extract)
    # print(len(list_columns(input_file)))
    # show_preview(input_file)
    file_1 = "/home/abhi2001/SRA/Dyadic_Model/data/X2_SRA_A_07-05-2024_10-39-10-mod.csv"
    file_2 = "/home/abhi2001/SRA/Dyadic_Model/data/X2_SRA_B_07-05-2024_10-41-46-mod.csv"
    sync_column = ' TimeInteractionSubscription'
    print(find_sync_point(file_1, file_2, sync_column))