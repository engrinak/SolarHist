import os
import ftplib
import pandas as pd
import glob
import seaborn as sns
import matplotlib.pyplot as plt


def filter_partial_strings(larger_strings, partial_strings):
    filtered_strings = [
        string for string in larger_strings if not any(partial in string for partial in partial_strings)
    ]
    return filtered_strings

def read_txt_files_to_dataframe(directory):
    # Get a list of all .txt files in the directory
    txt_files = glob.glob(f"{directory}/*.txt")

    # Initialize an empty list to store individual DataFrames
    dataframes = []

    #remove 1994, 1995 and 1996 for using a different date format
    txt_files = filter_partial_strings(txt_files, ["1994", "1995", "1996"])
    
    new_formats = ["2022Q4", "2023Q1", "2023Q2", "2023Q3", "2023Q4" "2024Q1", "2024Q2"]

    # Read each .txt file into a DataFrame and append it to the list
    for file in txt_files:

        # In Q4 of 2022, they started using a decimal format, changing the col widths
        if any(item in file for item in new_formats):
            width = [10, 6, 3, 2, 2, 2, 2, 2, 2, 2,
                         6, 3, 2, 2, 2, 2, 2, 2, 2,
                         6, 7, 6, 6, 6, 6, 6, 6, 6,]
        else:
            width = [10, 6, 3, 2, 2, 2, 2, 2, 2, 2,
                         6, 3, 2, 2, 2, 2, 2, 2, 2,
                         6, 3, 2, 2, 2, 2, 2, 2, 2,]

        # Read in the data, using fixed width format
        df = pd.read_fwf(file, skiprows=12, header=None, widths=width, na_values=['-1', '*'])

        dataframes.append(df)

    # Concatenate all DataFrames into a single DataFrame
    combined_df = pd.concat(dataframes, ignore_index=True)

    return combined_df

def generate_heatmap(data, value_column, date_column, raw_date_col):
    sns.set(rc={'axes.facecolor':'lightblue', 'figure.facecolor':'dimgrey', 'axes.titlecolor':'white', 
                'axes.labelcolor':'white', 'xtick.labelcolor':'white', 'ytick.labelcolor':'white'})
    # Convert the date column to datetime format
    data[date_column] = pd.to_datetime(data[raw_date_col])

    # Extract the day of the year and year from the date
    data['day_of_year'] = data[date_column].dt.dayofyear
    data['year'] = data[date_column].dt.year

    # Pivot the data to create a heatmap-friendly format
    heatmap_data = data.pivot_table(index='year', columns='day_of_year', values=value_column, fill_value=0)

    # Create the heatmap
    plt.figure(figsize=(18, 6))
    sns.heatmap(heatmap_data, cmap='rocket_r', linewidths=0)

    # Customize the plot
    plt.title(f"Heatmap of Planetary A Index (High Latitude) for each day of the year")
    plt.xlabel("Day of the year")
    plt.ylabel("Year")

    # Show the plot
    plt.show()


def ftp_downloader(ftp_url, remote_directory, local_directory, file_prefix):

    ftp_url = ftp_url
    remote_directory = remote_directory
    local_directory = local_directory
    file_prefix = file_prefix

    # Ensure the local directory exists
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    # Connect to the FTP server
    ftp = ftplib.FTP(ftp_url)
    ftp.login()

    # Change to the remote directory
    ftp.cwd(remote_directory)

    # Set the FTP connection to binary mode - much faster
    ftp.sendcmd("TYPE I")

    # Get the list of files and sizes in the remote directory
    remote_files = []
    ftp.dir(lambda x: remote_files.append(x))

    # Download the files with "DGD" in the file name
    for remote_file_info in remote_files:
        remote_file_parts = remote_file_info.split()
        remote_file_size = int(remote_file_parts[4])
        remote_file_name = remote_file_parts[-1]

        if file_prefix in remote_file_name:
            local_file = os.path.join(local_directory, remote_file_name)

            # Check if the file exists locally
            if os.path.exists(local_file):
                # Get the local file's size
                local_file_size = os.path.getsize(local_file)

                # If the file sizes match, skip this file
                if remote_file_size == local_file_size:
                    print(f"{remote_file_name} already exists and has the same size. Skipping.")
                    continue

            # Download the remote file
            with open(local_file, 'wb') as f:
                print(f"Downloading {remote_file_name}...")
                ftp.retrbinary(f"RETR {remote_file_name}", f.write)
            print(f"{remote_file_name} downloaded successfully.")

    # Close the FTP connection
    ftp.quit()
    print("Synchronization complete.")

    return


if __name__ == "__main__":

    # Configurations
    ftp_url = "ftp.swpc.noaa.gov"
    remote_directory = "/pub/indices/old_indices/"
    # Directory containing the .txt files
    local_directory = "./kpdata/"
    file_prefix = "DGD"

    # Download any new files to the local directory
    ftp_downloader(ftp_url, remote_directory, local_directory, file_prefix)

    # Call the read_txt_files_to_dataframe function
    combined_df = read_txt_files_to_dataframe(local_directory)

    # Print the combined DataFrame
    print(combined_df)

    # Call the generate_heatmap function
    generate_heatmap(combined_df, value_column=10, date_column='date', raw_date_col=0)

    # Find dates of high activity.
    hotdates = combined_df.loc[combined_df[10] > 120]['date']
    
    # Read the data from AOGCC
    wells_df = pd.read_excel('Copy of wells.xlsx')
    wells_df = wells_df[['Api', 'Permit', 'WellName', 'OperatorName', 'SpudDate', 'CompletionDate']]
    wells_df['SpudDate_dt'] = pd.to_datetime(wells_df['SpudDate'])
    wells_df['CompletionDate_dt'] = pd.to_datetime(wells_df['CompletionDate'])

    # Compare dates of high activity to the spud date and completion date.
    # If high activity date is between spud and completion, then add to the output list.
    dataframes = []
    for date in hotdates:
        df = wells_df.loc[(wells_df['SpudDate_dt'] > date) & (wells_df['CompletionDate_dt'] < date)]
        dataframes.append(df)

    hotwells_df = pd.concat(dataframes, ignore_index=True)

    print(hotwells_df)