from ftp_server.ftp_client import *
import os

if __name__ == "__main__":
    # Connect to FTP and process files
    ftp = ftp_connect()
    merged_df, group = download_and_process_recent_files(ftp)
    ftp.quit()

    if not merged_df.empty:
        # Save as Parquet
        hadoop_container_path = os.getcwd() + "/qcomp_ecosystem/data/staging_area"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{group}_merged_sales_transactions_{timestamp}.csv"
        parquet_file = os.path.join(hadoop_container_path, file_name)
        save_as_csv(merged_df, parquet_file)
        print(f"Saved CSV file: {file_name}")

    else:
        print("No new files found in the last hour.")
