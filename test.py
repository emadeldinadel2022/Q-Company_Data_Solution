from ftp_server.ftp_client import *
import os

if __name__ == "__main__":
    # Connect to FTP and process files
    ftp = ftp_connect()
    merged_df = download_and_process_recent_files(ftp)
    ftp.quit()

    if not merged_df.empty:
        # Save as Parquet
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        parquet_file = os.path.join(os.getcwd(), f"merged_data_{timestamp}.parquet")
        save_as_parquet(merged_df, parquet_file)
    else:
        print("No new files found in the last hour.")
