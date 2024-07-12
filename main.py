from ftp_server.ftp_client import *
import os

if __name__ == "__main__":
    ftp = ftp_connect()
    
    merged_df, processed_group = find_and_process_group(ftp)
    ftp.quit()

    if merged_df is not None and not merged_df.empty:
        hadoop_container_path = os.getcwd() + "/qcomp_ecosystem/data/staging_area"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{processed_group}_merged_sales_transactions_{timestamp}.csv"
        parquet_file = os.path.join(hadoop_container_path, file_name)
        save_as_csv(merged_df, parquet_file)
        print(f"Saved CSV file: {file_name}")
    else:
        print("No groups with complete recent file sets found.")
