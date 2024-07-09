import io
import pandas as pd
from ftplib import FTP
from data_generation_layer.models import FSOFactory
from data_extraction_layer.parsers import ParserFactory
from data_extraction_layer.extraction import FileExtractor
from hdfs import InsecureClient
from datetime import datetime, timedelta
import tempfile
import os
import time

def ftp_connect(max_retries=5, retry_delay=2):
    print("Waiting for FTP server to start...")
    time.sleep(10)  
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1} to connect to FTP server...")
            ftp = FTP()
            ftp.connect('localhost', 2021)
            ftp.login('anonymous', '') 
            print("Connected to FTP server")
            return ftp
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Max retries reached. Unable to connect to FTP server.")
                raise

def is_file_recent(ftp: FTP, file_path: str, cutoff_time: datetime) -> bool:
    try:
        mdtm = ftp.sendcmd(f"MDTM {file_path}")[4:].strip()
        created_at = datetime.strptime(mdtm, "%Y%m%d%H%M%S")
        print(f"File: {file_path}, Created at: {created_at}, Cutoff: {cutoff_time}")
        return created_at > cutoff_time
    except Exception as e:
        print(f"Error checking file recency for {file_path}: {str(e)}")
        return False

def download_and_process_recent_files(ftp):
    groups = ["group1", "group2", "group3", "group4", "group5", "group6"]
    all_data = []
    cutoff_time = datetime.now() - timedelta(hours=5)

    for group in groups:
        try:
            group_path = f'/q_company_data/{group}'
            ftp.cwd(group_path)
            files = ftp.nlst()
            

            recent_files = [file for file in files if is_file_recent(ftp, group_path+'/'+file, cutoff_time)]
            recent_files.sort(key=lambda x: ftp.voidcmd(f"MDTM {x}"), reverse=True)
            recent_files = recent_files[:3]
            

            
            for file in recent_files:
                path = ftp.pwd() + "/" + file
                fso = FSOFactory.create_ftpfile(ftp, path)
                
                # Download file to a temporary location
                file_content = io.BytesIO()
                ftp.retrbinary(f"RETR {file}", file_content.write)
                file_content.seek(0)  # Reset file pointer to the beginning
                
                # Process the file
                parser = ParserFactory.get_parser(fso.type)
                data = parser.parse(file_content)
                
                # Add group information
                data["group"] = group
                
                print(data)
                all_data.append(data)
                
            
        except Exception as e:
            print(f"An error occurred: {str(e)} ", group)
        
        ftp.cwd("../..")


    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    
def save_as_parquet(df, file_path):
    df.to_parquet(file_path, index=False)
    print(f"Saved DataFrame to {file_path}")

