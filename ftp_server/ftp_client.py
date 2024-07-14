import io
import pandas as pd
from ftplib import FTP
from data_generation_layer.models import FSOFactory
from data_extraction_layer.parsers import ParserFactory
from datetime import datetime, timedelta
import time
import pytz

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

def get_file_timestamp(ftp: FTP, file_path: str) -> datetime:
    mdtm = ftp.sendcmd(f"MDTM {file_path}")[4:].strip()
    utc_time = datetime.strptime(mdtm, "%Y%m%d%H%M%S").replace(tzinfo=pytz.UTC)
    local_tz = datetime.now(pytz.utc).astimezone().tzinfo
    return utc_time.astimezone(local_tz)

def is_file_recent(timestamp: datetime, cutoff_time: datetime) -> bool:
    print(timestamp, cutoff_time)
    return timestamp > cutoff_time

def get_recent_files(ftp: FTP, group: str, cutoff_time: datetime):
    required_files = ['branches', 'sales_agents', 'sales_transactions']
    group_path = f'/q_company_data/{group}'
    ftp.cwd(group_path)
    files = ftp.nlst()
    
    recent_files = {}
    for file in files:
        file_type = file.split('.')[-1]
        for required_file in required_files:
            if required_file in file:
                timestamp = get_file_timestamp(ftp, file)
                if is_file_recent(timestamp, cutoff_time):
                    recent_files[required_file+"."+file_type] = (file, timestamp)
                break
    
    return recent_files if len(recent_files) == 3 else None

def process_group(ftp: FTP, group: str, cutoff_time: datetime):
    recent_files = get_recent_files(ftp, group, cutoff_time)
    if not recent_files:
        print(f"Skipping group {group}: Not all required recent files found")
        return None, None

    extracted_data = {}
    for file_type, (file, _) in recent_files.items():
        file_content = io.BytesIO()
        ftp.retrbinary(f"RETR {file}", file_content.write)
        file_content.seek(0)

        ftype = file_type.split('.')[1]
        fname = file_type.split('.')[0]
        parser = ParserFactory.get_parser(ftype)
        extracted_data[fname] = parser.parse(file_content)

    branches = extracted_data['branches']
    sales_agents = extracted_data['sales_agents']
    sales_transactions = extracted_data['sales_transactions']

    sales_agents.rename(columns={'sales_person_id': 'sales_agent_id'}, inplace=True)
    merged_df = pd.merge(sales_transactions, sales_agents, on='sales_agent_id', how='left')
    merged_df = pd.merge(merged_df, branches, on='branch_id', how='left')
    merged_df.rename(columns={'cusomter_lname': 'customer_lname', 'cusomter_email': 'customer_email'}, inplace=True)
    merged_df['group'] = group

    return merged_df, group

def download_and_process_recent_files(ftp: FTP, group: str):
    cutoff_time = datetime.now(pytz.utc).astimezone() - timedelta(hours=1)
    return process_group(ftp, group, cutoff_time)

def save_as_csv(df: pd.DataFrame, file_path: str) -> None:
    df.to_csv(file_path, index=False)
    print(f"Saved DataFrame to {file_path}")

def find_and_process_group(ftp: FTP):
    groups = ["group1", "group2", "group3", "group4", "group5", "group6"]
    cutoff_time = datetime.now(pytz.utc).astimezone() - timedelta(hours=1)

    for group in groups:
        print(f"Checking group: {group}")
        merged_df, processed_group = process_group(ftp, group, cutoff_time)
        
        if merged_df is not None and not merged_df.empty:
            print(f"Successfully processed group: {processed_group}")
            return merged_df, processed_group

    return None, None
