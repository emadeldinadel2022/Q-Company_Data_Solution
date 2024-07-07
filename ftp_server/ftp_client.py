import time
from ftplib import FTP
from data_generation_layer.models import FSOFactory

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

def download_ftp_files():
    ftp = ftp_connect()
    
    # Change to the directory containing the files
    groups = ["group1", "group2", "group3", "group4", "group5", "group6"]

    fso_files = []
    for group in groups:
        try:   
            ftp.cwd(f'q_company_data/{group}/')
            files = ftp.nlst()
            print("downloading files from ", ftp.pwd())
            for file in files:
                path = ftp.pwd() + "/" + file
                print("path "+path)
                fso_files.append(FSOFactory.create_ftpfile(ftp, path))

            ftp.cwd("../..")
        except Exception as e:
            print(f"An error occurred: {str(e)} ",group)


    for fso in fso_files:
        print("fso: "+fso.path, fso.name, fso.size, fso.created_at)    
    print(len(fso_files))
    
    
    ftp.quit()

