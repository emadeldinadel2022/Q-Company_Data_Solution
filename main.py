from ftp_server.ftp_client import download_ftp_files

if __name__ == "__main__":
    print("Starting FTP client script...")
    try:
        download_ftp_files()
        print("All files downloaded successfully.")
    except Exception as e:
        print("An error occurred:", str(e))