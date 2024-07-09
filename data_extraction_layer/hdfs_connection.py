from hdfs import InsecureClient

# Configure the HDFS client
hdfs_host = "localhost"  # Use localhost since we're port forwarding
hdfs_port = "9879"  # The port mapped to 9870 in the container
hdfs_user = "itversity"  # The user inside the container

# Create the HDFS client
client = InsecureClient(f"http://{hdfs_host}:{hdfs_port}", user=hdfs_user)

# Test the connection
try:
    status = client.status('/')
    print("Successfully connected to HDFS")
    print("Root directory status:", status)
except Exception as e:
    print("Failed to connect to HDFS:", str(e))

# Example: List files in the root directory
for entry in client.list('/'):
    print(entry)

def put_file_to_hdfs(local_path, hdfs_path):
    """
    Upload a file to HDFS.

    :param local_path: Path to the local file to be uploaded.
    :param hdfs_path: Destination path in HDFS.
    """
    try:
        client.upload(hdfs_path, local_path)
        print(f"File {local_path} uploaded to {hdfs_path} successfully.")
    except Exception as e:
        print(f"Failed to upload {local_path} to HDFS: {str(e)}")

# Example usage
local_file = "dataset/group1/branches_SS_raw_1.csv"
hdfs_destination = "/data/branches.csv"
put_file_to_hdfs(local_file, hdfs_destination)
