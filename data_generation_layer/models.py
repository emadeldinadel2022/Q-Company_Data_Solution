from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import os
import stat
import pytz
import time
from ftplib import FTP

class FileSystemObject(ABC):
    def __init__(self, name: str, path: str, created_at: datetime, owner: str, 
                 group: str, permissions: str, size: int) -> None:
        self.__name = name
        self.__path = path
        self.__created_at = created_at
        self.__owner = owner
        self.__group = group
        self.__permissions = permissions
        self.__size = size
    
    @abstractmethod
    def info(self):
        pass

    @property
    def name(self) -> str:
        return self.__name

    @property
    def path(self) -> str:
        return self.__path

    @property
    def created_at(self) -> datetime:
        return self.__created_at

    @property
    def owner(self) -> str:
        return self.__owner

    @property
    def group(self) -> str:
        return self.__group
    
    @property
    def permissions(self) -> str:
        return self.__permissions
    
    @property
    def size(self) -> int:
        return self.__size

class File(FileSystemObject):
    def __init__(self, name: str, path: str, created_at: datetime, owner: str, group: str, 
                 business_group: str, permissions: str, size: int, type: str):
        super().__init__(name, path, created_at, owner, group, permissions, size)
        self.__business_group = business_group
        self.__type = type
    
    @property
    def business_group(self) -> str:
        return self.__business_group
    @property
    def type(self) -> str:
        return self.__type
    
    def info(self):
        info = (
            f"File Name: {self.name}\n"
            f"File Absolute Path: {self.path}\n"
            f"File Created At: {self.created_at}\n"
            f"File Owner: {self.owner}\n"
            f"File Group: {self.group}\n"
            f"File Business Group: {self.business_group}\n"
            f"File Permissions: {self.permissions}\n"
            f"File Size: {self.size}\n"
            f"File Type: {self.type}"
        )

        return info

class Directory(FileSystemObject):
    def __init__(self, name: str, path: str, created_at: datetime, owner: str, group: str, 
                 permissions: str, size: int):
        super().__init__(name, path, created_at, owner, group, permissions, size)

    def info(self):
        info = (
            f"Directory Name: {self.name}\n"
            f"Directory Absolute Path: {self.absloute_path}\n"
            f"Directory Created At: {self.created_at}\n"
            f"Directory Owner: {self.owner}\n"
            f"Directory Group: {self.group}\n"
            f"Directory Permissions: {self.permissions}\n"
            f"Directory Size: {self.size}\n"
        )
        return info 
class FTPFile(File):
    def __init__(self, name, path, created_at, owner, group, business_group, permissions, size, type):
        super().__init__(name, path, created_at, owner, group, business_group, permissions, size, type)

class FTPDirectory(Directory):
    def __init__(self, name, path, created_at, owner, group, permissions, size):
        super().__init__(name, path, created_at, owner, group, permissions, size)
 
class FSOFactory:
    @classmethod
    def create_file(cls, path: str) -> FileSystemObject:
        stats = os.stat(path)
        return File(
            name=cls._get_file_name(path),
            path=path,
            created_at=datetime.fromtimestamp(stats.st_ctime),
            owner=cls._get_owner(stats),
            group=cls._get_group(stats),
            business_group=cls._get_business_group(path),
            permissions=cls._get_permissions(stats),
            size=stats.st_size,
            type=cls._get_file_type(path)
        )
    
    @classmethod
    def create_dir(cls, path: str) -> FileSystemObject:
        stats = os.stat(path)
        return Directory(
            name=os.path.basename(path),
            path=path,
            created_at=datetime.fromtimestamp(stats.st_ctime),
            owner=cls._get_owner(stats),
            group=cls._get_group(stats),
            permissions=cls._get_permissions(stats),
            size=stats.st_size
        )
    
    @classmethod
    def create_ftpfile(cls, ftp: FTP, path: str) -> FileSystemObject:
        try:
            ftp.voidcmd('TYPE I')
            # Get file size
            size = ftp.size(path)
            # Get modified time
            
            mdtm = ftp.sendcmd(f"MDTM {path}")[4:].strip()
            utc_time = datetime.strptime(mdtm, "%Y%m%d%H%M%S")
            utc_time = utc_time.replace(tzinfo=pytz.UTC)  # Make the datetime object timezone-aware

            local_tz = datetime.now(pytz.utc).astimezone().tzinfo

            created_at = utc_time.astimezone(local_tz)

            
            return File(
                name=cls._get_file_name(path),
                path=path,
                created_at=created_at,
                owner='ftpuser',  # FTP server owner might not be retrievable
                group='ftpgroup',  # FTP server group might not be retrievable
                business_group=cls._get_business_group(path),
                permissions='-rw-r--r--',  # Default permissions for simplicity
                size=size,
                type=cls._get_file_type(path)
            )
        except Exception as e:
            print(f"Error creating file object: {e}")
            return None
    
    @classmethod
    def create_ftpdir(cls, ftp: FTP, path: str) -> FileSystemObject:
        try:
            created_at = datetime.now()  
            size = 0  

            return Directory(
                name=os.path.basename(path),
                path=path,
                created_at=created_at,
                owner='ftpuser',  
                group='ftpgroup',  
                permissions='drwxr-xr-x',  
                size=size
            )
        except Exception as e:
            print(f"Error creating directory object: {e}")
            return None

    
    @classmethod
    def _get_business_group(cls, path: str) -> str:
        return path.strip().split("/")[1]

    @classmethod
    def _get_file_name(cls, path: str) -> str:
        return os.path.basename(path)

    @classmethod
    def _get_owner(cls, stats) -> str:
        try:
            import pwd
            return pwd.getpwuid(stats.st_uid).pw_name
        except ImportError:
            return str(stats.st_uid)

    @classmethod
    def _get_group(cls, stats) -> str:
        try:
            import grp
            return grp.getgrgid(stats.st_gid).gr_name
        except ImportError:
            return str(stats.st_gid)

    @classmethod
    def _get_permissions(cls, stats) -> str:
        return stat.filemode(stats.st_mode)

    @classmethod
    def _get_file_type(cls, path: str) -> str:
        _, extension = os.path.splitext(path)
        return extension[1:] if extension else "unknown"