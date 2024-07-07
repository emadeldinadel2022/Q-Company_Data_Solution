from abc import ABC, abstractmethod
import pandas as pd

class FileParser(ABC):
    @abstractmethod
    def parse(self, file_path: str) -> pd.DataFrame:
        pass

class CSVParser(FileParser):
    def parse(self, file_path: str) -> pd.DataFrame:
        return pd.read_csv(file_path)
    
class JSONParser(FileParser):
    def parse(self, file_path: str) -> pd.DataFrame:
        return pd.read_json(file_path)

class ParserFactory:
    @staticmethod
    def get_parser(file_type: str) -> FileParser:
        if file_type == 'csv':
            return CSVParser()
        elif file_type == 'json':
            return JSONParser()
        else:
            raise ValueError(f"Unsupported file type: {file_type}")