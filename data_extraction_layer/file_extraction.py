from data_generation_layer.models import File
from data_extraction_layer.parsers import ParserFactory
from datetime import datetime
import pandas as pd
from typing import Callable, Optional


class FileExtractor:
    def __init__(self, file_object: File):
        self.__file_object = file_object
        self.__extraction_timestamp = None
        self.parser = ParserFactory.get_parser(self.fso_object.type)
        self.data = None

    @property
    def fso_object(self) -> File:
        return self.__file_object

    @property
    def extraction_timestamp(self) -> Optional[datetime]:
        return self.__extraction_timestamp

    def set_extraction_timestamp(self, timestamp: datetime) -> None:
        self.__extraction_timestamp = timestamp

    def extract(self, extraction_strategy: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None) -> None:
        data = self.parser.parse(self.fso_object.path)

        if extraction_strategy is None:
            self.data = data
        else:
            self.data = extraction_strategy(data)
        
        self.set_extraction_timestamp(datetime.now())

    @staticmethod
    def incremental_extraction(data: pd.DataFrame) -> pd.DataFrame:
        pass

def merge(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(dfs).query('~index.duplicated()')


