from data_generation_layer.models import File, FSOFactory
from data_extraction_layer.parsers import ParserFactory
from datetime import datetime
import os
import re
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

def merge_dataFrames(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(dfs)

def get_group_number(group_name: str) -> int:
    return int(re.findall(r'\d+', group_name)[0])

def process_group(group_path: str):
    items = os.listdir(group_path)
    files = [item for item in items if os.path.isfile(os.path.join(group_path, item))]
    extracted_data = {}
    
    for file_name in files:
        file_path = os.path.join(group_path, file_name)
        fso = FSOFactory.create_file(file_path)
        extractor = FileExtractor(fso)
        extractor.extract()
        splited_file_name = file_name.split('.')[0]
        updated_file_name = splited_file_name.split('_')[0] if file_name.__contains__('branches') else '_'.join(splited_file_name.split('_')[0:2])
        extracted_data[updated_file_name] = extractor.data

    
    branches = extracted_data['branches']
    sales_agents = extracted_data['sales_agents']
    sales_transactions = extracted_data['sales_transactions']

    sales_agents.rename(columns={'sales_person_id': 'sales_agent_id'}, inplace=True)

    merged_df = pd.merge(sales_transactions, sales_agents, on='sales_agent_id', how='left')
    merged_df = pd.merge(merged_df, branches, on='branch_id', how='left')
    merged_df['group'] = get_group_number(os.path.basename(group_path))

    return merged_df
