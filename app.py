import os
from data_generation_layer.models import FSOFactory
from data_extraction_layer.extraction import FileExtractor, process_group
import pandas as pd
import re

path = "dataset"

all_groups = []

for root, dirs, files in os.walk(path):
    for d in dirs:
        group_path = os.path.join(root, d)
        group_df = process_group(group_path)
            
        group_df.to_csv(f"./dataset/{d}_merged.csv", index=False)

        all_groups.append(group_df)

all_groups_df = pd.concat(all_groups, ignore_index=True)
    
all_groups_df.to_csv("all_groups_merged.csv", index=False)


# fs_objects = []
# extracted_files = []

# for root, dirs, files in os.walk(path):
#     for f in files:
#         fso = FSOFactory.create_file(os.path.join(root, f))
#         fs_objects.append(fso)
#         extracted_file = FileExtractor(fso)
#         extracted_file.extract()
#         extracted_file.data['group'] = get_group_number(fso.business_group)
#         extracted_files.append(extracted_file)



# branches = []
# for f in extracted_files:
#     if "branch" in f.fso_object.name:
#         branches.append(f.data.set_index('branch_id'))

# merged_branches = merge(branches)

# sales_people = []
# for f in extracted_files:
#     if "sales_agent" in f.fso_object.name:
#         sales_people.append(f.data.set_index('sales_person_id'))

# merged_sales_people = merge(sales_people)
#hire date in sales person is change, ignore the hire date

# seles_transactions = []
# for f in extracted_files:
#     if "sales_transaction" in f.fso_object.name:
#         print(f.fso_object.name)
#         seles_transactions.append(f.data)


# merged_seles_transactions = merge(seles_transactions)
# print(merged_seles_transactions)
#merged_seles_transactions.to_csv("./dataset/trans.csv")

# merged_branches.reset_index(inplace=True)
# merged_sales_people.reset_index(inplace=True)
# print(merged_sales_people)
# merged_sales_people.rename({'sales_person_id': 'sales_agent_id'}, axis=1, inplace=True)
# print(merged_seles_transactions.columns)
# merged_data = merged_seles_transactions.merge(merged_sales_people, on='sales_agent_id', how='left').merge(merged_branches, on='branch_id', how='left')
# merged_data.to_csv("./dataset/merged_data.csv", index=False)