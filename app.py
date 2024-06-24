import os
from data_generation_layer.models import FSOFactory
from data_extraction_layer.file_extraction import FileExtractor, merge
import pandas as pd

path = "dataset"

new_files = []
for root, dirs, files in os.walk(path):
    for f in files:
        new_files.append(FSOFactory.create_file(os.path.join(root, f)))

extracted_files = []
for f in new_files:
    extracted_files.append(FileExtractor(f))


for f in extracted_files:
    f.extract()

branches = []
for f in extracted_files:
    if "branch" in f.fso_object.name:
        branches.append(f.data.set_index('branch_id'))

merged_branches = merge(branches)
print(merged_branches)

sales_people = []
for f in extracted_files:
    if "sales_agent" in f.fso_object.name:
        sales_people.append(f.data.set_index('sales_person_id'))

merged_sales_people = merge(sales_people)
print(merged_sales_people)
#hire date in sales person is change

seles_transactions = []
for f in extracted_files:
    if "sales_transaction" in f.fso_object.name:
        seles_transactions.append(f.data.set_index('transaction_id'))

merged_seles_transactions = merge(seles_transactions)
print(merged_seles_transactions)