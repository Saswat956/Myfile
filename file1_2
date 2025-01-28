

import base64
import hashlib
import json
import pandas as pd

# Path to the JSON file
data_file = r"/Users/saswatswain/Downloads/Oracle_datatypes_POC.json"

def fetch_attributes(properties, hier, element_dict, cntr):
    """Read data file, extract and build distinct hierarchies"""
    for idx, block in enumerate(properties):
        #cntr += 1
        col_desc={}
        if 'type' in block:
            cntr += 1
            data_type = block['type']

            # Handle document and jsonObject types
            if block['type'] in ['document', 'jsonObject']:
                attribute_hierarchy = hier + '.' + block['code'] if 'code' in block else hier + '.' + block.get('name', '')
                hash_str = hashlib.md5(attribute_hierarchy.encode('utf-8')).hexdigest()
                element_dict[hash_str] = ['Object', 'Parent', attribute_hierarchy, cntr, {}]

                if 'properties' in block:
                    fetch_attributes(block['properties'], attribute_hierarchy, element_dict, cntr)

            # Handle array and jsonArray types
            elif block['type'] in ['array', 'jsonArray']:
                attribute_hierarchy = hier + '.' + block['code'] if 'code' in block else hier + '.' + block.get('name', '')
                hash_str = hashlib.md5(attribute_hierarchy.encode('utf-8')).hexdigest()
                element_dict[hash_str] = ['Object', 'Parent', attribute_hierarchy, cntr, {}]

                if 'properties' in block:
                    fetch_attributes(block['properties'], attribute_hierarchy, element_dict, cntr)

            # Handle other types
            else:
                attribute_hierarchy = hier + '.' + block['code'] if 'code' in block else hier + '.' + block.get('name', '')
                hash_str = hashlib.md5(attribute_hierarchy.encode('utf-8')).hexdigest()

                # Construct column description with individual assignments
                #col_desc = {}
                col_desc['description'] = block['description'] if 'description' in block else ''
                col_desc['active_indicator'] = block['isActivated'] if 'isActivated' in block else ''
                col_desc['pii_indicator'] = block['piiIn'] if 'piiIn' in block else ''
                col_desc['primary_key_indicator'] = block['primaryKey'] if 'primaryKey' in block else ''
                col_desc['data_type_name'] = data_type
                col_desc['column_length_number'] = block['length'] if 'length' in block else ''

                element_dict[hash_str] = ['Column', 'Leaf', attribute_hierarchy, cntr, col_desc]
    return

def process_hackolade_data(inp_trnsfm, db_nm):
    """Read data file, extract and build distinct hierarchies from scanner output"""
    entity_dict = {}
    cntr = 0
    top_hier = db_nm

    # Create Database hierarchy
    hash_str = hashlib.md5(top_hier.encode('utf-8')).hexdigest()
    entity_dict[hash_str] = ['Database', 'Parent', top_hier, cntr, {}]

    # Create Schema hierarchy
    schema_nm = db_nm + '.' + 'EALDB'
    hash_str = hashlib.md5(schema_nm.encode('utf-8')).hexdigest()
    entity_dict[hash_str] = ['Schema', 'Parent', schema_nm, cntr, {}]

    # Process collections
    data = inp_trnsfm['collections']
    for idx, items in enumerate(data):
        json_data = items
        cntr += 1

        if 'collectionName' not in json_data:
            raise Exception("Collection is missing a 'collectionName'.")

        collection_name = json_data['collectionName']
        collection_hierarchy = schema_nm + '.' + collection_name
        hash_str = hashlib.md5(collection_hierarchy.encode('utf-8')).hexdigest()
        entity_dict[hash_str] = ['Table', 'Parent', collection_hierarchy, cntr, {}]

        if 'properties' not in json_data:
            raise Exception("Properties missing in Collection")

        fetch_attributes(json_data['properties'], collection_hierarchy, entity_dict, cntr)

    # Create sorted entity list
    entity_list = []
    for k, v in entity_dict.items():
        entity_list.append([k, v[0], v[1], v[2], v[4]])

    entity_list_sorted = sorted(entity_list, key=lambda x: x[3])
    return entity_list_sorted

if __name__ == "__main__":
    with open(data_file, mode='r') as file_in:
        inp_transform = json.load(file_in)
        attribute_list = process_hackolade_data(inp_transform, 'EALDB')

        for idx, node_list in enumerate(attribute_list):
            print(idx, node_list)

        df = pd.DataFrame(attribute_list)
        df.to_csv(r"out-ealdb-1.csv", index=False)
-----------------------------------------------------------------------------------------------------------------------------------
attribute_hierarchy = str(hier + '.' + block['code'])

attribute_hierarchy = hier + '.' + block['code'] if 'code' in block else hier + '.' + block.get('name', '')


if 'properties' not in json_data.keys():
    raise Exception("Properties missing in Collection")

if 'properties' not in json_data:
    raise Exception("Properties missing in Collection")

1.File path variable renamed and updated.
2.Simplified .keys() checks and refactored conditions in both functions.
3.Added fallback for missing code or name when constructing attribute_hierarchy.
4.Enhanced col_desc with additional fields (pii_indicator, primary_key_indicator, etc.).
5.Fixed the sorting key in entity_list_sorted.
6.Updated the hardcoded schema name in process_hackolade_data.
7.Updated the output CSV path and formatting.

