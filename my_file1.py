import base64
import hashlib
import json
import pandas as pd

# Path to the JSON file
data_file = r"data.json"

def fetch_attributes(properties, hier, element_dict, cntr):
    """Read data file, extract and build distinct hierarchies"""
    for idx, block in enumerate(properties):
        cntr += 1
        if 'type' in block:
            data_type = [block['type']]
            if block['type'] in ['document', 'jsonObject']:
                attribute_hierarchy = str(hier + '.' + block.get('code', block.get('name', '')))
                hash_str = hashlib.md5(attribute_hierarchy.encode('utf-8')).hexdigest()
                element_dict[hash_str] = ['Object', 'Parent', attribute_hierarchy, cntr, {}]

                if 'properties' in block:
                    fetch_attributes(block['properties'], attribute_hierarchy, element_dict, cntr)
            elif block['type'] in ['array', 'jsonArray']:
                attribute_hierarchy = str(hier + '.' + block.get('code', block.get('name', '')))
                hash_str = hashlib.md5(attribute_hierarchy.encode('utf-8')).hexdigest()
                element_dict[hash_str] = ['Object', 'Parent', attribute_hierarchy, cntr, {}]

                if 'properties' in block:
                    fetch_attributes(block['properties'], attribute_hierarchy, element_dict, cntr)
            else:
                attribute_hierarchy = str(hier + '.' + block.get('code', block.get('name', '')))
                hash_str = hashlib.md5(attribute_hierarchy.encode('utf-8')).hexdigest()
                
                col_desc = {
                    'description': block.get('description', ''),
                    'active_indicator': block.get('isActivated', ''),
                    'pii_indicator': block.get('piiIn', ''),
                    'primary_key_indicator': block.get('primaryKey', ''),
                    'data_type_name': data_type,
                    'column_length_number': block.get('length', '')
                }
                
                element_dict[hash_str] = ['Column', 'Leaf', attribute_hierarchy, cntr, col_desc]
    return

def process_hackolade_data(inp_trnsfm, db_nm):
    """Read data file, extract and build distinct hierarchies from scanner output"""
    entity_dict = {}
    cntr = 0
    top_hier = db_nm
    hash_str = hashlib.md5(top_hier.encode('utf-8')).hexdigest()
    entity_dict[hash_str] = ['Database', 'Parent', top_hier, cntr, {}]

    schema_nm = db_nm + '.' + 'EALDB'
    hash_str = hashlib.md5(schema_nm.encode('utf-8')).hexdigest()
    entity_dict[hash_str] = ['Schema', 'Parent', schema_nm, cntr, {}]

    data = inp_trnsfm['collections']
    for idx, items in enumerate(data):
        json_data = items
        cntr += 1

        collection_name = json_data.get('collectionName')
        if not collection_name:
            raise Exception("Collection is missing a 'collectionName'.")
        
        collection_hierarchy = f"{schema_nm}.{collection_name}"
        hash_str = hashlib.md5(collection_hierarchy.encode('utf-8')).hexdigest()
        entity_dict[hash_str] = ['Table', 'Parent', collection_hierarchy, cntr, {}]

        properties = json_data.get('properties')
        if properties is None:
            raise Exception("Properties missing in Collection")
        
        fetch_attributes(properties, collection_hierarchy, entity_dict, cntr)

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
