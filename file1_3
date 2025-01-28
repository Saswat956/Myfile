----------------------------------------------------------------------------------------------------------------------------------------

import base64
import hashlib
import json
import pandas as pd

# Base class for handling attribute fetching
class BaseAttributeProcessor:
    def __init__(self, properties, hier, element_dict):
        self.properties = properties
        self.hier = hier
        self.element_dict = element_dict
        self.cntr = 0

    def fetch_attributes(self):
        raise NotImplementedError("Subclasses should implement this method.")

# Child class for Oracle processing
class OracleAttributeProcessor(BaseAttributeProcessor):
    def fetch_attributes(self):
        for idx, block in enumerate(self.properties):
            col_desc = {}
            if 'type' in block:
                self.cntr += 1
                data_type = block['type']

                attribute_hierarchy = self._get_attribute_hierarchy(block)
                hash_str = hashlib.md5(attribute_hierarchy.encode('utf-8')).hexdigest()
                
                if hash_str not in self.element_dict:
                    col_desc['description'] = block.get('description', '')
                    col_desc['active_indicator'] = block.get('isActivated', '')
                    col_desc['data_type_name'] = data_type
                    # Additional Oracle-specific fields can be added here
                    self.element_dict[hash_str] = ['Column', 'Leaf', attribute_hierarchy, self.cntr, col_desc]

                if 'properties' in block:
                    OracleAttributeProcessor(block['properties'], attribute_hierarchy, self.element_dict).fetch_attributes()

    def _get_attribute_hierarchy(self, block):
        return f"{self.hier}.{block.get('code', block.get('name', ''))}"

# Child class for DB2 processing
class DB2AttributeProcessor(BaseAttributeProcessor):
    def fetch_attributes(self):
        for idx, block in enumerate(self.properties):
            col_desc = {}
            if 'type' in block:
                self.cntr += 1
                data_type = block['type']

                attribute_hierarchy = self._get_attribute_hierarchy(block)
                hash_str = hashlib.md5(attribute_hierarchy.encode('utf-8')).hexdigest()
                
                if hash_str not in self.element_dict:
                    col_desc['description'] = block.get('description', '')
                    col_desc['active_indicator'] = block.get('isActivated', '')
                    col_desc['data_type_name'] = data_type
                    # Additional DB2-specific fields can be added here
                    self.element_dict[hash_str] = ['Column', 'Leaf', attribute_hierarchy, self.cntr, col_desc]

                if 'properties' in block:
                    DB2AttributeProcessor(block['properties'], attribute_hierarchy, self.element_dict).fetch_attributes()

    def _get_attribute_hierarchy(self, block):
        return f"{self.hier}.{block.get('code', block.get('name', ''))}"

def process_hackolade_data(inp_trnsfm, db_nm):
    entity_dict = {}
    
    # Determine which processor to use based on dbVendor
    db_vendor = inp_trnsfm.get("dbVendor", "")
    print(db_vendor)
    if db_vendor == "Oracle":
        processor_class = OracleAttributeProcessor
    elif db_vendor == "DB2":
        processor_class = DB2AttributeProcessor
    else:
        raise ValueError(f"Unsupported dbVendor: {db_vendor}")

    cntr = 0
    top_hier = db_nm
    hash_str = hashlib.md5(top_hier.encode('utf-8')).hexdigest()
    entity_dict[hash_str] = ['Database', 'Parent', top_hier, cntr, {}]

    schema_nm = f"{db_nm}.EALDB"
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

        if 'properties' not in json_data:
            raise Exception("Properties missing in Collection")

        # Use the appropriate processor to fetch attributes
        processor_instance = processor_class(json_data['properties'], collection_hierarchy, entity_dict)
        processor_instance.fetch_attributes()

    entity_list_sorted = sorted(entity_dict.items(), key=lambda x: x[1][3])
    return [[k, v[0], v[1], v[2], v[4]] for k, v in entity_list_sorted]

if __name__ == "__main__":
    data_file_path = r"/POC.json"
    
    with open(data_file_path, mode='r') as file_in:
        inp_transform = json.load(file_in)

    attribute_list = process_hackolade_data(inp_transform, 'EALDB')
    
    for idx, node_list in enumerate(attribute_list):
        print(idx, node_list)

    df = pd.DataFrame(attribute_list)
    df.to_csv(r"output.csv", index=False)

Function Definitions:
fetch_attributes(properties, hier, element_dict, cntr, current_node_data_type=None):
This function is responsible for recursively extracting attributes from properties in the JSON structure.
It builds a hierarchy based on the type of each block (e.g., document, array, or column).
It creates unique keys using MD5 hashes to avoid duplicates and stores relevant metadata (like descriptions and indicators) in a dictionary called element_dict.
Different handling is implemented based on the type of data (e.g., PostgreSQL-specific fields like udt_name).
process_hackolade_data(inp_trnsfm, db_nm):
This function reads the input transformation data and builds distinct hierarchies for the database schema.
It initializes an entity dictionary to store hierarchical information about databases, schemas, and tables.
It iterates over collections in the JSON data to extract properties and call fetch_attributes for processing.
Main Execution Block:
The script reads the specified JSON file.
It processes the data using process_hackolade_data, specifying 'EALDB' as the database name.
Finally, it prints out the extracted attributes and saves them as a CSV file.
