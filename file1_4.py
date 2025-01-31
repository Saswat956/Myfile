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

# Child class for PostgreSQL processing
class PostgreSQLAttributeProcessor(BaseAttributeProcessor):
    def fetch_attributes(self):
        for idx, block in enumerate(self.properties):
            col_desc = {}
            if 'type' in block:
                self.cntr += 1
                data_type = [block['type']]
                data_type = ",".join(data_type)

                if block['type'] in ['document', 'jsonObject']:
                    self._process_json_object(block, col_desc)
                elif block['type'] in ['array', 'jsonArray']:
                    self._process_json_array(block, col_desc)
                else:
                    self._process_regular_attribute(block, col_desc, data_type)

                if 'properties' in block:
                    PostgreSQLAttributeProcessor(block['properties'], self.hier, self.element_dict).fetch_attributes()

    def _process_json_object(self, block, col_desc):
        if 'arrayItem' not in block.keys():
            attribute_hierarchy = self._get_attribute_hierarchy(block)
            hash_str = hashlib.md5(attribute_hierarchy.encode('utf-8')).hexdigest()

            if hash_str not in self.element_dict:
                attr_type_nm = 'Object'
                if len(attribute_hierarchy.split('.')) == 3:
                    attr_type_nm = 'Table'
                col_desc['description'] = block.get('description', '')
                col_desc['active_indicator'] = block.get('isActivated', '')
                col_desc['Default value'] = block.get('default', block.get('defaultValue', ''))
                col_desc['data_type_name'] = block['type']
                col_desc['Precision'] = block.get('precision', '')
                self.element_dict[hash_str] = [attr_type_nm, 'Parent', attribute_hierarchy, self.cntr, col_desc]

    def _process_json_array(self, block, col_desc):
        attribute_hierarchy = self._get_attribute_hierarchy(block)
        hash_str = hashlib.md5(attribute_hierarchy.encode('utf-8')).hexdigest()

        col_desc['description'] = block.get('description', '')
        col_desc['active_indicator'] = block.get('isActivated', '')
        col_desc['Default value'] = block.get('default', block.get('defaultValue', ''))
        col_desc['data_type_name'] = block['type']
        col_desc['Precision'] = block.get('precision', '')
        self.element_dict[hash_str] = ['Object', 'Parent', attribute_hierarchy, self.cntr, col_desc]

    def _process_regular_attribute(self, block, col_desc, data_type):
        attribute_hierarchy = self._get_attribute_hierarchy(block)
        hash_str = hashlib.md5(attribute_hierarchy.encode('utf-8')).hexdigest()

        db_data_type = block.get('udt_name', block.get('mode', data_type))
        if 'length' in block and block['length']:
            db_data_type += f"({block['length']})"
        elif 'timePrecision' in block and block['timePrecision']:
            db_data_type += f"({block['timePrecision']})"
        if 'precision' in block and 'scale' in block and block['scale'] != 0:
            db_data_type += f"({block['precision']},{block['scale']})"

        col_desc['description'] = block.get('description', '')
        col_desc['active_indicator'] = block.get('isActivated', '')
        col_desc['pii_indicator'] = block.get('piiIn', '')
        col_desc['primary_key_indicator'] = block.get('primaryKey', '')
        col_desc['data_type_name'] = db_data_type
        col_desc['Default Value'] = block.get('default', block.get('defaultValue', ''))
        col_desc['Precision'] = block.get('precision', '')
        col_desc['column_length_number'] = block.get('maxLength', block.get('length', ''))
        col_desc['Column nullability'] = 'not null' if block.get('required') == 'true' else 'null'

        self.element_dict[hash_str] = ['Column', 'Leaf', attribute_hierarchy, self.cntr, col_desc]

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
    elif db_vendor == "PostgreSQL":
        processor_class = PostgreSQLAttributeProcessor
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
    data_file_path = r"/Users/saswatswain/Downloads/Oracle_datatypes_POC.json"
    
    with open(data_file_path, mode='r') as file_in:
        inp_transform = json.load(file_in)

    attribute_list = process_hackolade_data(inp_transform, 'EALDB')
    
    for idx, node_list in enumerate(attribute_list):
        print(idx, node_list)

    df = pd.DataFrame(attribute_list)
    df.to_csv(r"output.csv", index=False)
