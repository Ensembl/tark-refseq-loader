import json

def remove_duplicates(json_array):
    unique_objects = {}
    
    for obj in json_array:
        # Create a unique key based on all attributes
        key = (obj['assembly'], obj['biotype'], obj['exon_set_checksum'], obj['loc_checksum'],
               obj['loc_end'], obj['loc_start'], obj['loc_region'], obj['loc_strand'],
               obj['stable_id'], obj['stable_id_version'], obj['sequence'])

        print(key)
        print(type(key))
        
        # Use the key to prevent duplicates
        if key not in unique_objects:
            unique_objects[key] = obj

        print(unique_objects)
    
    # Return the filtered objects as a list
    return list(unique_objects.values())

# Example usage
json_input = [
    {"assembly": "assembly1", "biotype": "type1", "exon_set_checksum": "checksum1", "loc_checksum": "checksum2",
     "loc_end": 100, "loc_start": 1, "loc_region": "region1", "loc_strand": "+", "stable_id": "id1",
     "stable_id_version": "v1", "sequence": "ATCG"},
    {"assembly": "assembly1", "biotype": "type1", "exon_set_checksum": "checksum1", "loc_checksum": "checksum2",
     "loc_end": 100, "loc_start": 1, "loc_region": "region1", "loc_strand": "+", "stable_id": "id1",
     "stable_id_version": "v1", "sequence": "ATCG"}  # Duplicate
]

# Remove duplicates
output = remove_duplicates(json_input)
# print(json.dumps(output, indent=4))


