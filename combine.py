import sys
import os

import json

# total arguments
n = len(sys.argv)

if n < 2:
    print("Usage: python combine.py <<folder with files>>")
    print("Provide the mandatory folder location with individual fhir json files.")
    print("python combine.py /usr/local/files_to_combine")
    exit(1)

path = sys.argv[1]
raw_files = os.listdir(path)

json_array = []

for raw_file in raw_files:
    full_path = os.path.join(path, raw_file)
    file = open(full_path)
    json_data = json.load(file)

    json_array.append(json_data['FhirResource'])

    print("Done loading file " + raw_file)

json_object = json.dumps(json_array, indent=1)
with open("combined_data.json", "w") as outfile:
    outfile.write(json_object)
