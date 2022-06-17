import sys
import os
from glob import glob

import json

# total arguments
n = len(sys.argv)

if n < 2:
    print("Usage: python combine.py <<folder with files>>")
    print("Provide the mandatory folder location with individual fhir json files.")
    print("python combine.py /usr/local/files_to_combine")
    exit(1)

inputFolder = sys.argv[1]
baseOutputFolder = sys.argv[2]

raw_files = glob(inputFolder + "/*.json")

jsonObject = {
    'AllergyIntolerance': [],
    'CarePlan': [],
    'CareTeam': [],
    'Claim': [],
    'Composition': [],
    'Condition': [],
    'Device': [],
    'DiagnosticReport': [],
    'DocumentReference': [],
    'Encounter': [],
    'ExplanationOfBenefit': [],
    'ImagingStudy': [],
    'Immunization': [],
    'Location': [],
    'Medication': [],
    'MedicationAdministration': [],
    'MedicationRequest': [],
    'MedicationStatement': [],
    'Observation': [],
    'Organization': [],
    'Patient': [],
    'Practitioner': [],
    'PractitionerRole': [],
    'Procedure': [],
    'Provenance': []
}

for raw_file in raw_files:
    full_path = os.path.join(inputFolder, raw_file)
    print("Processing File " + raw_file)
    file = open(full_path)
    json_data = json.load(file)
    resParentObject = json_data['FhirResource']['entry']
    for resource in resParentObject:
        jsonString = json.dumps(resource['resource'], indent=None)
        jsonObject[resource['resource']['resourceType']].append(jsonString)

for singleObject in jsonObject:
    outputFolder = os.path.join(baseOutputFolder, singleObject)
    if not os.path.exists(outputFolder):
        os.makedirs(outputFolder)
    outputFile = os.path.join(outputFolder, singleObject + '.ndjson')
    with open(outputFile, "w") as outfile:
        outfile.write('\n'.join(jsonObject[singleObject]))
print("Done...")
