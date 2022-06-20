import os
import sys
import json
import datetime
from glob import glob
from itertools import islice


def chunks(data, size=50000):
    it = iter(data)
    for i in range(0, len(data), size):
        yield {k: data[k] for k in islice(it, size)}


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

jsonObject = {}

for raw_file in raw_files:
    full_path = os.path.join(inputFolder, raw_file)
    print("Processing File " + raw_file)
    file = open(full_path)
    json_data = json.load(file)
    resParentObject = json_data['FhirResource']['entry']
    for resource in resParentObject:
        skipRecord = False
        # Resource types can be following
        # Account, ActivityDefinition, AllergyIntolerance, Appointment, AuditEvent,
        # BundleCarePlan, CareTeam, Claim, ClaimResponse, Communication, CommunicationRequest,
        # Composition, Condition, Contract, Coverage, CoverageEligibilityRequest, CoverageEligibilityResponse,
        # DetectedIssue, Device, DeviceDefinition, DeviceRequest, DiagnosticReport, DocumentReference,
        # Encounter, Endpoint, EpisodeOfCare, ExplanationOfBenefit, Goal, Group, HealthcareService,
        # ImagingStudy, Immunization, ImmunizationRecommendation, InsurancePlan, Library, List, Location,
        # Measure, MeasureReport, Media, Medication, MedicationAdministration, MedicationDispense,
        # MedicationKnowledge, MedicationRequest, MedicationStatement, NutritionOrder, Observation,
        # ObservationDefinition, OperationDefinition, OperationOutcome, Organization, OrganizationAffiliation,
        # Parameters, Patient, PlanDefinition, Practitioner, Procedure, Provenance, PractitionerRole,
        # QuestionnaireResponse, Questionnaire, RelatedPerson, RequestGroup, RiskAssessment, Slot, Specimen,
        # Subscription, Substance, ServiceRequest, SupplyRequest, Task, TestReport, TestScript, VisionPrescription
        resourceType = resource['resource']['resourceType']
        match resourceType:
            case "AllergyIntolerance":
                # skip if required field reaction is missing
                if 'reaction' not in resource['resource'].keys():
                    skipRecord = True
                    continue
            case "DiagnosticReport":
                # skip empty records
                if 'id' not in resource['resource'].keys():
                    skipRecord = True
                    continue
            case "Encounter":
                if 'class' not in resource['resource'].keys():
                    resource['resource']['class'] = {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                                                     "code": "IMP", "display": "inpatient encounter"}
            case "MedicationStatement":
                if 'status' not in resource['resource'].keys():
                    resource['resource']['status'] = 'active'

                startDate = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
                endDate = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

                if 'effectivePeriod' in resource['resource'].keys():
                    if 'start' in resource['resource']['effectivePeriod'].keys():
                        startDate = datetime.datetime.fromisoformat(
                            resource['resource']['effectivePeriod']['start']).replace(tzinfo=datetime.timezone.utc)
                    if 'end' in resource['resource']['effectivePeriod'].keys():
                        endDate = datetime.datetime.fromisoformat(
                            resource['resource']['effectivePeriod']['end']).replace(tzinfo=datetime.timezone.utc)
                if endDate < startDate:
                    endDate = startDate + datetime.timedelta(days=7)
                    resource['resource']['effectivePeriod']['end'] = endDate.strftime("%Y-%m-%dT%H:%M:%SZ")
                elif 'period' in resource['resource'].keys():
                    if 'start' in resource['resource']['period'].keys():
                        startDate = datetime.datetime.fromisoformat(resource['resource']['period']['start']).replace(
                            tzinfo=datetime.timezone.utc)
                    if 'end' in resource['resource']['period'].keys():
                        endDate = datetime.datetime.fromisoformat(resource['resource']['period']['end']).replace(
                            tzinfo=datetime.timezone.utc)
                    else:
                        endDate = startDate
                    if endDate < startDate:
                        endDate = startDate + datetime.timedelta(days=7)
                        resource['resource']['period']['end'] = endDate.strftime("%Y-%m-%dT%H:%M:%SZ")
            case default:
                pass
        if not skipRecord:
            jsonString = json.dumps(resource['resource'], indent=None)
            if resourceType not in jsonObject.keys():
                jsonObject[resourceType] = []
            jsonObject[resourceType].append(jsonString)

for singleObject in jsonObject:

    match singleObject:
        # Document Reference Is Very Large File,
        # 50 MB limit can reach in 1000 records
        # therefore reducing the batch side to 100
        case "DocumentReference":
            batchSize = 100
        case default:
            batchSize = 10000
    # Split the dictionary if it has more than 10000 records
    splitRecords = chunks({i: i for i in jsonObject[singleObject]}, batchSize)

    # Create output folder if it does not exist,the output
    # folder name is same as FHIR R4 Resource Names
    outputFolder = os.path.join(baseOutputFolder, singleObject)
    if not os.path.exists(outputFolder):
        os.makedirs(outputFolder)

    fileNumber = 0
    # write records in different files according to chunk size
    for item in splitRecords:
        fileNumber += 1
        outputFile = os.path.join(outputFolder, singleObject + '.' + '{:>03}'.format(fileNumber) + '.ndjson')
        with open(outputFile, "w") as outfile:
            outfile.write('\n'.join(item))
            # outfile.write('\n'.join(jsonObject[singleObject]))

print("Done...")
