import csv
import json
import os
import string
import tarfile
import time

included_cols = ['id', 'name', 'timestamp', 'platform', 'environment', 'participants', 'participant_count']


def make_json(pathToFiles):
    global successfullyProcessed, linesProcessed, invalidFiles, missingRequiredColumns, processed
    for filename in os.listdir(pathToFiles):

        f = os.path.join(pathToFiles, filename)

        if os.path.isdir(f):
            print("Diving into folder " + f)
            make_json(f)
            continue

        print("Processing file " + f)

        if filename.startswith('.'):
            print("Ignoring hidden file " + f)
            continue

        if not is_csv(f):
            print("Invalid file " + f)
            invalidFiles = invalidFiles + 1
            continue

        print("Valid file " + f)

        processed = True
        with open(f, encoding='utf-8') as csvf:
            csvReader = csv.DictReader(csvf)
            for rows in csvReader:
                if processFiles(f, rows) == 1:
                    break
        if processed:
            successfullyProcessed = successfullyProcessed + 1


def processFiles(f, rows):
    global missingRequiredColumns, linesProcessed, processed
    if not ('id' in rows and 'name' in rows and 'timestamp' in rows and 'participants' in rows and
            'participant_count' in rows):
        print("File " + f + " does not contain all the required columns. Aborting... ")
        missingRequiredColumns = missingRequiredColumns + 1
        processed = False
        return 1
    vals = {}
    for x in range(len(included_cols)):
        vals[included_cols[x]] = rows[included_cols[x]]
    key = rows['id']
    data[key] = vals
    linesProcessed = linesProcessed + 1
    return 0


def is_csv(infile):
    try:
        with open(infile, newline='') as csvfile:
            start = csvfile.read(4096)

            if not all([c in string.printable or c.isprintable() for c in start]):
                return False
            csv.Sniffer().sniff(start)
            return True
    except:
        return False


successfullyProcessed, linesProcessed, invalidFiles, missingRequiredColumns = 0, 0, 0, 0

pathToFiles = 'be-files.tar.gz'  # input('What the path to your input files?\n')  # 'be-files.tar.gz'
jsonFilePath = 'output.json'  # input('What the path to your output?\n')  # 'output.json'

start_time = time.time()

file = tarfile.open(pathToFiles)
file.extractall('./files')

data = {}

make_json("./files")

with open(jsonFilePath, 'w', encoding='utf-8') as jsonf:
    jsonf.write(json.dumps(data, indent=4))

print(successfullyProcessed, "files were successfully processed")
print(linesProcessed, "lines were successfully processed")
print(invalidFiles, "files were invalid")
print(missingRequiredColumns, "files did not contain all required columns")

print("Execution time is %s seconds" % (time.time() - start_time))

file.close()
