import csv
import json
import multiprocessing
import os
import string
import sys
import tarfile
import time


def main():
    start_time = time.time()

    manager = multiprocessing.Manager()
    d = manager.dict()
    d["data"] = 0

    pathToFiles = 'be-files.tar.gz'  # input('What the path to your input files?\n')  # 'be-files.tar.gz'
    jsonFilePath = 'output.json'  # input('What the path to your output?\n')  # 'output.json'

    file = tarfile.open(pathToFiles)
    file.extractall('./files')

    make_json_executer("./files", d)

    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf:
        jsonf.write(json.dumps(d["data"], indent=4))

    print("Execution time is %s seconds" % (time.time() - start_time))

    file.close()


def make_json_executer(pathToFiles, d):
    for filename in os.listdir(pathToFiles):
        proc = multiprocessing.Process(
            target=make_json,
            args=(pathToFiles, filename, d)
        )
        proc.start()


def make_json(pathToFiles, filename, d):
    # global invalidFiles, successfullyProcessed
    f = os.path.join(pathToFiles, filename)

    if os.path.isdir(f):
        print("Diving into folder " + f)
        make_json_executer(f, d)
        sys.exit()

    print("Processing file " + f)

    if filename.startswith('.'):
        print("Ignoring hidden file " + f)
        sys.exit()

    if not is_csv(f):
        print("Invalid file " + f)
        # invalidFiles = invalidFiles + 1
        sys.exit()

    print("Valid file " + f)

    processed = True
    with open(f, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)
        for rows in csvReader:
            if processLines(rows, f, d) == 1:
                break


def processLines(rows, f, d):
    if not ('id' in rows and 'name' in rows and 'timestamp' in rows and 'participants' in rows and
            'participant_count' in rows):
        print("File " + f + " does not contain all the required columns. Aborting... ")
        return 1
    key = rows['id']
    # d["data"][key] = rows
    # linesProcessed = linesProcessed + 1
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


if __name__ == '__main__':
    main()


