# Log Data Cleansing

This repository serves as Jaskirat Vig's final interview at Shield AI.

## Problem Statement

- Given a tar.gz file, need to extract it, convert to JSON and perform analysis
- Ensure id, name, timestamp, participantCount and participants are included (platform and environment are optional)
- Remove any other fields from schema
- Leverage parallelism to lower latency of program execution
- Return a processing summary

## Setup/Usage

This repo contains a sample log file "be-files.tar.gz" and two main.py files.
To execute main.py, simply run ```python main.py```. You will be asked to specify your input/output file paths.
The output will a summary of the files successfully processed and any errors encountered in the process.

main2.py is the version that although implements concurrency is not fully functional.

## External Libraries Used

- csv: Determine if the input file is in csv format 
- json: Write the output as json 
- multiprocessing: Execute multiple threads in our for loops to minimize execution time 
- os: Determine if a path is a file or directory 
- string: Determine if a character in a file is a valid ASCII character 
- sys: Exit threads running in parallel 
- tarfile: Open tar.gz files 
- time: Calculate total execution time

## Methods

- main(): Executes program 
- make_json_executer(pathToFiles, d): Iterates through files in a given folder and initiates a multithreading process 
- make_json(pathToFiles, filename, d): File validation checks and iterates through rows of file 
- processLines(rows, f, d): Add relevant rows to json structure 
- is_csv(infile): CSV validator

## Concurrency

- multiprocessing.manager is used to store variables across different threads 
- multiprocessing.process is used to start multiple processes at the same time so that we can iterate through files simultaneously 
- Drastically reduces execution time as the file processing is not done sequentially 
- Order doesn’t matter so parallelism is the correct approach