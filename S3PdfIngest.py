#!/usr/bin/env python3

# Manual ingestion and processing of PDFs stored in  AWS S3.
#  Download zip files of pdfs after comparing local to S3.
#  File names match regex and Pieces of Paper?
#  Sort files Good vs Bad and move to finished.

# Directory Structure
# ./pdfs
# ./pdfs/manual
# ./pdfs/manual/zip
# ./pdfs/manual/zip/processed
# ./pdfs/manual/working
# ./pdfs/manual/working/bad_pdf
# ./pdfs/manual/archive
# ./pdfs/manual/logs

import fileinput
import fnmatch
import glob
import math
import os
import pathlib
import re
import shutil
from datetime import date
from zipfile import ZipFile

import boto3
import PyPDF3 as pypdf

# Global variables
bucket = 'YourBucket'
s3 = boto3.client('s3')
#  Directory variables
base_dir = 'pdfs/'

finished_dir = os.path.join(base_dir, 'manual/finished')
working_dir = os.path.join(base_dir, 'manual/working')
badpdf_dir = os.path.join(base_dir, 'manual/working/bad_pdf')
zip_dir = os.path.join(base_dir, 'manual/zip')
processed_dir = os.path.join(base_dir, 'manual/zip/processed')
logs_dir = os.path.join(base_dir, 'manual/logs')
archive_dir = os.path.join(base_dir, 'manual/archive')

"""
--- Initialize directories if they don't exist ---
"""
def initialize_dirs():
    dirs = [working_dir, badpdf_dir, zip_dir, processed_dir, logs_dir, archive_dir, finished_dir]
    for x in dirs:
        pathlib.Path(x).mkdir(mode=0o744, exist_ok=True)


"""
 --- Pull zips from S3 to zip_dir ---
  Use boto3 to connect to s3 and download zip with filename logic
  Eval logic of what zips are in processed_dir against list of whats in
  S3 bucket, and download the differce (unprocessed zips)
"""
# Get bucket contents
#  Assist from https://alexwlchan.net/2017/07/listing-s3-keys/
def get_s3_keys(bucket):
    #Get a list of keys in an S3 bucket.
    keys = []
    resp = s3.list_objects_v2(Bucket=bucket)
    for obj in resp['Contents']:
        keys.append(obj['Key'])
    return keys

# Compare processed zips to bucket contents and make diff variable
def compare_files(s3files):
    all_s3_zips = s3files # array of filenames from get_s3_keys
    processed_zips = [os.path.basename(x) for x in glob.glob(processed_dir + '/*.zip')]
    print(f'S3 zips: {all_s3_zips}')
    print(f'Processed zips: {processed_zips}')
    files_diff = list(set(all_s3_zips) - set(processed_zips))
    print(f'>Difference in file lists: {files_diff} \n')
    return files_diff  # this will be what we want to download

# download new zips
def download_zips(bucket, files_diff):
    for x in files_diff:
        save_as = zip_dir + '/' + x
        try:
            s3.download_file(bucket, x, save_as)
        except:
            print(f'ERROR: Unexpected problem with downloading: {x} \n')
            raise
        else:
            print(f'Downloaded file(s) from S3: {x} \n')



"""
# --- Extract zip to working_dir ---
"""
# Unzip method to unzip into working_dir
def unzip_files(files_diff):
    for x in files_diff:
        try:
            with ZipFile(zip_dir + '/' + x ,'r') as zipObj:
                zipObj.extractall(working_dir)
        except:
            print(f'ERROR: Failed to extract files from: {x}')
            raise
        else:
            print(f'Files extracted from: {x}')


"""
# --- Check file names for proper convention ---
"""
# Lets check each portion of the filename so we can report what's incorrect
def check_names():
    # Patterns
    file_regex = 'yourRegex'
    all_pdfs = [os.path.basename(x) for x in glob.glob(working_dir + '/*.pdf')]
    for name in all_pdfs:
        if re.search(file_regex, name) is not None:
            print(f'good: {name}')
        else:
            shutil.move(working_dir + '/' + name, badpdf_dir + '/' + name)
            print(f'WARNING: Bad pdf name. Moved to bad_pdf directory: {name}')

# Make sure PiecesOfPaper is in the name and is correct
def check_pop():
    # Method to count Pages
    def pageCount(pdffile):
        reader = pypdf.PdfFileReader(open(pdffile, "rb"))
        return reader.getNumPages()

    all_pdfs_long = glob.glob(working_dir + '/*.pdf')
    for name in all_pdfs_long:
        #get POP from filename
        shortname = os.path.basename(name)
        fields = shortname.strip().split('_') # We want fields[3] for pop

        #get PAGES from pdf and convert to POP= pages*0.5 and round up - Double sided
        pop = math.ceil(pageCount(name) * 0.5)

        #Compare conditional to rename file if filename pop is wrong
        if int(fields[3]) == int(pop):
            print(f'[{shortname}] matches regex')
        else:
            #Logic to rename the file with correct POP
            # Could be made into a method to call
            fields[3] = str(pop)
            temp_name_list = fields
            new_name_str = '_'.join(temp_name_list)

            #Logic to save new name string
            try:
                shutil.move(name, working_dir + '/' + new_name_str)
            except:
                print(f'Move failed: Renaming bad POP name to good POP name: [{name}]')
                raise
            else:
                print(f'WARNING: POP Mismatch [{shortname}] was renamed to [{new_name_str}]')


"""
# --- Copy files to finished directory ---
"""
def move_good_pdfs():
    # Create a txt file to capture all moved files to transfer for archival
    def write_to_file(filename):
        file_start = archive_dir + '/' + 'processed_pdfs_list_'
        d = date.today()
        file_end = d.strftime("%m-%d-%Y") + ".txt"
        file_name_list = file_start + file_end
        with open(file_name_list, "a+") as file_object:
            file_object.write(filename + "\n")
            file_object.close()
            #print(f'Written to processed_pdfs_list file: [{filename}]')

    all_pdfs_long = glob.glob(working_dir + '/*.pdf')
    #Logic to move each file in working to finished
    for name in all_pdfs_long:
        shortname = os.path.basename(name)
        destination = finished_dir + '/' + shortname
        try:
            shutil.move(name, destination)
        except:
            print(f'ERROR: Move failed for working/*.pdf to transfer dir:  [{name}]')
            raise
        else:
            print(f'[{shortname}] moved to  [{destination}]')
            write_to_file(shortname)

"""
# --- Archive steps to move zip to processed_dir ---
#  Maybe check previously processed zips against whats available in S3.
#  Generate a list of all the PDF files ingested.
#  Clean up the working_dir
#  Add portion to upload zip_filelist_diff for a backup copy?
"""
# Move zip(s) to processed
def move_zips():
    zips = glob.glob(zip_dir + '/*.zip')
    for name in zips:
        shortname = os.path.basename(name)
        destination = processed_dir + '/' + shortname
        try:
            shutil.move(name, destination)
        except:
            print(f'ERROR: Move failed for zip to processed dir:  [{name}]')
            raise
        else:
            print(f'[{shortname}] moved to  [{destination}]')

# Ensure working folders are clean
def cleanup(folders):
    return #none at the moment

"""
# --- Log results to log_dir ---
"""
# TODO: add logging

# MAIN
if __name__ == '__main__':
    print('\n---Running initialize_dirs--')
    initialize_dirs()

    print('\n---Running download_zips---')
    files_diff = compare_files(get_s3_keys(bucket))
    download_zips(bucket, files_diff)

    print('\n---Running unzip_files---')
    unzip_files(files_diff)

    print('\n---Running check_names---')
    check_names()

    print('\n---Running check_pop---')
    check_pop()

    print('\n---Running move_good_pdfs---')
    move_good_pdfs()

    print('\n---Performing Cleanup and Archiving---')
    move_zips()

else:
    print('I am being imported from another module')
