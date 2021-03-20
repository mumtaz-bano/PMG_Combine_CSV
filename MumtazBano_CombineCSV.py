# -*- coding: utf-8 -*-
"""
Created on Sat Mar 20 16:27:27 2021

@author: mumta
"""


import sys
import pandas as pd
import os
from pathlib import Path

# list of input file names from the command line
files = sys.argv[1:]

"""
Find the memory useage for the files to decide if we need to use chunks
"""
def memory_usage(files):
    for file in files:
        file_df = pd.read_csv(file)
        memory_usage_each_file = file_df.memory_usage().sum()
        print('Total Current memory usage is-', memory_usage_each_file,'Bytes.')
      

"""
This method will check for :
    1. At least 2 arguments passed in the command line. 1st argument is always the .py file and after that, the files to process.
    2. Existence of the file in the given path 
    3. Extension of each file - We are checking for csv files
"""
def validate():
    
    # At least one csv file should be present in the argument
    if (len(sys.argv)) < 2:
        raise ValueError("No files provided in command line arguments")
            
        
    # Once the number of arguments are correct, loop through each file
    # to see the path and extension    
    for file in range(len(files)):
        
        # path validtion
        filePath = Path(files[file])
        if not filePath.exists():
            raise FileNotFoundError('File not found at the given path')
            
        # extension validation    
        filePathWithoutExtension, fileExtension = os.path.splitext(files[file])
        if fileExtension!= ".csv":
            raise Exception('File ' + str(filePathWithoutExtension) + ' is not in csv format')
              


"""
This method takes files provided in the command line as argument and process them to 
combine and create stdout as per the requirement.
"""
def combine_csv():
    
    # check for the vlidity of the arguments passed from commnd line
    validate()
    
    # As for this scenario, the test files could be of any size, by default we have kept the chunksize = 10*6
    # Hence commented the method calling
    #memory_usage(files) 

    # change chunk size to fit the data
    chunksize = 10*6

    # list of datafrmes where all the given files will be combined 
    df_concat = []

    # iterating over the files
    for file in files:
        chunk_list = []  # append each chunk df here 
        
        # based on the chunksize, read the csv file (To avoid memory related issues)
        # This data itself will be a list of chunks (Each chunk will have the list of rows)
        # We would iterate this data list to store in the chunk_list[]
        data = pd.read_csv(file, chunksize = chunksize,error_bad_lines=False)
       
        # iterating over the data for each chunk
        for chunk in data:
            # to have the filename as a new column with the base filename.
            # '.split('/')[-1]' will give the give the filename present at the end of the path.
            chunk['filename'] = file.split('/')[-1]
            
            # add chunks to list 
            chunk_list.append(chunk)
    
    
        # now convert the chunk_list to data frame and concat to the list df_concat
        df_concat.append(pd.concat(chunk_list))
                  
        
        """ Once we have the list of dataframe for each file. We will concat it into one
            to produce the stdout as required """

    combined_files = pd.concat(df_concat, ignore_index = True)
            
            # write combined dataframe as a csv and output file to stdout
    combined_files.to_csv(sys.stdout,index=False)
    


# calling the method to combine the csv files
combine_csv()


