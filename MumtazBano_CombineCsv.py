# -*- coding: utf-8 -*-
"""
Created on Sun Mar 21 12:38:11 2021

@author: mumta
"""

import sys
import pandas as pd
import os
from pathlib import Path

class CombineCSVFiles:
    
    """
    Find the memory usage for the files to decide if we need to use chunks
    """
    def memory_usage(self, argv):
        files = argv[1:]
        for file in files:
            file_df = pd.read_csv(file)
            memory_usage_each_file = file_df.memory_usage().sum()
            print('Total Current memory usage is-', memory_usage_each_file,'Bytes.')
          
    
    """
    This method will check for :
        1. At least 2 arguments passed in the command line. 1st argument is always the .py file and after that, the files to process.
        2. Existence of the file in the given path 
        3. Extension of each file - We are checking for csv files
        4. CSV should not be empty
    """
    def validate(self, argv):
        
        files = argv[1:]
         
        # At least one csv file should be present in the argument
        if (len(argv)) < 2:
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
         
            # empty file path 
            df_filePath = pd.read_csv(filePath)
            if df_filePath.empty :
                raise ValueError("csv file is empty")
                  
    
    
    """
    This method takes files provided in the command line as argument and process them to 
    combine and create stdout as per the requirement.
    """
    def combine_csv(self, argv: list):
        files = argv[1:]
        
        # check for the validity of the arguments passed from commnd line
        self.validate(argv)
        
        # As for this scenario, the test files could be of any size, by default we have kept the chunksize = 10*6
        # Hence commented the method calling
        #self.memory_usage(files) 
    
        # change chunk size to fit the data
        chunksize = 10*6
    
        # list of datafrmes where all the given files will be combined 
        df_concat = []
    
        # iterating over the files
        for file in files:
            chunk_list = []  # append each chunk here 
            
            # based on the chunksize, read the csv file (To avoid memory related issues)
            # This data itself will be a list of chunks (Each chunk will have the list of rows)
            # We would iterate this data list to store in the chunk_list[]
            data = pd.read_csv(file, chunksize = chunksize)
    
            # iterating over the data for each chunk
            for chunk in data:
    
                # to have the filename as a new column with the base filename.
                # '.split('/')[-1]' will give the give the filename present at the end of the path.
                chunk['filename'] = file.split('/')[-1]
                
                # add chunks to list 
                chunk_list.append(chunk)
        
        
            # now convert the chunk_list to dataframe and concat to the list df_concat
            df_concat.append(pd.concat(chunk_list))
           
            
            """ Once we have the list of dataframe for each file. We will concat it into one
                to produce the stdout as required """
    
        combined_files = pd.concat(df_concat, ignore_index = True)
            
        # write combined dataframe as a csv and output file to stdout
        combined_files.to_csv(sys.stdout,index=False,line_terminator='\n')
    
    
    
# main method
def main():
    combiner = CombineCSVFiles()
    combiner.combine_csv(sys.argv)
    
# calling the method to combine the csv files    
if __name__ == '__main__':
    main()