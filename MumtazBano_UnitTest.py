# -*- coding: utf-8 -*-
"""
Created on Sun Mar 21 12:40:07 2021

@author: mumta
"""

import pandas as pd
import sys
import unittest
from MumtazBano_CombineCsv import CombineCSVFiles
from io import StringIO
import warnings

class TestCombineMethod(unittest.TestCase):

    # initialize 
    testOutputFilePath = "./test_output.csv"
    csvCombinerFilePath = "./MumtazBano_CombineCsv.py"
    accessoriesFilePath = "./fixtures/accessories.csv"
    clothingFilePath = "./fixtures/clothing.csv"
    emptyFilePath = "./empty.csv"
    householdCleanersPath="./fixtures/household_cleaners.csv"
    

    testOutputFile = None
    combineCSVObj = CombineCSVFiles()


    @classmethod
    def tearDownClass(cls):
         
        if cls.testOutputFile is not None:
            cls.testOutputFile.close()
            cls.testOutputFile = None

       
    def loadTestData(self):
        self.testOutputFile = open(self.testOutputFilePath, 'w+', encoding="utf-8")

    def setUp(self):
        # setup
        self.output = StringIO()
        sys.stdout = self.output
        self.loadTestData()
        
        # to ignore the python resource warnings
        if not sys.warnoptions:
            warnings.simplefilter("ignore")
        


    def testCombinedValueInCsv(self):
        
        argv = [self.csvCombinerFilePath, self.accessoriesFilePath, self.clothingFilePath,self.householdCleanersPath]
        
        self.combineCSVObj.combine_csv(argv)
        
        # Write the output of combine_csv in the file
        self.testOutputFile.write(self.output.getvalue())

        # read csv to dataframe
        df_accessories = pd.read_csv(self.accessoriesFilePath, lineterminator='\n')
        df_clothing = pd.read_csv(self.clothingFilePath, lineterminator='\n')
        df_householdCleaners = pd.read_csv(self.householdCleanersPath, lineterminator='\n')
        
        sumOfEachFiles = (df_accessories.shape[0]+df_clothing.shape[0]+df_householdCleaners.shape[0])
        
        with open(self.testOutputFilePath) as combinedFile:
            combined_df = pd.read_csv(combinedFile, lineterminator='\n')
            self.testOutputFile.close() 
            self.testOutputFile = None
            
            # test the sumOfIndividul files is equal to the length of the the dtaFrme of out put combined file
            self.assertEqual(combined_df.shape[0], sumOfEachFiles)
           
    
    def testValueErrorWithoutInputCsvFile(self):

        # run csv_combiner with no arguments
        argv = [self.csvCombinerFilePath]
        self.assertRaises(ValueError, lambda: self.combineCSVObj.combine_csv(argv))


    def testValueErrorWithEmptyFile(self):

        # run csv_combiner with an empty file
        argv = [self.csvCombinerFilePath, self.emptyFilePath]
        self.assertRaises(ValueError, lambda: self.combineCSVObj.combine_csv(argv))


    def testFileNotFoundErrorWithIncorrectFilePath(self):

        # run csv_combiner with a file that doesn't exist
        argv = [self.csvCombinerFilePath, "no_file.csv"]
        self.assertRaises(FileNotFoundError, lambda: self.combineCSVObj.combine_csv(argv))
