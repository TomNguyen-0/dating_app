#!/usr/bin/python
# -*- coding:utf-8 -*-

"""
© Prog Leasing, LLC 2023, all rights reserved
Description: Load SQL tables as pandas dataframes 

Revision History
ChangeDate      ChangedBy              Ticket#				Comments
05/10/2023      Tom Nguyen             BLUE-1145            intial create modeled from DisableEnablejob.py

"""
import pyodbc
import pandas as pd
import time
import pickle
import platform


#where and how long is data cached by default
CACHE_TIME = 60*24 # default cache time is 24 hrs
CACHE_FILE_NAME = 'results.cache'


        
class DWH_DB:

    def __init__(self, verbose=False, servername='DW-SQL-QAS', database='DWHRisk'):
        self.servername = servername
        self.database = database
        if platform.system() == 'Windows':
            _DATABASE_ = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.servername};DATABASE={self.database};Trusted_Connection=yes'  
        self.connect_string = _DATABASE_
        self.verbose = verbose
        

    def query(self, qry):
        start_time = time.time()

        cnxn = pyodbc.connect(self.connect_string, autocommit=True)
        data = pd.read_sql(qry, con=cnxn)
        cnxn.close()

        if self.verbose:
            print("Runtime = %.2f"%(time.time()-start_time))

        return data
    
    def cnt(self, tbl_name):
        start_time = time.time()

        cnxn = pyodbc.connect(self.connect_string, autocommit=True)
        row_cnt = pd.read_sql("SELECT count(*) FROM " + tbl_name , con=cnxn)
        cnxn.close()

        if self.verbose:
            print("Runtime = %.2f"%(time.time()-start_time))

        return row_cnt
    
    def get_1000(self, tbl_name):
        start_time = time.time()

        cnxn = pyodbc.connect(self.connect_string, autocommit=True)
        data = pd.read_sql("SELECT TOP 1000 * FROM " + tbl_name , con=cnxn)
        cnxn.close()

        if self.verbose:
            print("Runtime = %.2f"%(time.time()-start_time))

        return data
    
    def get_tbl(self, tbl_name):
        start_time = time.time()

        cnxn = pyodbc.connect(self.connect_string, autocommit=True)
        data = pd.read_sql("SELECT * FROM " + tbl_name , con=cnxn)
        cnxn.close()

        if self.verbose:
            print("Runtime = %.2f"%(time.time()-start_time))

        return data
    
    def execute_query(self, qry):
        start_time = time.time()

        cnxn = pyodbc.connect(self.connect_string, autocommit=True)
        cur = cnxn.cursor()
        cur.execute(qry)
        
        if self.verbose:
            print("Runtime = %.2f"%(time.time()-start_time))
            
        return None
  