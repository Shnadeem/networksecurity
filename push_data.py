import os
import sys

from dotenv import load_dotenv

load_dotenv()

MONGO_DB_URL=os.getenv("MONDO_DB_URL")
#print(MONGO_DB_URL)

import certifi
ca=certifi.where()

import pandas as pd
import numpy as np
import pymongo
from networksecurity.logging.logger import logging
from networksecurity.exception.exception import NetworkSecurityException

class NetworkDataExtract():
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    
    def csv_to_json_convertor(self, file_path):
        try:
            data=pd.read_csv(file_path)
            records=data.to_dict(orient="records")
            #print(records)
            return records
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    
    def insert_data_mongodb(self,records,database,collection):
        try:
            self.records=records
            self.database=database
            self.collection=collection

            self.mongo_client=pymongo.MongoClient(MONGO_DB_URL)
            db=self.mongo_client[database]
            collection_obj=db[collection]
            
            if self.records:
                results=collection_obj.insert_many(records)
                return len(results.inserted_ids)

        except Exception as e:
            raise NetworkSecurityException(e,sys)


if __name__=="__main__":
    extract=NetworkDataExtract()
    records=extract.csv_to_json_convertor("Network_Data\phisingData.csv")
    inserted_records=extract.insert_data_mongodb(records=records,database="NADSAI",collection="NetworkData" )
    print(inserted_records)
