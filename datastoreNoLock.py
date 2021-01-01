#Create operation
'''
    *Stores key-value pair
    *key   - string      - limit 32 chars
    *value - JSON object - 16KB - 16*1024 bytes
    *if Create invoked for existing key, throw error message
'''

#Read operation
'''
    *input   - key
    *output  - value as JSON object
'''

#Delete
'''
    *input - key
    *delete the instance of the provided key
'''
#Additional features
'''
    *Data store must be less than 1 GB
    *incorporate Time-To-Live (ttl) - no of seconds the key must be retained in data store
    *once the Time-To-Live expires, key is not available for Read or Delete 
        (ie)on TTL expiry remove the key from data store
    *appropriate error messages on unexpected usages
    *support multi-threading
'''
import json
import threading
import time
import os

class DataStorageNoLock:

    def __init__(self):
        self.storage_limit = 1024 * 1024 * 1024  #1GB limit
        self.value_limit   = 16 * 1024           #16KB limit

    def create(self,key,value,ttl=0):
        if(type(key)==str and len(key)<=32):
            if(os.stat('data.txt').st_size <= self.storage_limit):
                with open("data.txt","r") as json_file:
                    storage = json.load(json_file)
                    if key in storage:
                        return "ERROR: Create operation cannot be done for an existing key"
                    else:
                        if(len(value) <= self.value_limit):
                            if ttl==0:
                                value['ttl']=ttl
                            else:
                                value['ttl']= time.time()+ttl 
                            storage[key]=value
                            with open('data.txt', 'w') as outfile:
                                json.dump(storage, outfile,indent=4)
                            return "Operation successful"
                        else:
                            return "ERROR: Value limit exceeded; Expected value to be less than 16KB"
            else:
                return "Storage limit exceeded"
        else:
            return "ERROR: Received key with unexpected characteristics; Expected a string capped at 32 chars"


    def read(self,key):
        with open('data.txt','r') as json_file:
            data = json.load(json_file)
            if key in data:
                if data[key]['ttl']==0 or (data[key]['ttl']!=0 and time.time() < data[key]['ttl']):
                    del data[key]['ttl']
                    return data[key]
                
                if data[key]['ttl']!=0:
                    return "ERROR: Time-To-Live Expired"
            else:
                return "ERROR: Key not found in data store"

    def delete(self,key):
        with open("data.txt","r") as json_file:
            data = json.load(json_file)
            if key in data:
                if data[key]['ttl']==0 or (data[key]['ttl']!=0 and time.time() < data[key]['ttl']):
                    del data[key]
                    with open('data.txt','w') as json_file:
                        json.dump(data,json_file,indent=4)
                    return "Successfully deleted key-value pair"

                if data[key]['ttl']!=0:
                    return "ERROR: Time-To-Live Expired"
            else:
                return "ERROR: Key not found in data store"



