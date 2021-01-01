import unittest
from datastore import DataStorage 
from datastoreNoLock import DataStorageNoLock
import time
import json
import threading 

ds=DataStorage()
ds_noLock = DataStorageNoLock()

class TestStringMethods(unittest.TestCase):
    def checkPresence(self,key):
        with open('data.txt','r') as json_file:
            data = json.load(json_file)
            if key in data:
                return True
            else:
                return False
    
    #successful create
    def test_createSuccess(self):
        message = ds.create('IBM',{'CEO': 'Arvind Krishna','website': 'www.ibm.com'})
        self.assertEqual(self.checkPresence("IBM"), True)
        self.assertEqual(message,"Operation successful")

    #successful read
    def test_readSuccess(self):
        ds.create('Google',{'CEO': 'Sundar Pichai','website': 'www.google.com'})
        self.assertEqual(ds.read('Google'), {'CEO': 'Sundar Pichai','website': 'www.google.com'})

    #successful delete
    def test_deleteSuccess(self):
        ds.delete('IBM')
        self.assertEqual(self.checkPresence('IBM'), False) 

    #redundant key create error
    def test_createKeyError(self):
        ds.create('Microsoft',{'CEO': 'Satya Nadella','website': 'www.microsoft.com'})
        message = ds.create('Microsoft',{'CEO': 'Satya Nadella','website': 'www.microsoft.com'})
        self.assertEqual(message,"ERROR: Create operation cannot be done for an existing key")

    #If key is not a string
    def test_createKeyTypeError(self):
        message = ds.create(456,{'CEO': 'Rajeev Suri','website': 'www.nokia.com'})
        self.assertEqual(message ,"ERROR: Received key with unexpected characteristics; Expected a string capped at 32 chars")  

    def test_readKeyError(self):
        #Key not found error for read
        message = ds.read("Green2Go")
        self.assertEqual(message,"ERROR: Key not found in data store") 

    def test_deleteKeyError(self):
        #Key not found error for delete
        message = ds.delete("Green2Go")
        self.assertEqual(message,"ERROR: Key not found in data store")


     #Time to Live cases of create, read, delete
    def test_ttlSuccess(self):
        message=ds.create('Adobe',{'CEO': 'Shantanu Narayen','website': 'www.adobe.com'},3) 
        self.assertEqual(self.checkPresence("Adobe"), True)
        self.assertEqual(message,"Operation successful")
        self.assertEqual(ds.read('Adobe'),{'CEO': 'Shantanu Narayen','website': 'www.adobe.com'})
        time.sleep(3) 
        #Disable read after TTL expiry
        self.assertEqual(ds.read('Adobe'),"ERROR: Time-To-Live Expired") 
        #Disable delete after TTL expiry
        self.assertEqual(ds.delete('Adobe'),"ERROR: Time-To-Live Expired") 


    
    #CreateThreadSafe -  demonstration with DataStorage with lock
    def createThreadSafe(self):
        limit = 12
        threads = [threading.Thread(target=ds.create, args=("key-{0}".format(i), {"value": i})) for i in range(limit)]
      
        for t in threads:
            t.start()

        for t in threads:
            t.join()

        with open("data.txt", "r") as json_file:
            storage = json.load(json_file)
            for i in range(limit):
                key = "key-{0}".format(i)
                if(key not in storage):
                    return False
            return True

    def test_createThreadSafe(self):
        self.assertEqual(self.createThreadSafe(),True)


    #DeleteThreadSafe - Thread safe demonstration with DataStorage with lock
    def deleteThreadSafe(self):
        limit = 7
        arr = [("key-{0}".format(i), {"value": i}) for i in range(limit)]
        for inst in arr:
            ds.create(inst[0],inst[1])

        threads = [threading.Thread(target=ds.delete, args=("key-{0}".format(i),)) for i in range(limit)]
      
        for t in threads:
            t.start()

        for t in threads:
            t.join()

        with open("data.txt", "r") as json_file:
            storage = json.load(json_file)
            for i in range(limit):
                key = "key-{0}".format(i)
                if(key in storage):
                    return False
            return True

    def test_deleteThreadSafe(self):
        self.assertEqual(self.deleteThreadSafe(),True)

    #No Thread safe for Create - Thread safe demonstration with DataStorage with No lock
    def createNoThreadSafe(self):
        limit = 12
        threads = [threading.Thread(target=ds_noLock.create, args=("key-{0}".format(i), {"value": i})) for i in range(limit)]
        
        for t in threads:
            t.start()

        for t in threads:
            t.join()

        with open("data.txt", "r") as json_file:
            storage = json.load(json_file)
            for i in range(limit):
                key = "key-{0}".format(i)
                if(key not in storage):
                    return False
            return True 

    def test_createNoThreadSafe(self):
        self.assertEqual(self.createNoThreadSafe(),False)



    
    #DeleteNoThreadSafe - No Thread safe demonstration with DataStorage with No lock
    def deleteNoThreadSafe(self):
        limit = 7
        arr = [("key-{0}".format(i), {"value": i}) for i in range(limit)]
        for inst in arr:
            ds_noLock.create(inst[0],inst[1])

        threads = [threading.Thread(target=ds_noLock.delete, args=("key-{0}".format(i),)) for i in range(limit)]
      
        for t in threads:
            t.start()

        for t in threads:
            t.join()

        with open("data.txt", "r") as json_file:
            storage = json.load(json_file)
            for i in range(limit):
                key = "key-{0}".format(i)
                if(key in storage):
                    return False
            return True

    def test_deleteNoThreadSafe(self):
        self.assertEqual(self.deleteNoThreadSafe(),False)


if __name__ == '__main__':
    file=open("data.txt","w")
    file.write("{}")
    file.close()
    unittest.main()
