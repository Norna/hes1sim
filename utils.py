
import swiftclient 
import os
import json
import base64
import uuid
import datetime
import itertools
import pickle
from pymongo import MongoClient
from collections import OrderedDict
from bson.objectid import ObjectId

#python-swiftclient
#python-keystoneclient

'''Represents OSConnectionParameter '''
class OSConnectionParameter(object):
    UserName = ""
    Password = ""
    TenantName = ""
    Url = ""

'''Represents OpenStack connection provider'''
class OSConnectionProvider(object):

    def __init__(self):
        self.IsInitialized = False

    def Initialize(self,container,limit, connParam):
        if isinstance(connParam, OSConnectionParameter) == False:
            raise InvalidArgumentException("connParam")

        self.Container = container
        self.Limit = limit
        self.ConnParam = connParam
        self.Connection = swiftclient.Connection(
							    authurl = self.ConnParam.Url,
							    user = self.ConnParam.UserName,
							    key = self.ConnParam.Password,
							    tenant_name = self.ConnParam.TenantName,
							    auth_version = 3.0)
        self.IsInitialized = True;

    def GetContainer(self):
        if self.IsInitialized == False:
            raise InvalidOperationException("Connection is not initialized")

        try:
            cntResponse,cntListing = self.Connection.get_container(self.Container, limit = self.Limit)
        except Exception as e:
            raise Exception("Connection errors - %s!" % str(e))
        
        return [ item['name'] for item in cntListing]
    def Put(self,name,data):
        self.Connection.put_object(self.Container,name,contents=data)

    def Get(self,name):
        (response, obj) = self.Connection.get_object(self.Container, name)
        return obj

    def Delete(self,name):
        self.Connection.delete_object(self.Container, name)

class DBOperation(object):
    def __init__(self,name,db):
        self._name = name
        self._db = db

    def insert(self,record):
        assert isinstance(record,OrderedDict)
        resp = self._db[self._name].insert_one(record)
        return str(resp.inserted_id)

    def delete(self,arg):
        resp = self._db[self._name].delete_many(arg)
        return resp.deleted_count

    def delete_by_id(self,record_id):
        resp = self._db[self._name].delete_one({"_id":ObjectId(record_id)})
        return resp.deleted_count

class DBConnection (object):
    def __init__(self,connString):
        self.Conn = MongoClient(connString)
    def use(self, dbName):
        return DBOperation(dbName,self.Conn[dbName]) 

class SweepParameter (object):
    Name = ""
    Value = 0.0
    def __str__(self):
        return "%s:%d" % (self.Name,self.Value)    

class SweepParameterInfor (object):
    Name = ""
    MinValue = 0.0
    MaxValue = 0.0
    StepValue = 0.0
    Tick = 0
    
    def __init__(self,name,minValue,maxValue):
        self.Name = name
        self.MinValue = minValue
        self.MaxValue = maxValue
    def Generate(self):
        if self.MinValue > self.MaxValue:
            raise Exception("Invalid min and max value")
        if self.StepValue <= 0:
            raise Exception("Invalid step value!")
        
        parameters = list()
        value = self.MinValue
        while value < self.MaxValue:
            param = SweepParameter()
            param.Name = self.Name
            param.Value = value
            parameters.append(param)
            value = value + self.StepValue

        return parameters

class SweepParameterStepInfo (SweepParameterInfor):

    def __init__(self,name,minValue,maxValue,stepValue):
        super(SweepParameterStepInfo,self).__init__(name,minValue,maxValue)
        if self.MinValue + stepValue > maxValue:
            raise Exception("Step value is too big!")
        self.StepValue = stepValue
        self.Tick = (self.MaxValue - self.MinValue) / self.StepValue

class SweepParameterTickInfo (SweepParameterInfor):
    def __init__(self,name,minValue,maxValue,tick):
        super(SweepParameterTickInfo,self).__init__(name,minValue,maxValue)

        if tick < 0 :
            raise Exception("Invalid tick value!")
    
        self.Tick = tick
        self.StepValue = (self.MaxValue - self.MinValue) / self.Tick
        
class SweepRunInfor (object):
    Key = ""
    BatchKey = ""
    Author = ""
    Descriptions = ""
    Tag = ""
    EnsembleKey = ""
    Parameters = dict()

    def __init__(self):
        self.Key = str(uuid.uuid4())
    
    def AddParameter(self,parameter):
        assert isinstance(parameter,SweepParameter)
        self.Parameters[parameter.Name] = parameter
    
    def _toJSON (self):
        data = OrderedDict()
        data["Key"] = self.Key
        data["Author"] = self.Author
        data["Descriptions"] = self.Descriptions
        data["BatchKey"] = self.BatchKey
        data["Tag"] = self.Tag
        data["EnsembleKey"] = self.EnsembleKey

        pItems = list()
        for key,param in self.Parameters.items():
            item = OrderedDict()
            item["Name"] = param.Name
            item["Value"] = param.Value
            pItems.append(item)
        data["Parameters"] = pItems

        return json.dumps(data)
    def _fromJSON (self,jsonData):
        data = json.loads(jsonData)

        if not "Key" in data:
            raise Exception("Cannot find Key in the input!")          
        if not "Author" in data:
            raise Exception("Cannot find author in the input!")
        if not "Parameters" in data:
            raise Exception("Cannot find parameters in the input!")
        if not "Descriptions" in data:
            raise Exception("Cannot find Descriptions in the input!")
        if not "BatchKey" in data:
            raise Exception("Cannot find BatchKey in the input!")
        if not "Tag" in data:
            raise Exception("Cannot find Tag in the input!")
        if not "EnsembleKey" in data:
            raise Exception("Cannot find EnsembleKey in the input!")                         
        
        self.Key = data["Key"]
        self.Author = data["Author"]
        self.Descriptions = data["Descriptions"]
        self.BatchKey = data["BatchKey"]
        self.Tag = data["Tag"]
        self.EnsembleKey = data["EnsembleKey"]

        self.Parameters = dict()
        for parameter in data["Parameters"]:
            swp = SweepParameter()
            swp.Name = parameter["Name"]
            swp.Value = parameter["Value"]
            self.Parameters[swp.Name] = swp
    
    def toInput(self):
        return base64.urlsafe_b64encode(self._toJSON().encode('ascii'))

    def fromInput(self,input):
        jsonData = base64.urlsafe_b64decode(input).decode('ascii')
        self._fromJSON(jsonData)

class SweepRunInforCollection(list):
    pass

class SweepRunBatchInfo (object):
    Key = ""
    Author = ""
    Descriptions = ""
    Tag = ""
    EnsembleSize=1
    CreatedDate = ""
    ParamterInfo = list()

    def __init__(self,author,descriptions = "",tag = "",ensembleSize=1):
        self.Key = str(uuid.uuid4())
        self.Author = author
        self.Tag = tag
        self.Descriptions = descriptions
        self.CreatedDate = datetime.datetime.utcnow()
        self.EnsembleSize = ensembleSize
    def _getSweepRunInfor(self,ensembleKey):
        sr = SweepRunInfor()
        sr.BatchKey = self.Key
        sr.Author = self.Author
        sr.Descriptions = self.Descriptions
        sr.Tag = self.Tag
        sr.EnsembleKey = ensembleKey
        sr.Parameters = dict()

        return sr

    def AddInfo(self,parameterInfo):
        assert isinstance(parameterInfo,SweepParameterInfor)
        self.ParamterInfo.append(parameterInfo)
    
    def GetCollection(self):
        infoList = list()
        for item in self.ParamterInfo:
            lst = item.Generate()
            infoList.append(item.Generate())
        prods = itertools.product(*infoList)
        coll = SweepRunInforCollection()

        for pd in prods:
            ensembleKey = str(uuid.uuid4())
            for i in range(0,self.EnsembleSize):
                sr = self._getSweepRunInfor(ensembleKey)
                for item in pd:
                    sr.AddParameter(item)
                coll.append(sr)

        return  coll 

class HES1RunRecord (object):
    Key = ""
    BatchKey = ""
    Author = ""
    Descriptions = ""
    Tag = ""
    EnsembleKey = ""
    Parameters = OrderedDict()
    StartDate = ""
    EndDate = ""
    
    @staticmethod
    def New(runInfo):
        assert isinstance(runInfo,SweepRunInfor)
        
        rec = HES1RunRecord()
        rec.Key = runInfo.Key
        rec.BatchKey = runInfo.BatchKey
        rec.Author = runInfo.Author
        rec.Descriptions = runInfo.Descriptions
        rec.Tag = runInfo.Tag
        rec.Parameters = runInfo.Parameters
        rec.EnsembleKey = runInfo.EnsembleKey

        return rec

    def ToDict (self):
        data = OrderedDict()
        data["Key"] = self.Key
        data["Author"] = self.Author
        data["Descriptions"] = self.Descriptions
        data["BatchKey"] = self.BatchKey
        data["Tag"] = self.Tag
        data["EnsembleKey"] = self.EnsembleKey
        data["StartDate"] = self.StartDate
        data["EndDate"] = self.EndDate

        pItems = list()
        for key,param in self.Parameters.items():
            item = OrderedDict()
            item["Name"] = param.Name
            item["Value"] = param.Value
            pItems.append(item)
        data["Parameters"] = pItems

        return data
