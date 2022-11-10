# -*- coding: utf-8 -*-
"""MongoDB_test.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1Wrfo5GA8M5Q_DY3_SZ5Y0wdll-mZvj8d

**連線資料庫**
"""

# coding=UTF-8

from pymongo import MongoClient
from datetime import datetime
import time

'''連結mongodb不須認證'''
def connect2MongoDB(url='mongodb://127.0.0.1'):
    client = MongoClient(url)
    return client

def getDatabase(client, databaseName='test'):
    return client[databaseName]

def getCollectionCursor(db, collectionName='test1'):
    return db[collectionName]

#計算時間
def timeSpent(f):
    def wrapper(*args, **kwargs):
        start=datetime.now()
        response = f(*args, **kwargs)
        print(f.__name__)
        print ('Time:', datetime.now()-start)
        return response
    return wrapper

"""**資料處理**"""

#資料處理

from obspy import UTCDateTime, Stream, Trace
import struct
import os
import numpy as np

def unpackAfile(infile):
    temp = []
# == opening Afile ==
    b= os.path.getsize(infile)
    FH = open(infile, 'rb')
    line = FH.read(b)
    fileHeader= struct.unpack("<4s3h6bh6s", line[0:24])
    
    fileLength = fileHeader[3]
    port = fileHeader[10]
    FirstStn = fileHeader[11][0:4].decode('ASCII').rstrip()
    print(port)

# =================================Header===================================
    
    portHeader = []
    for i in range(24,port*32,32):
        port_data = struct.unpack("<4s4s3sbh2b4s12b",line[i:i+32])
        portHeader.append(port_data)

# =================================Data===================================    

    dataStartByte = 24+int(port)*32
    dataPoint = 3*int(port)*int(fileLength)*100
    times = int(port)*3*4
    data=[]

    data = struct.unpack("<%di"%dataPoint,line[dataStartByte:dataStartByte+dataPoint*4])

    
    portHeader = np.array(portHeader)    
    data = np.array(data)    
    idata =data.reshape((3,port,fileLength*100),order='F')
    
#== write to obspy Stream --
    sttime = UTCDateTime(fileHeader[1],fileHeader[4],fileHeader[5],fileHeader[6],fileHeader[7],fileHeader[8],fileHeader[2])
    sttime = datetime(fileHeader[1],fileHeader[4],fileHeader[5],fileHeader[6],fileHeader[7],fileHeader[8],fileHeader[2])
    
    print(fileHeader[1],fileHeader[4],fileHeader[5],fileHeader[6],fileHeader[7],fileHeader[8],fileHeader[2])
    npts = fileHeader[3]*fileHeader[9]
    samp = fileHeader[9]

    # afst = Afile's Stream
    afst = Stream()
    for stc in range(fileHeader[10]):#port
        stn = portHeader[stc][0].decode('ASCII').rstrip()
        instrument = portHeader[stc][1].decode('ASCII').rstrip()
        loc = '0'+str(portHeader[stc][6].decode('ASCII'))
        net = str(portHeader[stc][7].decode('ASCII')).rstrip()
        GPS = int(portHeader[stc][3])
        # remove GPS unlock or broken station
        if ( GPS == 1 or GPS == 2 ):
            chc = 0
            if instrument == 'FBA':
                chc = 1
            elif instrument == 'SP':
                chc = 4
            elif instrument == 'BB':
                chc = 7
            
            # for each channel in port
            for ch in range(3):
                chn = 'Ch'+str(chc+ch)
                
                stats = {'network': net, 'station': stn, 'location': loc,
                        'channel': chn, 'npts': npts, 'sampling_rate': samp,
                        'starttime': sttime}
                
                data = np.array(idata[ch][stc], dtype=float)
                sttmp = Stream([Trace(data=data, header=stats)])
                afst += sttmp
                data = np.array2string(data, separator=' ')
                stats['data'] = data
                temp.append(stats)
    
    #stst = stms[0]
    #stst.detrend('linear').plot()
    #stms.write('test.mseed', format='MSEED')    
    #print(afst[34].stats)
    #print(afst[35].stats)
    #afst.write("example.mseed", format="MSEED")
    return afst, FirstStn, fileHeader, temp
      
_, FirstStnm, fileHeader, data = unpackAfile('./05290646.40C')

"""測試範例說明:

.40c 檔案有36筆資料
  
新增資料部分 因為MongoDB 用 _id 當unique key
重複資料插入時，可能獲取的 _id相同而造成操作錯誤，
所以在代碼里手動指定 _id 避免錯誤

**測試範例一**

新增資料至資料庫集合 (Database collection)

JsonData 接受格式為python dict資料結構
"""

# 新增資料

def insert2MongoDB(collection, JsonData):
    collection.insert_many(JsonData)

@timeSpent
def insertTest(times, data, database='test', collection='test1'):
	client = connect2MongoDB()
	db = getDatabase(client, database)
	collection = getCollectionCursor(db, collection)
	index = 1
	
	if isinstance(data, list):
		for i in range(times):
			for d in data:
				d['_id'] = index
				index += 1
			#print(data[1]['_id'])
			insert2MongoDB(collection, data)
		#print(i)
	client.close()
	return

"""**測試範例二**

讀取collection內所有資料
"""

# 讀取collection內所有的資料

def selectAll(collection, limit=500):
    for data in collection.find().limit(limit):
        pass
      
@timeSpent
def selectAllQuery(database='test', collection='test1'):
    client = connect2MongoDB()
    db = getDatabase(client, database)
    collection = getCollectionCursor(db, collection)                
    selectAll(collection)
    client.close()
    return

"""**測試範例三**

讀取collection內指定測站的資料
"""

# 讀取collection內特定測站的資料

def selectStation(collection, station = "TAP"):
    for data in collection.find({"station": station}):
        #print(data)
        pass

  
@timeSpent
def selectStationQuery(database='test', collection='test1'):
    client = connect2MongoDB()
    db = getDatabase(client, database)
    collection = getCollectionCursor(db, collection)                
    selectStation(collection)
    client.close()
    return

"""**測試範例四**

依時間區段從collection內讀取資料
"""

# 依時間區段讀取資料

def selectDateRange(collection, start = datetime(2018, 5, 24, 7, 51, 4), end = datetime(2018, 5, 24, 7, 52, 4)):
    for data in collection.find( {'starttime': {'$lt': end, '$gte': start}}):
        #print(data)
        pass

@timeSpent
def selectDateRangeQuery(database='test', collection='test1'):
    client = connect2MongoDB()
    db = getDatabase(client, database)
    collection = getCollectionCursor(db, collection)
    selectDateRange(collection)
    client.close()
    return

"""**測試範例五**

匹配網路後依測站將資料分群統計
"""

# 匹配網路後依測站將資料分群統計

def pipelineAggregate(collection):
    pipeline = [{'$match': {'network': 'SMT'}},{'$group': {"_id": "$station", "count": {"$sum": 1}}}]
    result = collection.aggregate(pipeline)   
    #print(list(result))

@timeSpent        
def pipelineQuery(database='test', collection='test1'):
    client = connect2MongoDB()
    db = getDatabase(client, database)
    collection = getCollectionCursor(db, collection)
    pipelineAggregate(collection)
    client.close()
    return

# 5W/36 = 1388 資料5萬筆 case 1
# 20W/36 = 5555 資料20萬筆 case 2

caseInfo = ['50000','200000']
databaseCollection = [ 
                       {'database':'test','collection':'test1', 'Loops':1388},
                       {'database':'test','collection':'test2', 'Loops':5555}
                     ]

for i in range(2):
    print('Case: ', i+1, ' ', caseInfo[i])
    insertTest(databaseCollection[i]['Loops'], data, databaseCollection[i]['database'], databaseCollection[i]['collection'])
    selectAllQuery(databaseCollection[i]['database'], databaseCollection[i]['collection'])
    selectStationQuery( databaseCollection[i]['database'], databaseCollection[i]['collection'])
    selectDateRangeQuery( databaseCollection[i]['database'], databaseCollection[i]['collection'])
    pipelineQuery( databaseCollection[i]['database'], databaseCollection[i]['collection'])