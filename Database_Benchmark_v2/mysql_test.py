# coding: utf-8

from obspy import *
import struct
import os
import numpy as np

def unpackAfile(infile):

# == opening Afile ==
    b= os.path.getsize(infile)
    FH = open(infile, 'rb')
    line = FH.read(b)
    fileHeader= struct.unpack("<4s3h6bh6s", line[0:24])
    
    fileLength = fileHeader[3]
    port = fileHeader[10]
    FirstStn = fileHeader[11][0:4].decode('ASCII').rstrip()
    #print(fileHeader)
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
    npts = fileHeader[3]*fileHeader[9]
    samp = fileHeader[9]
    afst = Stream()
    
    for stc in range(fileHeader[10]):
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
    
    return afst, FirstStn, fileHeader
      
data, FirstStnm, fileHeader= unpackAfile('./05290646.40C')

import pymysql
#連接到mysql
def connect2mysql(host,user,passwd,db):
    db = pymysql.connect(
        host=host,
        port=3306,
        charset='utf8',
        user=user,
        passwd=passwd,
        db=db
    )
    cursor = db.cursor()
    return db, cursor

#將資料存入mysql
def insert2mysql(table, data):
    sql = """INSERT INTO """+table+"""(network,station,location,channel,starttime,endtime,sampling_rate,delta,npts,calib,data) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    l = len(data)
    inserts = []
    for i in range(0,l):
        inserts.append([str(data[i].stats['network']), str(data[i].stats['station']), 
		str(data[i].stats['location']), str(data[i].stats['channel']), str(data[i].stats['starttime']),
		str(data[i].stats['endtime']), str(data[i].stats['sampling_rate']), str(data[i].stats['delta']), 
		str(data[i].stats['npts']), str(data[i].stats['calib']), str(data[i].data)])
    cursor.executemany(sql,inserts)
    db.commit()

#查找Table內所有資料
def selectAll(table):
    cursor.execute("select * from "+table+" limit 500")
    data = cursor.fetchall()
    return data

#查找指定station之資料
def selectStation(table, station):
    cursor.execute("select * from "+table+" where station='%s'" % (station))
    data = cursor.fetchall()
    return data

import datetime
#查找指定時間段內之資料
def selectDateRange(table,starttime,endtime):
    cursor.execute("select * from "+table+" where starttime >'%s' and endtime < '%s'" % (starttime,endtime))
    data = cursor.fetchall()
    return data
	
#以station來分類得到各類資料筆數
def selectGroupByStation(table, network):
    cursor.execute("select station,count(*) from "+table+" where network ='%s' group by station" % (network))
    data = cursor.fetchall()
    return data

import time
#計算mysql操作所花費的時間
def timeSpent(f):
    def wrapper(*args, **kwargs):
        start=datetime.datetime.now()
        response = f(*args, **kwargs)
        print ('Time:', datetime.datetime.now()-start)
        return response
    return wrapper

#配合上面之api來計算mysql操作時間
@timeSpent
def insertTest(times,data,table):
    print('insertTest:')
    for i in range(times):
        insert2mysql(table,data)
    return
    
@timeSpent
def selectAllQuery(table):
    print('selectAllQuery:')
    selectAll(table)
    return
  
@timeSpent
def selectStationQuery(table,station='TAP'):
    print('selectStationQuery:')
    selectStation(table,station)
    return
  
@timeSpent
def selectDateRangeQuery(table):
    print('selectDateRangeQuery:')
    start = datetime.datetime(2018, 5, 24, 7, 51, 4)
    end = datetime.datetime(2018, 9, 24, 7, 52, 4)
    selectDateRange(table,start,end)
    return
  
@timeSpent        
def selectGroupByStationQuery(table, network='SMT'):
    print('selectGroupByStationQuery:')
    selectGroupByStation(table, network)
    return

#效能測試1
db, cursor=connect2mysql('127.0.0.1','root','recast203','test') #連線

# 5W/36 = 1388 資料5萬筆 case 1
# 20W/36 = 5555 資料20萬筆 case 2

caseInfo = ['50000','200000']
databaseCollection = [ 
                       {'database':'test','collection':'test1', 'Loops':1388},
                       {'database':'test','collection':'test2', 'Loops':5555}
                     ]

for i in range(2):
  print('Case: ', i+1, ' ', caseInfo[i])
  insertTest(databaseCollection[i]['Loops'], data, databaseCollection[i]['collection'])
  selectAllQuery(databaseCollection[i]['collection'])
  selectStationQuery(databaseCollection[i]['collection'])
  selectDateRangeQuery(databaseCollection[i]['collection'])
  selectGroupByStationQuery(databaseCollection[i]['collection'])

db.close() #關閉連接

