from pyhive import hive  # or import hive
cursor = hive.connect(host='CT100',port=10000,auth='LDAP',username='root',password='00000', database='default').cursor()
cursor.execute("CREATE TABLE earthquakedata3(network STRING, station STRING, location STRING, channel STRING, npts STRING, sampling_rate STRING, starttime STRING, delta STRING, calib STRING) partitioned by (count INT) CLUSTERED BY(station) SORTED BY(station) INTO 11 BUCKETS row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile")
cursor.execute("set hive.enforce.bucketing = true")
# 命令強制執行 bucketing
import time
start = time.time()
cursor.execute("from earthquakedata2 insert overwrite table earthquakedata3 PARTITION (count=1) select network , station , location , channel , npts , sampling_rate , starttime , delta , calib WHERE count=1")
# 插入 earthquakedata2 到 earthquakedata3
end=time.time()
end=end-start
print("insert bucket table:50000",end)
start = time.time()
cursor.execute("select * from earthquake2")
end=time.time-start
print("query bucket table:50000",end)