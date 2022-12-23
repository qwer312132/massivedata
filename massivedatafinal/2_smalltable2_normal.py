from pyhive import hive  # or import hive
cursor = hive.connect(host='CT100',port=10000,auth='LDAP',username='root',password='00000', database='default').cursor()
cursor.execute("CREATE TABLE earthquakedata2(network STRING, station STRING, location STRING, channel STRING, npts STRING, sampling_rate STRING, starttime STRING, delta STRING, calib STRING) row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile")
# 這邊有兩種 insert table 的方式，請同學以自己目前要測的內容取消註解 "#" 來執行 hive 指令
import time
start = time.time()
# 一般 table 插 一般 table
cursor.execute("insert into table earthquakedata2 select * from earthquakedata1")

# 一般 table 插 Partition table
#cursor.execute("insert into table earthquakedata2 partition (count=1) select network , station , location , channel , npts , sampling_rate , starttime , delta , calib from earthquakedata1")

# 插入 earthquakedata1 到 earthquakedata2
end=time.time()
end=end-start
print("insert normal table:50000",end)
start = time.time()
cursor.execute("select * from earthquake2")
end=time.time-start
print("query normal table:50000",end)