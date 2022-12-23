from pyhive import hive  # or import hive
cursor = hive.connect(host='CT100',port=10000,auth='LDAP',username='root',password='00000', database='default').cursor()
cursor.execute('drop Table earthquakedata1')
cursor.execute('drop Table earthquakedata2')
cursor.execute('drop Table earthquakedata3')
cursor.execute("CREATE TABLE earthquakedata1 (network STRING, station STRING, location STRING, channel STRING, npts STRING, sampling_rate STRING, starttime STRING, delta STRING, calib STRING) row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile")
cursor.execute("load data local inpath '/root/network20.csv' into table earthquakedata1")