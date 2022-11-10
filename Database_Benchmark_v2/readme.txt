###檔案說明###
db.txt 			創建新資料庫table (用於MYSQL)
mongodb_test.py	MongoDB Benchmark
mysql_test.py		MySQL Benchmark
05290646.40C		來源資料集


#mongodb _id重複會發生錯誤，所以再次測試時需要把之前的資料刪除
###mongodb刪除資料庫###
在mongo shell下輸入

mongo
use database_name
db.dropDatabase()