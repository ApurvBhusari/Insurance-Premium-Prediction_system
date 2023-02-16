import os
import csv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
cloud_config= {
  'secure_connect_bundle':'/config/workspace/secure-connect-insurance-premium.zip'
}
auth_provider = PlainTextAuthProvider('vsiPyZoYebTquFrtbBRnWUNI', 'PBGummdEBwZfEPoQjJGdU7DAx3fD-XnOW5c4iyxsNdx8MuTC_AkX2f56M2u2xGnO2XY2zYN9JH9TWOD9sJCw_b6yZkIFJ7-KkKn-mtrkgZZFvFpT3Z9ReIK7_fBX82N3')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
row = session.execute("select release_version from system.local")
if row:
    print(row[0])
else:
    print('An error occurred')

DATA_FILE_PATH="insurance.csv"
KEYSPACE_NAME="ineuron"
TABLE_NAME="expenses"

if __name__=="__main__":
    row = session.execute(f"CREATE TABLE IF NOT EXISTS {KEYSPACE_NAME}.{TABLE_NAME} (age int PRIMARY KEY,sex text,bmi float,children int,smoker text,region text,expenses float);")
    with open(DATA_FILE_PATH,'r') as data:
        next(data)
        data_csv= csv.reader(data,delimiter=',')
        #csv reader object
        print(data_csv)
        all_value= []
        for i in data_csv:
            session.execute(f"insert into {KEYSPACE_NAME}.{TABLE_NAME}(age,sex ,bmi,children,smoker,region,expenses)values(%s,%s,%s,%s,%s,%s,%s)",[int(i[0]),i[1],float(i[2]),int(i[3]),i[4],i[5],float(i[6])])
        print('data_dump_to_cassandra')    
    
    



