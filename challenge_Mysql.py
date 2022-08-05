import mysql.connector as conn
import logging
import pandas as pd
import pymongo
import json

logging.basicConfig(filename='assignement_sql.test',level=10,format='%(asctime)s %(name)s %(levelname)s %(message)s')
try:

    mydb=conn.connect(host='localhost',user='root',password='Vijay@248621',allow_local_infile=True)

    cursor=mydb.cursor(buffered=True)
#1.Create a table attribute dataset and dress dataset through python
    cursor.execute("use  ineuron")
    cursor.execute("create table if not exists attributeDataSet(Dress_ID int(10),Style varchar(10),Price varchar(20),Rating int(10),Size varchar(20),Season varchar(20),NeckLine varchar(20),SleeveLength	varchar(20),waiseline varchar(20),Material varchar(20),FabricType	varchar(20),Decoration varchar(20),Pattern_Type	varchar(20),Recommendation int(10))")
    logging.info("Table:attributeDataSet created")

    cursor.execute("create table if not exists salesDataSet(Dress_ID int,29_8_2013 int,31_8_2013 int,09_02_2013 int,09_04_2013 int,09_06_2013 int,09_08_2013 int,09_10_2013 int,09_12_2013 int,14_9_2013 int,16_9_2013 int,18_9_2013 int,20_9_2013 int,22_9_2013 int,24_9_2013 int,26_9_2013 int,28_9_2013 int,30_9_2013 int,10_02_2013 int,10_04_2013 int,10_06_2013 int,10_08_2013 int,10_10_2013 int,10_12_2013 int)")
    logging.info("Table:salesDataSet created")

#2.bulk upload for these table for respective databases
    cursor.execute("LOAD DATA LOCAL INFILE 'C:/Users/vijay/Attribute_DataSet.csv' INTO TABLE attributeDataSet Fields Terminated by ',' Lines Terminated by '\r\n' ignore 1 rows")
    logging.info("Data is successfully loaded from local storage to table:attributeDataSet")

    cursor.execute("Load Data local infile 'C:/Users/vijay/Downloads/data fsds -20220724T095114Z-001/data fsds/Dress Sales.csv' INTO TABLE salesDataSet Fields terminated by ',' Lines Terminated by '\r\n' ignore 1 rows")
    logging.info("Data is successfully loaded from local storage to table:salesDataSet")
#3.read these dataset in pandas as a dataframe
    cursor.execute("select * from attributeDataSet")
    df_attDS=pd.DataFrame(cursor.fetchall(),columns=['Dress_ID','Style','Price','Rating','Size','Season','NeckLine','SleeveLength','waiseline','Material','FabricType','Decoration','Pattern Type','Recommendation'])
    logging.info("Data is assigned to DF from table attributeDataSet")
    cursor.execute("select * from salesDataSet")
    df_salesDS=pd.DataFrame(cursor.fetchall(),columns=['Dress_ID','29/8/2013','31/8/2013','09-02-2013','09-04-2013','09-06-2013','09-08-2013','09-10-2013','09-12-2013','14/9/2013','16/9/2013','18/9/2013','20/9/2013','22/9/2013','24/9/2013','26/9/2013','28/9/2013','30/9/2013','10-02-2013','10-04-2013','10-06-2013','10-08-2010','10-10-2013','10-12-2013'])
    logging.info("Data is assigned to DF from table salesDataSet")
#4.convert attribute dataset in json format
    df_attDS.to_json("attributeDataSet.json")
    df_salesDS.to_json("df_salesDS.json")
    #5.store this dataset into mongodb at one go(insert_many)
    client = pymongo.MongoClient("mongodb+srv://vijaysankar117:Vijay_248621@vijcluster0.0phlp.mongodb.net/?retryWrites=true&w=majority")

    db1=client['DressSalesData']
    print(db1)
    logging.info("DressSalesData Database created successfully")
    Attrcoll=db1['AttributeDataset']
    logging.info("Attrcoll collection created successfully")
    DressSalesColl=db1['DressSales']

    Attrcoll.drop()
    DressSalesColl.drop()

    with open("attributeDataSet.json") as file:
        file_Attr=json.load(file)
    Attrcoll.insert_one(file_Attr)
    logging.info("Successfully inserted the Attribute Data into collection")
    with open("df_salesDS.json") as file:
        file_sales=json.load(file)
        DressSalesColl.insert_one(file_sales)

    logging.info("Successfully inserted the sales data into collection")
#6.in sql tsk try to perform left join operation with attribute dataset on column:dress id left attribute dataset right side:dress sales

    cursor.execute("select * from attributeDataSet a left join salesDataSet b on a.Dress_ID=b.Dress_ID")
    logging.info("attributedataset is left joined with salesdataset successfully")
#7.Write a sql query to find out how many unique dress that we have based on dress id
    cursor.execute("select count(Distinct Dress_ID) from attributeDataSet")
    print(("The No of distinct Dress Ids:%s") % cursor.fetchall()[0])
  #8.How many dress is having recommendation 0
    cursor.execute("select count(*) from attributedataset where recommendation='0'")
    print("No of Dress IDs with recommendation '0'is %s"%cursor.fetchall()[0])
#9.Try to find out total dress sale for each dress id
    df_salesDS['total']=df_salesDS[['29/8/2013','31/8/2013','09-02-2013','09-04-2013','09-06-2013','09-08-2013','09-10-2013','09-12-2013','14/9/2013','16/9/2013','18/9/2013','20/9/2013','22/9/2013','24/9/2013','26/9/2013','28/9/2013','30/9/2013','10-02-2013','10-04-2013','10-06-2013','10-08-2010','10-10-2013','10-12-2013']].agg('sum',axis=1)
    df_salesDS1=df_salesDS.groupby('Dress_ID')['total'].agg('sum')
    logging.info("Total Dress Salaes for each dress Id given")
    print(df_salesDS1)

except Exception as e:
    logging.error(e)

