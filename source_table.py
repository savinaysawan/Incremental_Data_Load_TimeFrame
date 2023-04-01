# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental load

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create database on loaction

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ssd_spark_db location '/dbfs/FileStore/SSD_01'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data from file to DataFrame 

# COMMAND ----------

read_df=spark.read.option("inferSchema" , True).option("header",True).csv("dbfs:/FileStore/SSD_01/EMPLOYEE")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Display DataFrame content 

# COMMAND ----------

display(read_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Add audit_creation_datetime column into dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

read_df_02=read_df.withColumn("audit_creation_datetime",current_timestamp())

# COMMAND ----------

read_df_02.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### writing data into employee table

# COMMAND ----------

read_df_02.write.format("delta").mode("append").saveAsTable("ssd_spark_db.employee")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update data or Data Transformation if needed 

# COMMAND ----------

# MAGIC %sql
# MAGIC update ssd_spark_db.employee set emp_dept='ENGINEER'
# MAGIC WHERE emp_dept='ENGNEER'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from ssd_spark_db.employee;

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Same for second Table as well

# COMMAND ----------

read_df=spark.read.option("inferSchema" , True).option("header",True).csv("dbfs:/FileStore/SSD_01/DEPARTMENT")
from pyspark.sql.functions import current_timestamp
read_df_02=read_df.withColumn("audit_creation_datetime",current_timestamp())
read_df_02.write.format("delta").mode("append").saveAsTable("ssd_spark_db.department")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ssd_spark_db.department;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table ssd_spark_db.department;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ssd_spark_db.control_table_02 (
# MAGIC     Serial_id int ,
# MAGIC     Table_name varchar(255),
# MAGIC     Status varchar(255),
# MAGIC     Last_Update_date  string
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert one starting  data into control Table

# COMMAND ----------

#%sql
#insert into ssd_spark_db.control_table_02(Serial_id,Table_name,Status,Last_Update_date) values(1,"target_table_x","completed",current_timestamp)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from ssd_spark_db.control_table_02;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ssd_spark_db.control_table_02 SELECT * FROM (
# MAGIC with cte_01 as
# MAGIC (
# MAGIC SELECT MAX(Serial_id) as id_1 from ssd_spark_db.control_table_02
# MAGIC ),
# MAGIC cte_02 as
# MAGIC (
# MAGIC select c.id_1+1 as Serial_id,
# MAGIC 'target_table_x' as Table_name,
# MAGIC 'InProgress' as Status,
# MAGIC current_timestamp as Last_Update_date
# MAGIC from cte_01 c
# MAGIC )
# MAGIC SELECT * FROM cte_02)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ssd_spark_db.control_table_02

# COMMAND ----------

# MAGIC %md
# MAGIC ##Increamtal data processing

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ssd_spark_db.target_x (
# MAGIC     emp_id int ,
# MAGIC     FULL_NAME varchar(255),
# MAGIC     emp_dept varchar(255),
# MAGIC     work_location  varchar(255),
# MAGIC     username varchar(255),
# MAGIC     emp_rating  varchar(255),
# MAGIC     phone_number varchar(255),
# MAGIC     email varchar(255),
# MAGIC     salary int
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ssd_spark_db.target_x_dl (
# MAGIC     emp_id int ,
# MAGIC     FULL_NAME varchar(255),
# MAGIC     emp_dept varchar(255),
# MAGIC     work_location  varchar(255),
# MAGIC     username varchar(255),
# MAGIC     emp_rating  varchar(255),
# MAGIC     phone_number varchar(255),
# MAGIC     email varchar(255),
# MAGIC     salary int
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Loading data into target_table 
# MAGIC %sql
# MAGIC INSERT INTO ssd_spark_db.target_x SELECT * FROM (
# MAGIC with cte_date as 
# MAGIC (
# MAGIC select max(Last_Update_date) as min_date 
# MAGIC from ssd_spark_db.control_table_02
# MAGIC WHERE Status='completed'
# MAGIC ),
# MAGIC CTE_DATA as
# MAGIC (
# MAGIC select 
# MAGIC e1.emp_id AS emp_id,
# MAGIC (e1.First_Name || e1.Last_name) AS FULL_NAME,
# MAGIC e1.emp_dept AS emp_dept,
# MAGIC e1.work_location AS work_location,
# MAGIC d.username AS username,
# MAGIC d.emp_rating AS emp_rating,
# MAGIC d.phone_number AS phone_number,
# MAGIC e1.email AS email,
# MAGIC d.salary AS salary,
# MAGIC row_number () over( partition by e1.emp_id order by e1.audit_creation_datetime desc ) rank_01
# MAGIC from 
# MAGIC ssd_spark_db.employee e1
# MAGIC left outer join 
# MAGIC ssd_spark_db.department d
# MAGIC on 
# MAGIC e1.emp_id =d.emp_id
# MAGIC cross join cte_date c
# MAGIC where e1.audit_creation_datetime > c.min_date
# MAGIC --AND e1.audit_creation_datetime <= current_timestamp OPTIONAL IT WILL HELP WHEN WE WILL DO THE HISTORY LOAD BETWEEN 2 TIMESTAMP 
# MAGIC )
# MAGIC select 
# MAGIC emp_id ,
# MAGIC FULL_NAME ,
# MAGIC emp_dept ,
# MAGIC work_location , 
# MAGIC username ,
# MAGIC emp_rating,
# MAGIC phone_number,   
# MAGIC email,
# MAGIC salary 
# MAGIC from CTE_DATA
# MAGIC WHERE rank_01=1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ssd_spark_db.target_x

# COMMAND ----------

# MAGIC %md
# MAGIC ##inserting remaining data into Target_dl table(Droped Data or Duplicate Data or Unsatified Data)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ssd_spark_db.target_x_dl SELECT * FROM (
# MAGIC select 
# MAGIC emp_id ,
# MAGIC FULL_NAME ,
# MAGIC emp_dept ,
# MAGIC work_location , 
# MAGIC username ,
# MAGIC emp_rating,
# MAGIC phone_number,   
# MAGIC email,
# MAGIC salary 
# MAGIC from CTE_DATA
# MAGIC WHERE rank_01 !=1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ssd_spark_db.target_x_dl

# COMMAND ----------

# MAGIC %md
# MAGIC ### Updting control tabble 

# COMMAND ----------

# MAGIC %sql
# MAGIC update ssd_spark_db.control_table_02
# MAGIC set Status="completed", Last_Update_date =current_timestamp
# MAGIC where Serial_id IN (select MAX(Serial_id) from ssd_spark_db.control_table_02)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * FROM ssd_spark_db.control_table_02

# COMMAND ----------


