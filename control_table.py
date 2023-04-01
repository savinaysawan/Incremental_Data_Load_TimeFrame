# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE ssd_spark_db.employee;
# MAGIC DROP TABLE ssd_spark_db.department;
# MAGIC DROP TABLE ssd_spark_db.control_table_02;
# MAGIC DROP TABLE ssd_spark_db.target_x;
# MAGIC DROP TABLE ssd_spark_db.target_x_dl;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ssd_spark_db.control_table_02 (
# MAGIC     Serial_id int ,
# MAGIC     Table_name varchar(255),
# MAGIC     Status varchar(255),
# MAGIC     Last_Update_date  string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into ssd_spark_db.control_table_02(Serial_id,Table_name,Status,Last_Update_date) values(1,"target_table_x","completed",current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ssd_spark_db.control_table_02

# COMMAND ----------


