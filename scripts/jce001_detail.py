import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1723188961639 = glueContext.create_dynamic_frame.from_catalog(database="akimoto-finance-db", table_name="if001", transformation_ctx="AWSGlueDataCatalog_node1723188961639")

# Script generated for node SQL Query
SqlQuery448 = '''
select 
    CAST(`計算対象` AS INT) as calculation_flag,
    to_date(`日付`, 'yyyy/MM/dd') as date,
    CAST(`内容` AS VARCHAR(4096)) as description_text,
    CAST(`金額（円）` AS NUMERIC(10,2)) as amount,
    CAST(`保有金融機関` AS VARCHAR(256)) as account_name,
    CAST(`大項目` AS VARCHAR(256)) as major_category_name,
    CAST(`中項目` AS VARCHAR(256)) as minor_category_name,
    CAST(`メモ` AS VARCHAR(4096)) as memo_text,
    CAST(`振替` AS INT) as transfer_flag,
    CAST(`id` AS VARCHAR(256)) as id
from import_detail;
'''
SQLQuery_node1723191937178 = sparkSqlQuery(glueContext, query = SqlQuery448, mapping = {"import_detail":AWSGlueDataCatalog_node1723188961639}, transformation_ctx = "SQLQuery_node1723191937178")


postactions = '''
    BEGIN; 
    MERGE INTO conformed.tce001_detail 
    USING conformed.tce001_detail_temp_qvrrbg 
    ON tce001_detail.id = tce001_detail_temp_qvrrbg.id 
    WHEN MATCHED THEN UPDATE SET calculation_flag = tce001_detail_temp_qvrrbg.calculation_flag, 
                                 date = tce001_detail_temp_qvrrbg.date, 
                                 description_text = tce001_detail_temp_qvrrbg.description_text, 
                                 amount = tce001_detail_temp_qvrrbg.amount, 
                                 account_name = tce001_detail_temp_qvrrbg.account_name, 
                                 major_category_name = tce001_detail_temp_qvrrbg.major_category_name, 
                                 minor_category_name = tce001_detail_temp_qvrrbg.minor_category_name, 
                                 memo_text = tce001_detail_temp_qvrrbg.memo_text, 
                                 transfer_flag = tce001_detail_temp_qvrrbg.transfer_flag, 
                                 id = tce001_detail_temp_qvrrbg.id 
    WHEN NOT MATCHED THEN INSERT VALUES (tce001_detail_temp_qvrrbg.calculation_flag, 
                                         tce001_detail_temp_qvrrbg.date, 
                                         tce001_detail_temp_qvrrbg.description_text, 
                                         tce001_detail_temp_qvrrbg.amount, 
                                         tce001_detail_temp_qvrrbg.account_name, 
                                         tce001_detail_temp_qvrrbg.major_category_name, 
                                         tce001_detail_temp_qvrrbg.minor_category_name, 
                                         tce001_detail_temp_qvrrbg.memo_text, 
                                         tce001_detail_temp_qvrrbg.transfer_flag, 
                                         tce001_detail_temp_qvrrbg.id); 
    DROP TABLE conformed.tce001_detail_temp_qvrrbg; 
    END;
'''

preactions = '''
    CREATE TABLE IF NOT EXISTS conformed.tce001_detail (calculation_flag INTEGER, date DATE, description_text VARCHAR(4096), amount DECIMAL, account_name VARCHAR, major_category_name VARCHAR, minor_category_name VARCHAR, memo_text VARCHAR(4096), transfer_flag INTEGER, id VARCHAR);
    DROP TABLE IF EXISTS conformed.tce001_detail_temp_qvrrbg; 
    CREATE TABLE conformed.tce001_detail_temp_qvrrbg (calculation_flag INTEGER, date DATE, description_text VARCHAR(4096), amount DECIMAL, account_name VARCHAR, major_category_name VARCHAR, minor_category_name VARCHAR, memo_text VARCHAR(4096), transfer_flag INTEGER, id VARCHAR);
'''

# Script generated for node Amazon Redshift
AmazonRedshift_node1723189862743 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1723191937178, connection_type="redshift", connection_options={"postactions": postactions, "redshiftTmpDir": "s3://aws-glue-assets-450347635113-ap-northeast-1/temporary/", "useConnectionProperties": "true", "dbtable": "conformed.tce001_detail_temp_qvrrbg", "connectionName": "Redshift connection", "preactions": preactions}, transformation_ctx="AmazonRedshift_node1723189862743")

job.commit()