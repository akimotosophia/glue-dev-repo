import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1722770758259 = glueContext.create_dynamic_frame.from_catalog(database="gluecatalog-dev", table_name="akimoto_catalog_test", transformation_ctx="AWSGlueDataCatalog_node1722770758259")

# Script generated for node Amazon Redshift
AmazonRedshift_node1722770794667 = glueContext.write_dynamic_frame.from_options(frame=AWSGlueDataCatalog_node1722770758259, connection_type="redshift", connection_options={"postactions": "BEGIN; DELETE FROM public.test_table; insert into public.test_table select * from public.test_table_work; drop table if exists public.test_table_work;  END;", "redshiftTmpDir": "s3://aws-glue-assets-450347635113-ap-northeast-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.test_table_work", "connectionName": "Redshift connection", "preactions": "DROP TABLE IF EXISTS public.test_table_work; CREATE TABLE public.test_table_work (LIKE public.test_table) backup no; commit;"}, transformation_ctx="AmazonRedshift_node1722770794667")

job.commit()