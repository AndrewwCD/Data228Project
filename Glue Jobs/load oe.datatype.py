import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 oe.datatype.csv
S3oedatatypecsv_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://data228projectdata/Raw Data/oe.datatype.csv"]},
    transformation_ctx="S3oedatatypecsv_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3oedatatypecsv_node1,
    mappings=[
        ("datatype_code", "string", "datatype_code", "int"),
        ("datatype_name", "string", "datatype_name", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 oe.datatype.parquet
S3oedatatypeparquet_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://data228projectdata/Transformed Data/oe_datatype/",
        "partitionKeys": [],
    },
    transformation_ctx="S3oedatatypeparquet_node3",
)

job.commit()
