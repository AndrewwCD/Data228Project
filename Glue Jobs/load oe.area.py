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

# Script generated for node Amazon S3
AmazonS3_node1683417345415 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://data228projectdata/Raw Data/oe.area.csv"]},
    transformation_ctx="AmazonS3_node1683417345415",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=AmazonS3_node1683417345415,
    mappings=[
        ("state_code", "string", "state_code", "string"),
        ("area_code", "string", "area_code", "string"),
        ("areatype_code", "string", "areatype_code", "string"),
        ("area_name", "string", "area_name", "string"),
        ("latitude", "string", "latitude", "string"),
        ("longitude", "string", "longitude", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 oe.area.parquet
S3oeareaparquet_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://data228projectdata/Transformed Data/oe_area/",
        "partitionKeys": [],
    },
    transformation_ctx="S3oeareaparquet_node3",
)

job.commit()
