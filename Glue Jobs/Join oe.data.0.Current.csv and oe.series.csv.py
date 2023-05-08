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

# Script generated for node S3 oe.data.0.Current.csv
S3oedata0Currentcsv_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://data228projectdata/Raw Data/oe.data.0.Current.csv"]
    },
    transformation_ctx="S3oedata0Currentcsv_node1",
)

# Script generated for node S3 oe.series.csv
S3oeseriescsv_node1682544002707 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://data228projectdata/Raw Data/oe.series.csv"]},
    transformation_ctx="S3oeseriescsv_node1682544002707",
)

# Script generated for node Join
Join_node1682544008247 = Join.apply(
    frame1=S3oedata0Currentcsv_node1,
    frame2=S3oeseriescsv_node1682544002707,
    keys1=["series_id"],
    keys2=["series_id"],
    transformation_ctx="Join_node1682544008247",
)

# Script generated for node oe.series.parquet
oeseriesparquet_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1682544008247,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://data228projectdata/Transformed Data/oe_series/",
        "partitionKeys": [],
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="oeseriesparquet_node3",
)

job.commit()
