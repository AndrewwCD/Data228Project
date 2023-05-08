import snowflake.connector

# Replace with your own Snowflake login credentials
conn = snowflake.connector.connect(
    user='andrewdunton',
    password= ,#password removed for report
    account= ,#account removed for report
    warehouse= 'DATA228_WH',
    database= 'WAGES_COL',
    schema='PUBLIC'
)

cursor = conn.cursor()

stages = [
    ('cost_of_living_item_stage', 's3://data228projectdata/Transformed Data/cost_of_living_item/'),
    ('cost_of_living_price_stage', 's3://data228projectdata/Transformed Data/cost_of_living_price/'),
    ('oe_area_stage', 's3://data228projectdata/Transformed Data/oe_area/'),
    ('oe_areatype_stage', 's3://data228projectdata/Transformed Data/oe_areatype/'),
    ('oe_datatype_stage', 's3://data228projectdata/Transformed Data/oe_datatype/'),
    ('oe_footnote_stage', 's3://data228projectdata/Transformed Data/oe_footnote/'),
    ('oe_industry_stage', 's3://data228projectdata/Transformed Data/oe_industry/'),
    ('oe_occupation_stage', 's3://data228projectdata/Transformed Data/oe_occupation/'),
    ('oe_seasonal_stage', 's3://data228projectdata/Transformed Data/oe_seasonal/'),
    ('oe_sector_stage', 's3://data228projectdata/Transformed Data/oe_sector/'),
    ('oe_series_stage', 's3://data228projectdata/Transformed Data/oe_series/')
]

aws_key_id = ''#access key removed for report
aws_secret_key = ''

for stage_name, url in stages:
    create_stage_sql = (f"CREATE STAGE {stage_name} URL='{url}' CREDENTIALS=(AWS_KEY_ID='{aws_key_id}' \
                        AWS_SECRET_KEY='{aws_secret_key}') FILE_FORMAT=(TYPE='PARQUET')")
    cursor.execute(create_stage_sql)
    print(f"{stage_name} stage created successfully")

cursor.close()
conn.close()
