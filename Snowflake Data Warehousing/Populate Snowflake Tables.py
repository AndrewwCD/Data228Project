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

cur = cursor.cursor()

# switch to the WAGES_COL database
cur.execute('USE DATABASE WAGES_COL')

# create file format
cur.execute("""CREATE OR REPLACE FILE FORMAT my_parquet
               TYPE = 'PARQUET'
               NULL_IF = ('NULL', 'null','')
               TRIM_SPACE = TRUE;""")

# show file formats in the WAGES_COL database
cur.execute('SHOW FILE FORMATS IN DATABASE WAGES_COL')

# populate oe_areatype table
cur.execute("""COPY INTO oe_areatype 
               FROM @oe_areatype_stage 
               FILE_FORMAT = my_parquet 
               MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE""")

# populate cost_of_living_item table
cur.execute("""COPY INTO cost_of_living_item 
               FROM @cost_of_living_item_stage 
               FILE_FORMAT = my_parquet 
               MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE""")

# populate oe_industry table
cur.execute("""COPY INTO oe_industry 
               FROM @oe_industry_stage 
               FILE_FORMAT = my_parquet 
               MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE""")

# populate oe_occupation table
cur.execute("""COPY INTO oe_occupation 
               FROM @oe_occupation_stage 
               FILE_FORMAT = my_parquet 
               MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE""")

# populate oe_seasonal table
cur.execute("""COPY INTO oe_seasonal 
               FROM @oe_seasonal_stage 
               FILE_FORMAT = my_parquet 
               MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE""")

# populate oe_sector table
cur.execute("""COPY INTO oe_sector 
               FROM @oe_sector_stage 
               FILE_FORMAT = my_parquet 
               MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE""")

# populate oe_datatype table
cur.execute("""COPY INTO oe_datatype 
               FROM @oe_datatype_stage 
               FILE_FORMAT = my_parquet 
               MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE""")

# populate oe_footnote table
cur.execute("""COPY INTO oe_footnote 
               FROM @oe_footnote_stage 
               FILE_FORMAT = my_parquet 
               MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE""")

cursor.close()
conn.close()
