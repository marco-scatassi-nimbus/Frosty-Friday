# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session: snowpark.Session): 
    tableName = 'db_ff.week50.F_F_50'
    
    # calling a sql query
    dataframe = session.sql(f"select * from {tableName} where last_name = 'Deery'")

    # using snowpark methods
    dataframe = session.table(tableName).filter(col('last_name') == 'Deery')
    
    return dataframe
