#  FEATURE GENERATION ZONE


import psycopg2
from psycopg2 import Error
import pandas as pd


def import_from_localdb(conn):
    ## Get the data from the database in PostgreSQL
    try:
        cursor = conn.cursor()
        cursor.execute("select * from df_atur_barcelona_exploitation")
        record = cursor.fetchall()
        df_atur_barcelona_exploitation = pd.DataFrame(record, columns=['column1','year', 'month', 'code_district', 'name_district', 'code_neighborhood', 'name_neighborhood', 'gender', 'demmand_occupancy', 'total'])

        cursor.execute("select * from df_atur_education_exploitation")
        record = cursor.fetchall()
        df_atur_education_exploitation = pd.DataFrame(record, columns=['column1', 'Year', 'ccaa', 'gender', 'education_sector', 'rate', 'total'])

        cursor.execute("select * from df_atur_zone_exploitation")
        record = cursor.fetchall()
        df_atur_zone_exploitation = pd.DataFrame(record, columns=['column1', 'year', 'quarter', 'zone', 'gender', 'total'])

    except(Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")
    return df_atur_barcelona_exploitation, df_atur_education_exploitation, df_atur_zone_exploitation


def feature_selection_and_save(df_bcn, df_educ, df_zone):
    """### Head of the data sets"""

    # removing features not needed for the analysis
    df_atur_barcelona_exploitation_csv = df_bcn.drop(columns=['column1', 'code_district', 'code_neighborhood', 'month', 'name_district', 'gender'])

    #remove column1
    df_atur_education_exploitation_csv = df_educ.drop(columns=['column1'])

    #remove column1
    df_atur_zone_exploitation_csv = df_zone.drop(columns=['column1', 'gender'])

    ## Save the results
    df_atur_barcelona_exploitation_csv.to_csv('featgen_zone/df_atur_barcelona_featgen.csv')
    df_atur_education_exploitation_csv.to_csv('featgen_zone/df_atur_education_featgen.csv')
    df_atur_zone_exploitation_csv.to_csv('featgen_zone/df_atur_zone_featgen.csv')
