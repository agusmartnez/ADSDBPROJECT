# FORMATTED ZONE
## Get the data from the database in PostgreSQL"""

import psycopg2
from psycopg2 import Error
import pandas as pd
from localdb_conn import create_connection


def import_tables_fromdb():

    dataframe_barcelona = ["\"atur_per_sexe_csv_2011\"", "\"atur_per_sexe_csv_2012\"", "\"atur_per_sexe_csv_2013\"",
                           "\"atur_per_sexe_csv_2014\"", "\"atur_per_sexe_csv_2015\"", "\"atur_per_sexe_csv_2016\"",
                           "\"atur_per_sexe_csv_2017\"", "\"atur_per_sexe_csv_2018\"", "\"atur_per_sexe_csv_2019\"",
                           "\"atur_per_sexe_csv_2020\"", "\"atur_per_sexe_csv_2021\""]

    try:
        connection = create_connection("postgres")
        cursor = connection.cursor()
        for i in dataframe_barcelona:
          if i==dataframe_barcelona[0]:

            query = "select * from" + i
            cursor.execute(query)

            record = cursor.fetchall()
            df_atur_per_sexe = pd.DataFrame(record, columns=['Year', 'Month', 'Code_District',
                                       'Name_Districte', 'Code_Neighborhood', 'Name_Neighborhood',
                                       'Gender', 'Demmand_occupancy', 'Number'])

          else:
            df1 = df_atur_per_sexe
            query = "select * from" + i
            cursor.execute(query)
            record = cursor.fetchall()
            df_atur_per_sexe = pd.DataFrame(record, columns=['Year', 'Month', 'Code_District',
                                       'Name_Districte', 'Code_Neighborhood', 'Name_Neighborhood',
                                       'Gender', 'Demmand_occupancy', 'Number'])

            df_atur_per_sexe = pd.concat([df_atur_per_sexe, df1])

        cursor.execute("select * from export_ine_csv")
        record = cursor.fetchall()
        df_export_ine = pd.DataFrame(record, columns=['Gender', 'CCAA', 'Age', 'Quarter', 'Total'])

        cursor.execute("select * from export_ine_education_csv")
        record = cursor.fetchall()
        df_export_ine_education = pd.DataFrame(record, columns=['Gender', 'CCAA', 'Education_Sector', 'Year', 'Rate'])

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


    ## Save the results
    #We save the results in our local computer due to we can not connect this notebook to Google Drive. This datasets will be uploaded to the landing folder, into the `1. output formatted zone` folder.

    df_atur_per_sexe.to_csv('formatted_zone/atur_per_sexe_formatted.csv', encoding='utf-8')
    df_export_ine.to_csv('formatted_zone/export_ine_formatted.csv', encoding='utf-8')
    df_export_ine_education.to_csv('formatted_zone/df_export_ine_education_formatted.csv', encoding='utf-8')
