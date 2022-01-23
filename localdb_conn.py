import csv
import psycopg2
from psycopg2 import Error


def create_connection(dbname):
    if dbname == 'adsdb_analysis':
        conn = psycopg2.connect(user="adsdb",
                                password="adsdb",
                                host="localhost",
                                port="5432",
                                database="adsdb_analysis")
    if dbname == 'postgres':
        conn = psycopg2.connect(user="adsdb",
                                password="adsdb",
                                host="localhost",
                                port="5432",
                                database="postgres")

    return conn


def create_tables1(dbname):
    try:
        conn = create_connection(dbname)
        cur = conn.cursor()
        cur.execute("""CREATE TABLE atur_per_sexe_csv_2011(
            year int4,
            mes int4,
            codi_districte varchar,
            nom_districte varchar,
            codi_barri varchar,
            nom_barri varchar,
            sexe varchar,
            demanda_ocupacio varchar,
            nombre float4)
        """)
        cur.execute("""CREATE TABLE atur_per_sexe_csv_2012(
            year integer,
            mes int4,
            codi_districte varchar,
            nom_districte varchar,
            codi_barri varchar,
            nom_barri varchar,
            sexe varchar,
            demanda_ocupacio varchar,
            nombre float4)
        """)
        cur.execute("""CREATE TABLE atur_per_sexe_csv_2013(
            year integer,
            mes int4,
            codi_districte varchar,
            nom_districte varchar,
            codi_barri varchar,
            nom_barri varchar,
            sexe varchar,
            demanda_ocupacio varchar,
            nombre float4)
        """)
        cur.execute("""CREATE TABLE atur_per_sexe_csv_2014(
            year integer,
            mes int4,
            codi_districte varchar,
            nom_districte varchar,
            codi_barri varchar,
            nom_barri varchar,
            sexe varchar,
            demanda_ocupacio varchar,
            nombre float4)
        """)
        cur.execute("""CREATE TABLE atur_per_sexe_csv_2015(
            year integer,
            mes int4,
            codi_districte varchar,
            nom_districte varchar,
            codi_barri varchar,
            nom_barri varchar,
            sexe varchar,
            demanda_ocupacio varchar,
            nombre float4)
        """)
        cur.execute("""CREATE TABLE atur_per_sexe_csv_2016(
            year integer,
            mes int4,
            codi_districte varchar,
            nom_districte varchar,
            codi_barri varchar,
            nom_barri varchar,
            sexe varchar,
            demanda_ocupacio varchar,
            nombre float4)
        """)
        cur.execute("""CREATE TABLE atur_per_sexe_csv_2017(
            year integer,
            mes int4,
            codi_districte varchar,
            nom_districte varchar,
            codi_barri varchar,
            nom_barri varchar,
            sexe varchar,
            demanda_ocupacio varchar,
            nombre float4)
        """)
        cur.execute("""CREATE TABLE atur_per_sexe_csv_2018(
            year integer,
            mes int4,
            codi_districte varchar,
            nom_districte varchar,
            codi_barri varchar,
            nom_barri varchar,
            sexe varchar,
            demanda_ocupacio varchar,
            nombre float4)
        """)
        cur.execute("""CREATE TABLE atur_per_sexe_csv_2019(
            year integer,
            mes int4,
            codi_districte varchar,
            nom_districte varchar,
            codi_barri varchar,
            nom_barri varchar,
            sexe varchar,
            demanda_ocupacio varchar,
            nombre float4)
        """)
        cur.execute("""CREATE TABLE atur_per_sexe_csv_2020(
            year integer,
            mes int4,
            codi_districte varchar,
            nom_districte varchar,
            codi_barri varchar,
            nom_barri varchar,
            sexe varchar,
            demanda_ocupacio varchar,
            nombre float4)
        """)
        cur.execute("""CREATE TABLE atur_per_sexe_csv_2021(
            year integer,
            mes int4,
            codi_districte varchar,
            nom_districte varchar,
            codi_barri varchar,
            nom_barri varchar,
            sexe varchar,
            demanda_ocupacio varchar,
            nombre float4)
        """)
        cur.execute("""CREATE TABLE export_ine_csv(
            sexo varchar,
            ccaa varchar,
            edad varchar,
            periodo varchar,
            total varchar)
        """)
        cur.execute("""CREATE TABLE export_ine_education_csv(
            sexo varchar,
            ccaa varchar,
            nivel_educacion varchar,
            periodo int4,
            total varchar)
        """)
        conn.commit()

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)


def fill_tables1(dbname):
    try:
        conn = create_connection(dbname)
        cur = conn.cursor()
        fbcn = ['temporal/2011_atur_per_sexe.csv', 'temporal/2012_atur_per_sexe.csv', 'temporal/2013_atur_per_sexe.csv',
                'temporal/2014_atur_per_sexe.csv', 'temporal/2015_atur_per_sexe.csv', 'temporal/2016_atur_per_sexe.csv',
                'temporal/2017_atur_per_sexe.csv', 'temporal/2018_atur_per_sexe.csv', 'temporal/2019_atur_per_sexe.csv',
                'temporal/2020_atur_per_sexe.csv', 'temporal/2021_atur_per_sexe.csv']
        f_ine = 'temporal/export_ine.csv'
        f_ine_educ = 'temporal/export_ine_education.csv'

        with open(fbcn[0], 'r') as f:
            reader = csv.reader(f)
            next(reader) # Skip the header row.
            for row in reader:
                cur.execute("INSERT INTO atur_per_sexe_csv_2011 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", row)
        conn.commit()
        with open(fbcn[1], 'r') as f:
            reader = csv.reader(f)
            next(reader) # Skip the header row.
            for row in reader:
                cur.execute("INSERT INTO atur_per_sexe_csv_2012 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", row)
        conn.commit()
        with open(fbcn[2], 'r') as f:
            reader = csv.reader(f)
            next(reader) # Skip the header row.
            for row in reader:
                cur.execute("INSERT INTO atur_per_sexe_csv_2013 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", row)
        conn.commit()
        with open(fbcn[3], 'r') as f:
            reader = csv.reader(f)
            next(reader) # Skip the header row.
            for row in reader:
                cur.execute("INSERT INTO atur_per_sexe_csv_2014 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", row)
        conn.commit()
        with open(fbcn[4], 'r') as f:
            reader = csv.reader(f)
            next(reader) # Skip the header row.
            for row in reader:
                cur.execute("INSERT INTO atur_per_sexe_csv_2015 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", row)
        conn.commit()
        with open(fbcn[5], 'r') as f:
            reader = csv.reader(f)
            next(reader) # Skip the header row.
            for row in reader:
                cur.execute("INSERT INTO atur_per_sexe_csv_2016 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", row)
        conn.commit()
        with open(fbcn[6], 'r') as f:
            reader = csv.reader(f)
            next(reader) # Skip the header row.
            for row in reader:
                cur.execute("INSERT INTO atur_per_sexe_csv_2017 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", row)
        conn.commit()
        with open(fbcn[7], 'r') as f:
            reader = csv.reader(f)
            next(reader) # Skip the header row.
            for row in reader:
                cur.execute("INSERT INTO atur_per_sexe_csv_2018 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", row)
        conn.commit()
        with open(fbcn[8], 'r') as f:
            reader = csv.reader(f)
            next(reader) # Skip the header row.
            for row in reader:
                cur.execute("INSERT INTO atur_per_sexe_csv_2019 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", row)
        conn.commit()
        with open(fbcn[9], 'r') as f:
            reader = csv.reader(f)
            next(reader) # Skip the header row.
            for row in reader:
                cur.execute("INSERT INTO atur_per_sexe_csv_2020 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", row)
        conn.commit()
        with open(fbcn[10], 'r') as f:
            reader = csv.reader(f)
            next(reader) # Skip the header row.
            for row in reader:
                cur.execute("INSERT INTO atur_per_sexe_csv_2021 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", row)
        conn.commit()
        with open(f_ine, 'r') as f:
            reader = csv.reader(f, delimiter=';')
            next(reader) # Skip the header row.
            for row in reader:
                cur.execute("INSERT INTO export_ine_csv VALUES (%s, %s, %s, %s, %s)", row)
        conn.commit()
        with open(f_ine_educ, 'r') as f:
            reader = csv.reader(f, delimiter=';')
            next(reader) # Skip the header row.
            for row in reader:
                cur.execute("INSERT INTO export_ine_education_csv VALUES (%s, %s, %s, %s, %s)", row)
        conn.commit()

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if conn:
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")

def create_tables2(dbname):
    try:
        conn = create_connection(dbname)
        cur = conn.cursor()
        cur.execute("""CREATE TABLE df_atur_education_exploitation(
            column1 integer,
            Year int4,
            ccaa varchar,
            gender varchar,
            education_sector varchar,
            rate float4,
            Total float4)
        """)
        cur.execute("""CREATE TABLE df_atur_barcelona_exploitation(
            column1 integer,
            Year int4,
            Month int4,
            code_district int4,
            name_district varchar,
            code_neighborhood int4,
            name_neighborhood varchar,
            gender varchar,
            demmand_occupancy varchar,
            Total int4)
        """)
        cur.execute("""CREATE TABLE df_atur_zone_exploitation(
            column1 integer,
            Year int4,
            quarter int4,
            Zone varchar,
            gender varchar,
            total float4)
        """)
        conn.commit()
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)


def fill_tables2(dbname):
    conn = []
    try:
        conn = create_connection(dbname)
        cur = conn.cursor()
        file_educ = 'exploitation_zone/df_atur_education_exploitation.csv'
        file_bcn = 'exploitation_zone/df_atur_barcelona_exploitation.csv'
        file_zone = 'exploitation_zone/df_atur_zone_exploitation.csv'
        with open(file_educ, 'r') as f:
            reader_educ = csv.reader(f)
            next(reader_educ) # Skip the header row.
            for row in reader_educ:
                cur.execute("INSERT INTO df_atur_education_exploitation VALUES (%s, %s, %s, %s, %s, %s, %s)", row)

        with open(file_bcn, 'r') as f:
            reader_bcn = csv.reader(f)
            next(reader_bcn) # Skip the header row.
            for row in reader_bcn:
                cur.execute("INSERT INTO df_atur_barcelona_exploitation VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row)

        with open(file_zone, 'r') as f:
            reader_zone = csv.reader(f)
            next(reader_zone) # Skip the header row.
            for row in reader_zone:
                cur.execute("INSERT INTO df_atur_zone_exploitation VALUES (%s, %s, %s, %s, %s, %s)", row)

        conn.commit()
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if conn:
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")
