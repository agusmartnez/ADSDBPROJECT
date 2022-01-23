import formatted_notebook, trusted_notebook, exploitation_notebook, localdb_conn, feature_generation_notebook_1, \
    feature_generation_notebook_2, prepare_folders


if __name__ == '__main__':
    prepare_folders.check_create_folders()
    dbname1 = 'postgres'
    localdb_conn.create_tables1(dbname1)
    localdb_conn.fill_tables1(dbname1)
    formatted_notebook.import_tables_fromdb()
    trusted_notebook.trusted_part()
    exploitation_notebook.exploitation_zone()
    dbname2 = 'adsdb_analysis'
    localdb_conn.create_tables2(dbname2)
    localdb_conn.fill_tables2(dbname2)
    conn = localdb_conn.create_connection(dbname2)
    df1, df2, df3 = feature_generation_notebook_1.import_from_localdb(conn)
    feature_generation_notebook_1.feature_selection_and_save(df1, df2, df3)

    #ANALYSIS
    feature_generation_notebook_2.analysis_table_education()
    feature_generation_notebook_2.analysis_table_zone_by_areas()
    df_zone = feature_generation_notebook_2.analysis_table_zone_by_quarters()
    feature_generation_notebook_2.analysis_table_barcelona()

    #MODELS
    feature_generation_notebook_2.model_table_zone(df_zone)


