# TRUSTED ZONE

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np


def trusted_part():

    df_atur_per_sexe = pd.read_csv("formatted_zone/"
                                   "atur_per_sexe_formatted.csv", index_col=0)
    df_export_ine = pd.read_csv("formatted_zone/"
                                "export_ine_formatted.csv", index_col=0)
    df_export_ine_education = pd.read_csv("formatted_zone/"
                                          "df_export_ine_education_formatted.csv", index_col=0)


    df_export_ine_education = df_export_ine_education.replace(to_replace='..', value=np.nan)

    """## Data profiling
    
    For each table, a profile of each variable is generated to allow data scientists gain insights on the quality of the data. This can be descriptive multivariate analysis methods to explore the available data, assess its quality and gain knowledge about the value of the analysis that can be extracted from there.
    """

    def missing_values(df, column):
      length = len(df[column])
      count = df[column].count()

      number_of_missing_values = length - count
      pct_of_missing_values = float(number_of_missing_values / length)
      pct_of_missing_values = "{0:.1f}%".format(pct_of_missing_values*100)

      return(pct_of_missing_values)

    def show_values_on_bars(axs):
        def _show_on_single_plot(ax):
            for p in ax.patches:
                _x = p.get_x() + p.get_width() / 2
                _y = p.get_y() + p.get_height()
                value = int(p.get_height())
                ax.text(_x, _y, value, ha="center")

        if isinstance(axs, np.ndarray):
            for idx, ax in np.ndenumerate(axs):
                _show_on_single_plot(ax)
        else:
            _show_on_single_plot(axs)

    """### df_atur_per_sexe"""

    df_atur_per_sexe.columns.values

    df_atur_per_sexe.head(n=1)

    df_atur_per_sexe = df_atur_per_sexe. replace({'Atur registrat': 'Atur Registrat', 'Demanda no aturats': 'Demanda No Aturats'})

    df_atur_per_sexe.dtypes

    print('Mising values\n')
    for col in df_atur_per_sexe.columns.values:
      print(col + ': ' + missing_values(df_atur_per_sexe, col))

    columns = ['Year', 'Month', 'Name_Districte',
               'Gender', 'Demmand_occupancy']

    for column in columns:

      plt.figure(figsize=(16,6))
      sns.set_theme(style="whitegrid")
      ax = sns.countplot(x=column, data=df_atur_per_sexe)
      show_values_on_bars(ax)
      plt.show()
      plt.clf()

    """During the years 2011, 2012, 2013 and 2014 we have half the data than in subsequent years, since in 2015 a new category was added to Demand_occupacy called Demada No Aturats.
    
    All the months have the same data length except the month of December 2021 which is not up to date yet.
    
    According to district we can see that the largest have more records (they have more neighborhoods) and that the dataset is balanced in terms of the gender variable since there are the same records for male and female gender.
    """

    df_atur_per_sexe[df_atur_per_sexe['Year']==2011].Demmand_occupancy.unique()

    df_atur_per_sexe[df_atur_per_sexe['Year']==2015].Demmand_occupancy.unique()

    """Let's see some descriptive data about the number of people registered as unemployed:"""

    df_atur_per_sexe['Number'] = [int(x) for x in df_atur_per_sexe['Number']]

    df_atur_per_sexe[df_atur_per_sexe['Demmand_occupancy']=='Atur Registrat'].Number.describe()

    """### df_export_ine"""

    df_export_ine.columns.values

    df_export_ine.head(n=1)

    df_export_ine['Gender'] = df_export_ine['Gender'].astype('category')
    df_export_ine['CCAA'] = df_export_ine['CCAA'].astype('category')

    df_export_ine.dtypes

    print('Mising values\n')
    for col in df_export_ine.columns.values:
      print(col + ': ' + missing_values(df_export_ine, col))

    columns = ['Gender', 'CCAA', 'Age', 'Quarter']

    for column in columns:
      print(column + '\n')
      print(df_export_ine[column].value_counts())

    """All the dataset is balanced.
    
    Let's see some descriptive data about the number of people registered as unemployed:
    """

    df_export_ine['Total'] = [float(x.replace(',', '.')) for x in df_export_ine['Total']]

    df_export_ine.Total.describe()

    """### df_export_ine_education
    
    """

    df_export_ine_education.columns.values

    df_export_ine_education.head(n=1)

    df_export_ine_education.dtypes

    print('Mising values\n')
    for col in df_export_ine_education.columns.values:
      print(col + ': ' + missing_values(df_export_ine_education, col))

    df_export_ine_education = df_export_ine_education.dropna()

    """Since the cells that have a null value in the Rate variable do not provide us with any information, we decided to remove them from the dataset."""

    columns = ['Gender', 'CCAA', 'Education_Sector', 'Year']

    for column in columns:

      print(column + '\n')
      print(df_export_ine_education[column].value_counts())

    """The dataset is almost perfectly balanced.
    
    Let's see some descriptive data about the unemployment rate:
    """

    df_export_ine_education['Rate'] = [float(str(x).replace(',', '.')) for x in df_export_ine_education['Rate']]

    df_export_ine_education.Rate.describe()

    """## Deduplication"""

    df_atur_per_sexe.duplicated()
    df_export_ine.duplicated()
    df_export_ine_education.duplicated()

    df_atur_per_sexe[df_atur_per_sexe.duplicated(keep=False)]
    df_export_ine[df_export_ine.duplicated(keep=False)]
    df_export_ine_education[df_export_ine_education.duplicated(keep=False)]

    """We do not have any duplicate records in any table.
    
    ## Outlier detection
    
    Outliers of the Barcelona dataset taking into account the neighborhood:
    """

    ### Filtered by Name_Neighborhood BARCELONA ###

    df_neighborhood = pd.DataFrame(df_atur_per_sexe, columns = ['Name_Neighborhood', 'Number'])

    neighborhood = list(df_neighborhood.Name_Neighborhood.unique())

    threshold = 3

    out_cumul = []
    idx_cumul = []
    for i in neighborhood:
      outlier2 = []
      index2 = []
      filtered_period = df_neighborhood[df_neighborhood['Name_Neighborhood'] == neighborhood[0]]
      id = df_neighborhood[df_neighborhood['Name_Neighborhood'] == neighborhood[0]].index.tolist()
      mean2 = np.mean(filtered_period['Number'])
      std2 = np.std(filtered_period['Number'])
      for idx, val in enumerate(filtered_period['Number']):
        z = (val-mean2)/std2
        if z > threshold:
            outlier2.append(val)
            out_cumul.append(val)
            index2.append(id[idx])
            idx_cumul.append(id[idx])
      print('\nNeighborhood: ', i)
      print('Values of outliers:')
      print(outlier2, '\n')
      print('Index of the outliers:')
      print(index2, '\n')
      print('Total number of outliers:', len(outlier2))
      print('Mean:', mean2)
      print('Std:', std2, '\n')

    print('\n')
    print('Total number of outliers depending on the year period for the Barcelona dataset: ', len(out_cumul))
    print('Total number of samples:', len(df_neighborhood['Number']))


    ### Deleting rows with outliers:
    df_atur_per_sexe = df_atur_per_sexe.drop(labels=idx_cumul, axis=0)

    """INE dataset outliers taking into account the CCAA:"""

    ### Filtered by Comunities and cities INE ###

    df_city = pd.DataFrame(df_export_ine, columns = ['CCAA', 'Total'])

    cities = list(df_city.CCAA.unique())

    threshold = 3

    out_cumul = []
    idx_cumul = []
    for i in cities:
      outlier1 = []
      index1 = []
      filtered_period = df_city[df_city['CCAA'].str.match(i)]
      id = filtered_period.index.tolist()
      mean1 = np.mean(filtered_period['Total'])
      std1 = np.std(filtered_period['Total'])
      for idx, val in enumerate(filtered_period['Total']):
        z = (val-mean1)/std1
        if z > threshold:
            outlier1.append(val)
            out_cumul.append(val)
            index1.append(id[idx])
            idx_cumul.append(id[idx])
      print('\nCity: ', i)
      print('Values of outliers:')
      print(outlier1, '\n')
      print('Index of the outliers:')
      print(index1, '\n')
      print('Total number of outliers:', len(outlier1))
      print('Mean:', mean1)
      print('Std:', std1, '\n')

    print('\n')
    print('Total number of outliers depending on the comunities and cities for the INE dataset: ', len(out_cumul))
    print('Total number of samples:', len(df_city['Total']))


    ### Deleting rows with outliers:
    df_export_ine = df_export_ine.drop(labels=idx_cumul, axis=0)

    #Boxplot
    sns.set(rc={'figure.figsize':(50,8)})
    ax = sns.boxplot(x="CCAA", y="Total", data=df_city)

    """INE-education dataset outliers filtered by educational level:"""

    ### Filtered by Education Level ###
    df_education_level = pd.DataFrame(df_export_ine_education, columns = ['Education_Sector', 'Rate'])

    education = list(df_education_level.Education_Sector.unique())

    threshold = 3

    out_cumul = []
    idx_cumul = []
    for i in education:
      outlier3 = []
      index3 = []
      filtered_period = df_education_level[df_education_level['Education_Sector'].str.match(i)]
      id = filtered_period.index.tolist()
      mean3 = np.mean(filtered_period['Rate'])
      std3 = np.std(filtered_period['Rate'])
      for idx, val in enumerate(filtered_period['Rate']):
        z = (val-mean3)/std3
        if z > threshold:
            outlier3.append(val)
            out_cumul.append(val)
            index3.append(id[idx])
            idx_cumul.append(id[idx])
      print('\neducation: ', i)
      print('Values of outliers:')
      print(outlier3, '\n')
      print('Index of the outliers:')
      print(index3, '\n')
      print('Total number of outliers:', len(outlier3))
      print('Mean:', mean3)
      print('Std:', std3, '\n')

    print('\n')
    print('Total number of outliers depending on the education level for the INE dataset: ', len(out_cumul))
    print('Total number of samples:', len(df_education_level['Rate']))


    ### Deleting rows with outliers:
    df_export_ine_education = df_export_ine_education.drop(labels=idx_cumul, axis=0)

    """## Other data quality tasks
    
    #### Convert all required variables to categorical:
    """

    columns_df_atur_per_sexe = ['Year', 'Month', 'Code_District', 'Name_Districte',
           'Code_Neighborhood', 'Name_Neighborhood', 'Gender',
           'Demmand_occupancy']

    for col in columns_df_atur_per_sexe:
      df_atur_per_sexe[col] = df_atur_per_sexe[col].astype('category')

    columns_df_export_ine = ['Gender', 'CCAA', 'Age', 'Quarter']

    for col in columns_df_export_ine:
      df_export_ine[col] = df_export_ine[col].astype('category')

    columns_df_export_ine_education = ['Gender', 'CCAA', 'Education_Sector', 'Year']

    for col in columns_df_export_ine_education:
      df_export_ine_education[col] = df_export_ine_education[col].astype('category')

    """#### Remove column index of 'CCAA' from the two INE datasets:"""

    df_export_ine['CCAA'] = df_export_ine['CCAA'].str[3:]
    df_export_ine_education['CCAA'] = df_export_ine_education['CCAA'].str[3:]
    df_export_ine_education['Education_Sector'] = df_export_ine_education['Education_Sector'].str[3:]

    #df_export_ine = df_export_ine.drop('Age', 1)
    df_export_ine = df_export_ine.drop(columns=['Age'])

    #### Translate the categories into English:

    df_atur_per_sexe['Gender'] = df_atur_per_sexe['Gender'].replace({'Dones': 'Female', 'Homes': 'Male'})
    df_export_ine['Gender'] = df_export_ine['Gender'].replace({'Mujeres': 'Female', 'Hombres': 'Male'})
    df_export_ine_education['Gender'] = df_export_ine_education['Gender'].replace({'Mujeres': 'Female', 'Hombres': 'Male'})
    df_export_ine_education['Education_Sector'] = df_export_ine_education['Education_Sector'].replace({
        'Formaci??n general, formaci??n b??sica de adultos y habilidades personales': 'General education, basic adult education and personal skills',
        'Educaci??n': 'Education',
        'Artes, humanidades y lenguas': 'Arts, humanities and languages',
        'Ciencias sociales, periodismo y documentaci??n': 'Social sciences, journalism and documentation',
        'Negocios, administraci??n y derecho': 'Business, administration and law',
        'Ciencias naturales, qu??micas, f??sicas y matem??ticas': 'Natural , chemical, physical and mathematical sciences',
        'Tecnolog??as de la informaci??n y las comunicaciones (TIC)': 'Information and communication technologies (ICT)',
        'Mec??nica, electr??nica y otra formaci??n t??cnica, industria y construcci??n': 'Mechanics, electronics and other technical training, industry and construction',
        'Agricultura, ganader??a, pesca, silvicultura y veterinaria': 'Agriculture, livestock, fishing, forestry and veterinary medicine',
        'Salud y servicios sociales': 'Health and social services',
        'Servicios': 'Services',
        'Sectores desconocidos o no especificados': 'Unknown or unspecified sectors'})

    """## Save the new results
    
    We save the results in Google Drive in the landing folder, into the `1. output trusted zone` folder.
    """

    df_atur_per_sexe.to_csv("trusted_zone/atur_per_sexe_trusted.csv")
    df_export_ine.to_csv("trusted_zone/export_ine_trusted.csv")
    df_export_ine_education.to_csv("trusted_zone/df_export_ine_education_trusted.csv")
