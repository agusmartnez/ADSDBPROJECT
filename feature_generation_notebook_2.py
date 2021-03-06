
##2. feature_generation_notebook.ipynb

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import statsmodels.api as sm
import json

def load_data():
    ## Load our data
    df_atur_education = pd.read_csv("featgen_zone/df_atur_education_featgen.csv", index_col=0)
    df_atur_zone = pd.read_csv("featgen_zone/df_atur_zone_featgen.csv", index_col=0)
    df_atur_barcelona = pd.read_csv("featgen_zone/df_atur_barcelona_featgen.csv", index_col=0)
    return df_atur_education, df_atur_zone, df_atur_barcelona


def analysis_table_education():
    ## TABLE 1
    df_atur_education, _, _ = load_data()
    fig = px.bar(df_atur_education, x="Year", y="rate", facet_col="ccaa", facet_col_wrap=5, color="gender")
    fig.update_yaxes(matches=None)
    fig.update_yaxes(showticklabels=True)

    fig.update_xaxes(showticklabels=True)
    fig.update_yaxes(rangemode='tozero')
    fig.update_layout(showlegend=False)

    fig.update_layout(
        autosize=False,
        width=1400,
        height=900, title={
            'text': "Unemployed rate per year according to gender by CCAA",
            'x':0.5,
            'y':0.99,
            'yanchor': 'top'})

    fig.show()
    #In the graph above we can compare the amount of unemployment rate per genre in each region of Spain. We can see that there is a good balance between the two genders, even so, we can see that during some years in some regions in particular, the unemployment difference between men and women increases, e.g. in Castilla La Mancha, the last years in Aragon, in Extremadura in 2016, among other cases.

    df_atur_education_no_unknown = df_atur_education[df_atur_education['education_sector'] != 'Unknown or unspecified sectors']

    fig = px.bar(df_atur_education_no_unknown, x="Year", y="rate", facet_col="ccaa", facet_col_wrap=5, color="education_sector")
    fig.update_yaxes(matches=None)
    fig.update_yaxes(showticklabels=True)

    fig.update_xaxes(showticklabels=True)
    fig.update_yaxes(rangemode='tozero')
    fig.update_layout(showlegend=False)

    fig.update_layout(
        autosize=False,
        width=1400,
        height=900, title={
            'text': "Unemployed rate per year according to education sector by CCAA",
            'x':0.5,
            'y':0.99,
            'yanchor': 'top'})

    fig.show()
    #In the graph above we can compare the amount of unemployment rate by educational sector in each region of Spain. We can see how in many of the regions there is a good balance of unemployment for each sector, but there are some cases where there is a clear increase in unemployment for some of the sectors, for example in Ceuta for 2019, where there seems to be a lack of work for the ICT sector, the same happens in La Rioja during the 2017-2017 period.


#### TABLE 2
### Analysis 1: see the evolution of unemployment between the different areas
def analysis_table_zone_by_areas():
    _, df_atur_zone, _ = load_data()
    df_atur_zone_group1 = df_atur_zone.groupby(by=['zone', 'year']).agg({'total': 'sum'})
    df_atur_zone_group1 = df_atur_zone_group1.reset_index()

    fig = px.line(df_atur_zone_group1, x="year", y="total", facet_col="zone", facet_col_wrap=5, color="zone")

    fig.update_yaxes(matches=None)
    fig.update_yaxes(showticklabels=True)

    fig.update_xaxes(showticklabels=True)
    fig.update_yaxes(rangemode='tozero')
    fig.update_layout(showlegend=False)

    fig.update_layout(
        autosize=False,
        width=1400,
        height=900, title={
            'text': "Number of unemployed people by Zone",
            'x':0.5,
            'y':0.99,
            'yanchor': 'top'})

    fig.show()
    #Most of the CCAAs have in common that there was a peak at the beginning of the graph that corresponds to the economic crisis that Spain suffered during the years 2008-2014, then we see a drop in unemployment due to the recovery from this crisis and finally we see as there is a small peak in 2020 that corresponds to the coronavirus period.
    #If we compare Catalonia with Barcelona, we can see that the crisis of the years 2008-2014 affected Catalonia but not so much the city of Barcelona, but instead when Catalonia was already recovering from the crisis, lowering the number of unemployed, there is a peak in 2015 in Barcelona, which will be repeated in 2020, in the last case due to the coronavirus.


def analysis_table_zone_by_quarters():
    _, df_atur_zone_group2, _ = load_data()

    ### Analysis 2: See the evolution of unemployment by quarters in Spain
    df_atur_zone_group2 = df_atur_zone_group2.drop(df_atur_zone_group2[df_atur_zone_group2['zone']=='Catalu??a: Bacelona'].index)
    df_atur_zone_group2 = df_atur_zone_group2.groupby(by=['year', 'quarter']).agg({'total':'sum'})
    df_atur_zone_group2 = df_atur_zone_group2.reset_index()

    df_atur_zone_group2 = df_atur_zone_group2.replace({1:'T1', 2:'T2',3:'T3',4:'T4'})
    df_atur_zone_group2['year'] = df_atur_zone_group2.year.astype(object)

    df_atur_zone_group2['Date'] = df_atur_zone_group2['year'].map(str) + ' '+ df_atur_zone_group2['quarter']


    fig = px.bar(df_atur_zone_group2, x="Date", y="total", text='total', color='year')

    fig.update_traces(texttemplate='%{text:.1s}', textposition='outside', textfont_size=20)

    fig.update_layout(
        autosize=False,
        width=1000,
        height=600, title={
            'text': "Unemployed people in Spain by quarter",
            'x':0.5,
            'y':0.99,
            'yanchor': 'top'})

    fig.show()
    #We can see that for most years, the first quarter of each month is when there are more unemployed people than the other quarters of the year in Spain. This may be due to the temporary summer contracts ending and the beginning of the year.
    return df_atur_zone_group2

def model_table_zone(df_atur_zone_group2):
    ### Model
    #We want to make a model that predicts the number of unemployed people, to see how the coronavirus has affected the number of unemployed.

    df_model = df_atur_zone_group2
    df_model['quarter'] = df_model.quarter.replace({'T1':'01', 'T2': '04', 'T3': '07', 'T4': '10'})

    df_model['Date'] = df_model['year'].map(str) + '-' + df_atur_zone_group2['quarter']
    df_model['Date'] = pd.to_datetime(df_model['Date'])
    df_model = df_model.set_index('Date')

    df_model = df_model.drop(['year', 'quarter'], axis=1)

    df = df_model
    df_model = df_model[:36]

    #We first split our database into a training set and a test set.
    #- Training set: 2011-2017
    # Test set: 2018-2019

    steps = 8
    datos_train = df_model[:-steps]
    datos_test = df_model[-steps:]

    fig, ax = plt.subplots(figsize=(9, 4))
    datos_train['total'].plot(ax=ax, label='train')
    datos_test['total'].plot(ax=ax, label='test')
    ax.legend();

    #Now we use the SARIMAX algorithm, a very good tool to predict values through time series, to build our model with the training data.

    # Build Model
    mod = sm.tsa.statespace.SARIMAX(datos_train['total'],
                                    order=(1, 1, 1),
                                    seasonal_order=(1, 1, 1, 4),
                                    enforce_stationarity=False,
                                    enforce_invertibility=False)
    fitted = mod.fit(disp=-1)
    print(fitted.summary().tables[1])

    # Forecast
    fc = fitted.forecast(8, alpha=0.05)  # 95% conf
    pred_uc = fitted.get_forecast(steps=8)
    pred_ci = pred_uc.conf_int()
    # Make as pandas series
    fc_series = pd.Series(fc, index=datos_test.index)
    lower_series = pd.Series(pred_ci.iloc[:, 0], index=datos_test.index, dtype='float64')
    upper_series = pd.Series(pred_ci.iloc[:, 1], index=datos_test.index, dtype='float64')

    # Plot
    plt.figure(figsize=(9,4), dpi=100)
    plt.plot(datos_train, label='train')
    plt.plot(datos_test, label='test')
    plt.plot(fc_series, label='forecast')
    plt.fill_between(lower_series.index, lower_series, upper_series,
                     color='k', alpha=.15)
    plt.title('Train data vs Test data')
    plt.legend(loc='upper left', fontsize=8)
    plt.show()


    # Accuracy metrics
    def forecast_accuracy(forecast, actual):
        mape = np.mean(np.abs(forecast - actual)/np.abs(actual))  # MAPE
        me = np.mean(forecast - actual)             # ME
        mae = np.mean(np.abs(forecast - actual))    # MAE
        mpe = np.mean((forecast - actual)/actual)   # MPE
        rmse = np.mean((forecast - actual)**2)**.5  # RMSE
        return({'mape':mape, 'me':me, 'mae': mae,
                'mpe': mpe, 'rmse':rmse})

    forecast_accuracy(fc.values, datos_test.values)

    """Around 7.5% MAPE implies the model is about 92.5% accurate in predicting the  observations corresponding to the years 2018-2019."""

    # Forecast
    fc = fitted.forecast(16, alpha=0.05)  # 95% conf
    pred_uc = fitted.get_forecast(steps=16)
    pred_ci = pred_uc.conf_int()
    # Make as pandas series
    fc_series = pd.Series(fc, index=df[-8:].index)
    lower_series = pd.Series(pred_ci.iloc[:, 0], index=df[-8:].index, dtype='float64')
    upper_series = pd.Series(pred_ci.iloc[:, 1], index=df[-8:].index, dtype='float64')

    # Plot
    plt.figure(figsize=(9,4), dpi=100)
    plt.plot(df[-8:], label='actual')
    plt.plot(fc_series, label='forecast')
    plt.fill_between(lower_series.index, lower_series, upper_series,
                     color='k', alpha=.15)
    plt.title('Forecast vs Actuals 2020-2021')
    plt.legend(loc='upper left', fontsize=8)
    plt.show()

    # Accuracy metrics
    def forecast_accuracy(forecast, actual):
        mape = np.mean(np.abs(forecast - actual)/np.abs(actual))  # MAPE
        me = np.mean(forecast - actual)             # ME
        mae = np.mean(np.abs(forecast - actual))    # MAE
        mpe = np.mean((forecast - actual)/actual)   # MPE
        rmse = np.mean((forecast - actual)**2)**.5  # RMSE
        return({'mape':mape, 'me':me, 'mae': mae,
                'mpe': mpe, 'rmse':rmse})

    forecast_accuracy(fc.values, df[-8:].values)

    #Around 12.7% MAPE implies the model is about 83.3% accurate in predicting the  observations corresponding to the years 2020-2021.
    #We can see that due to the situation caused by the coronavirus, the number of unemployed is well above what the predicted number of unemployed should have been.


# TABLE 3
def analysis_table_barcelona():
    _, _, df_atur_barcelona = load_data()
    df_atur_barcelona = df_atur_barcelona[:24506] #from 2015 to 2021
    df_atur = df_atur_barcelona[df_atur_barcelona['demmand_occupancy']=='Atur Registrat']
    df_no_atur = df_atur_barcelona[df_atur_barcelona['demmand_occupancy']=='Demanda No Aturats']

    df_rate = df_atur.merge(df_no_atur, on=['year', 'name_neighborhood'], how='inner')

    df_rate2020 = df_rate[df_rate['year']==2020]
    df_rate2020 = df_rate2020.groupby(by=['name_neighborhood']).agg({'total_x':'sum', 'total_y': 'sum'})
    df_rate2020 = df_rate2020.reset_index()
    df_rate2020['rate'] = df_rate2020['total_x']/df_rate2020['total_y']*100

    df_rate2020.head()

    f = open('temporal/barris.geojson')
    barris_pol = json.load(f)
    f.close()
    max_count = df_rate2020['rate'].max()*0.3
    min_count = df_rate2020['rate'].min()
    fig = px.choropleth_mapbox(df_rate2020,
                               geojson=barris_pol,
                               locations='name_neighborhood',
                               color='rate',
                               color_continuous_scale="Purp",
                               featureidkey = 'properties.NOM',
                               range_color=(min_count, max_count),
                               mapbox_style="open-street-map",
                               zoom=10.5,
                               center = {"lat": 41.38879, "lon": 2.15899},
                               opacity=0.8
                              )
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    fig.show()
    #The analysis we have carried out with the Barcelona unemployment data has been to generate a new variable called Rate that allows us to see the ratio of people who demand unemployment with respect to the people who demand no unemployment.
    #Through the map, that we have made using the geojson that contains the polygon of each neighborhood, we see that the neighborhoods that have that highest rate are those located further north: Trinitat Vella, Trinitat Nova, Torre Bar??, Ciutat Meridiana and Vallbona.
