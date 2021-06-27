# -*- coding: utf-8 -*-
"""
Created on Sun Jun 27 16:49:19 2021

@author: u346483
"""
##########################################################################################
# First Step - Data Load and column pre-processing
##########################################################################################
import os
import pandas as pd
import numpy as np
import datetime as dt

# set data file path
PATH = 'C:/Users/u346483/Documents/Projects/SupplyChain-Case'
os.chdir(PATH)

# Load crimes data and column header processing # Date columns are converted to pandas datetime format at later stage on subsetted data
chicago_crimes = pd.read_csv("Crimes_-_2001_to_Present.csv", sep = ',')
chicago_crimes.columns = chicago_crimes.columns.str.replace(' ','_').str.lower()
# Cleaning primary_type column values and standardising values 
chicago_crimes['primary_type'] = chicago_crimes['primary_type'].replace(' - ', '-', regex=True)


# Load iucr code data and column header processing
iucr_codes = pd.read_csv("Chicago_Police_Department_-_Illinois_Uniform_Crime_Reporting__IUCR__Codes.csv", sep = ',')
iucr_codes.columns = iucr_codes.columns.str.replace(' ','_').str.lower()
# Cleaning iucr column and adding prefix 0's to standardze iucr_code acc to crime dataset 
iucr_codes['iucr'] = iucr_codes['iucr'].str.zfill(4)


# Load ward data and column header processing
ward_offices = pd.read_csv("Ward_Offices.csv", sep = ',')
ward_offices.columns = ward_offices.columns.str.replace(' ','_').str.lower()
# Subsettting the required columns
ward_alderman = ward_offices[['ward','alderman']]

##########################################################################################

# Second Step - transfomation and aggregation

##########################################################################################

# left join crime data with iucr_codes to get violent crime definition (index_code)
chicago_crimes_merged = pd.merge(left = chicago_crimes, right = iucr_codes,  how='left', on = 'iucr')

# Checking for NULLs in index_code after merging
#chicago_crimes_merged[chicago_crimes_merged['index_code'].isna()].groupby(['primary_type']).count()[['id','index_code']]

# To impute index_code for NULL cases, FBI code from crime data is used where index_code wasn't null 
# Below code gives a unique fbi_code and index_code mapping 
fbi_code_index = chicago_crimes_merged[chicago_crimes_merged['index_code'].notnull()].groupby(['fbi_code', 'index_code']).count()[['id']].reset_index()
fbi_code_index.drop(fbi_code_index.loc[(fbi_code_index['fbi_code']=='09') & (fbi_code_index['index_code']=='N')].index, inplace=True)

# Split crime_merged data in 2 parts - NULL and Not NULL index_codes datasets
x1 = chicago_crimes_merged[chicago_crimes_merged['index_code'].notnull()]
x2 = chicago_crimes_merged[chicago_crimes_merged['index_code'].isna()]

# Joining split data having NULL index_codes to fbi_index_code mapping created earlier
# Imputting the index_codes from fbi_code mapping table 
x2 = pd.merge(left = x2, right = fbi_code_index[['fbi_code','index_code']],  how='left', on = 'fbi_code', suffixes = ('','_new'))
#x2[x2['index_code_new'].isna()]

# Copying the not NULL index_codes to new columns for append
x1['index_code_new'] = x1[['index_code']]

# Append the data into one dataset
chicago_crimes_all = x1.append(x2, ignore_index=True, sort=False)
# Drop duplicate or non required columns and Filter data for only violent crime recog by index_code = 'I'
chicago_crimes_all.drop(['primary_description','secondary_description','index_code'], axis='columns', inplace=True)
chicago_violent_crimes =  chicago_crimes_all.loc[chicago_crimes_all['index_code_new']=='I'] #.reset_index()

# Deleting not required dataframes from memory
del x1
del x2
del ward_offices
del chicago_crimes_merged


# Join with ward data to get Alderman names
chicago_violent_crimes = pd.merge(left = chicago_violent_crimes, right = ward_alderman,  how='left', on = 'ward')
chicago_violent_crimes['ward_alderman'] = chicago_violent_crimes['ward'].astype(str) +'-'+ chicago_violent_crimes['alderman']


# Primary type column cleaning to standardse and replace CRIM & CRIMINAL and  Date column conversion to pandas datetime format
chicago_violent_crimes['primary_type'] = chicago_violent_crimes['primary_type'].replace('CRIM ', 'CRIMINAL ', regex=True)
chicago_violent_crimes['date'] = pd.to_datetime(chicago_violent_crimes['date'], format='%m/%d/%Y %I:%M:%S %p')

chicago_violent_crimes['month'] = chicago_violent_crimes['date'].dt.month
chicago_violent_crimes['hour'] = chicago_violent_crimes['date'].dt.hour
chicago_violent_crimes['weekday'] = chicago_violent_crimes['date'].dt.weekday


# Filtering out 2021 data since only 6 months data and removing any month bias
chicago_violent_crimes_2001_2020 = chicago_violent_crimes
chicago_violent_crimes_2001_2020.drop(chicago_violent_crimes_2001_2020.loc[(chicago_violent_crimes_2001_2020['year']==2021)].index, inplace=True)

##########################################################################################

# Third Step - Output layer and Final data aggregation at different cuts

##########################################################################################

# Aggregated Dataframes for ppt chart cuts
crimes_type = chicago_violent_crimes_2001_2020.groupby(['primary_type'])['id'].count().sort_values(ascending=False).reset_index(name="count_cases")
crimes_location_desc = chicago_violent_crimes_2001_2020.groupby(['location_description'])['id'].count().nlargest(20).reset_index(name="count_cases")
crimes_ward = chicago_violent_crimes_2001_2020.groupby(chicago_violent_crimes_2001_2020['ward_alderman'])['id'].count().sort_values(ascending=False).reset_index(name="count_cases")

# Time Period cuts - #Weekday: 0 is Monday 
crimes_year = chicago_violent_crimes_2001_2020.groupby(['year'])['id'].count().reset_index(name="count_cases")
crimes_month = chicago_violent_crimes_2001_2020.groupby(['month'])['id'].count().reset_index(name="count_cases")
crimes_weekday = chicago_violent_crimes_2001_2020.groupby(['weekday'])['id'].count().reset_index(name="count_cases")
crimes_hour = chicago_violent_crimes_2001_2020.groupby(['hour'])['id'].count().reset_index(name="count_cases")

# Pivot up the data for stacked or side by side chart
crimes_hour_type_temp = chicago_violent_crimes_2001_2020.groupby(['hour','primary_type'])['id'].count().reset_index(name="count_cases")
crimes_hour_type = crimes_hour_type_temp.pivot(index="hour", columns="primary_type", values="count_cases")

crimes_arrest_temp = chicago_violent_crimes_2001_2020.groupby(['year','arrest'])['id'].count().reset_index(name="count_cases")
crimes_arrest = crimes_arrest_temp.pivot(index="year", columns="arrest", values="count_cases")

crimes_domestic_temp = chicago_violent_crimes_2001_2020.groupby(['year','domestic'])['id'].count().reset_index(name="count_cases")
crimes_domestic = crimes_domestic_temp.pivot(index="year", columns="domestic", values="count_cases")

del crimes_hour_type_temp
del crimes_arrest_temp
del crimes_domestic_temp


# Overall Output csv for violent crimes 
chicago_violent_crimes_2001_2020.to_csv("violent_cases_export_final.csv", index=False)


##########################################################################################
##########################################################################################

# Victims analysis -  Other dataset processing

##########################################################################################

# Load victims data and column header processing
violent_crime_victims = pd.read_csv("Violence_Reduction_-_Victim_Demographics_-_Aggregated.csv", sep = ',')
violent_crime_victims.columns = violent_crime_victims.columns.str.replace(' ','_').str.lower()

# Replace NULLs with 'NA'
violent_crime_victims['primary_type'].fillna('NA', inplace=True)
violent_crime_victims['age'].fillna('NA', inplace=True)
violent_crime_victims['sex'].fillna('NA', inplace=True)
violent_crime_victims['race'].fillna('NA', inplace=True)
violent_crime_victims['domestic_i'].fillna('NA', inplace=True)

# Replace '(NOT PROVIDED)' value in data to 'NA'
violent_crime_victims['primary_type'] = violent_crime_victims['primary_type'].replace('(NOT PROVIDED)', 'NA')
violent_crime_victims['age'] = violent_crime_victims['age'].replace('(NOT PROVIDED)', 'NA')
violent_crime_victims['sex'] = violent_crime_victims['sex'].replace('(NOT PROVIDED)', 'NA')
violent_crime_victims['race'] = violent_crime_victims['race'].replace('(NOT PROVIDED)', 'NA')
violent_crime_victims['domestic_i'] = violent_crime_victims['domestic_i'].replace('(NOT PROVIDED)', 'NA')


# Output dataframes for analysis charts
victims_crime = violent_crime_victims.groupby(['primary_type'])['number_of_victims'].sum().reset_index()
victims_age = violent_crime_victims.groupby(['age'])['number_of_victims'].sum().reset_index()
victims_sex = violent_crime_victims.groupby(['sex'])['number_of_victims'].sum().reset_index()
victims_race = violent_crime_victims.groupby(['race'])['number_of_victims'].sum().reset_index()


##########################################################################################
##########################################################################################
