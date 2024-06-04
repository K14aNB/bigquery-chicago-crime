# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # **Bigquery - Chicago Crime**

# %% [markdown]
# ## **About the dataset**

# %% [markdown]
# This dataset reflects reported incidents of crime (with the exception of murders where data exists for each victim) that occurred in the City of Chicago from 2001 to present, minus the most recent seven days. Data is extracted from the Chicago Police Department's CLEAR (Citizen Law Enforcement Analysis and Reporting) system. In order to protect the privacy of crime victims, addresses are shown at the block level only and specific locations are not identified. This data includes unverified reports supplied to the Police Department. The preliminary crime classifications may be changed at a later date based upon additional investigation and there is always the possibility of mechanical or human error. Therefore, the Chicago Police Department does not guarantee (either expressed or implied) the accuracy, completeness, timeliness, or correct sequencing of the information and the information should not be used for comparison purposes over time.

# %% [markdown]
# ### **Setup**

# %% [markdown]
# **Check and install the dependencies**

# %%
base_url='https://raw.githubusercontent.com/K14aNB/'

# %%
repo_name='bigquery-chicago-crime'

# %%
nb_name='Bigquery-Chicago-Crime'

# %%
# !curl -sSL {base_url}{repo_name}'/main/requirements.txt'

# %%
# Run this command in terminal before running this notebook as .py script
# Installs dependencies from requirements.txt present in the repo
# %%capture
# !pip install -r {base_url}{repo_name}'/main/requirements.txt'

# %% [markdown]
# **Import the libraries**

# %%
from google.cloud import bigquery
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import db_dtypes
import env_setup
import os

# %% [markdown]
# **Environment Setup**

# %%
client,datasets=env_setup.setup(repo_name=repo_name,nb_name=nb_name)

# %% [markdown]
# **Tables**

# %%
for dataset in datasets:
    tables_list=list(client.list_tables(dataset))
    for table in tables_list:
        print(dataset.table(table.table_id))

# %%
crime=client.get_table('bigquery-public-data.chicago_crime.crime')

# %% [markdown]
# ### **Exploratory Data Analysis**

# %%
# Set a limit on number of bytes that can be billed per query
max_bytes_limit_per_query=3*10**8

# %% [markdown]
# **Schema of `crime` table**

# %%
crime.schema

# %% [markdown]
# **List first 5 records from `crime` table**

# %%
crime_first_five=client.list_rows(crime,max_results=5).to_dataframe()

# %%
crime_first_five

# %%
crime_first_five.info()

# %% [markdown]
# **Columns which have `TIMESTAMP` data**

# %%
timestamp_cols=[field.name for field in crime.schema if field.field_type=='TIMESTAMP']
print(timestamp_cols)

# %% [markdown]
# **Types of Crimes committed**

# %%
crime_type_query='''
                 SELECT primary_type,COUNT(1) AS count_of_crime_committed
                 FROM bigquery-public-data.chicago_crime.crime
                 GROUP BY primary_type
                 ORDER BY count_of_crime_committed DESC
                 '''
crime_type_query_run=False

# %%
# Dry Run
crime_type_query_config_dr=bigquery.QueryJobConfig(dry_run=True,use_query_cache=False)
crime_type_job_dr=client.query(crime_type_query,job_config=crime_type_query_config_dr)
print(f'Total bytes processed={crime_type_job_dr.total_bytes_processed}')

# %%
# Actual Run
if crime_type_job_dr.total_bytes_processed<=max_bytes_limit_per_query:
    crime_type_query_config=bigquery.QueryJobConfig(maximum_bytes_billed=crime_type_job_dr.total_bytes_processed+10**6)
    crime_type_job=client.query(crime_type_query,job_config=crime_type_query_config)
    crime_type_results=crime_type_job.to_dataframe()
    crime_type_query_run=True

# %%
crime_type_results

# %% [markdown]
# **Unique `location_description` where crimes were committed**

# %%
crime_locations_query='''
                      SELECT DISTINCT location_description
                      FROM bigquery-public-data.chicago_crime.crime
                      '''
crime_locations_query_run=False

# %%
# Dry Run
crime_locations_query_config_dr=bigquery.QueryJobConfig(dry_run=True,use_query_cache=False)
crime_locations_job_dr=client.query(crime_locations_query,job_config=crime_locations_query_config_dr)
print(f'Total bytes processed={crime_locations_job_dr.total_bytes_processed}')

# %%
# Actual Run
if crime_locations_job_dr.total_bytes_processed<=max_bytes_limit_per_query:
    crime_locations_query_config=bigquery.QueryJobConfig(maximum_bytes_billed=crime_locations_job_dr.total_bytes_processed+10**6)
    crime_locations_job=client.query(crime_locations_query,job_config=crime_locations_query_config)
    crime_locations_results=crime_locations_job.to_dataframe()
    crime_locations_query_run=True

# %%
crime_locations_results

# %% [markdown]
# **What types of crime took place in `GOVERNMENT BUILDING/PROPERTY`?**

# %%
gov_bldg_crime_types_query='''
                           SELECT primary_type,COUNT(1) AS counts_of_crime
                           FROM bigquery-public-data.chicago_crime.crime
                           WHERE location_description="GOVERNMENT BUILDING/PROPERTY"
                           GROUP BY primary_type
                           ORDER BY counts_of_crime DESC
                           '''
gov_bldg_crime_types_query_run=False

# %%
# Dry Run
gov_bldg_crime_types_query_config_dr=bigquery.QueryJobConfig(dry_run=True,use_query_cache=False)
gov_bldg_crime_types_job_dr=client.query(gov_bldg_crime_types_query,job_config=gov_bldg_crime_types_query_config_dr)
print(f'Total bytes processed={gov_bldg_crime_types_job_dr.total_bytes_processed}')

# %%
# Actual Run
if gov_bldg_crime_types_job_dr.total_bytes_processed<=max_bytes_limit_per_query:
    gov_bldg_crime_types_query_config=bigquery.QueryJobConfig(maximum_bytes_billed=gov_bldg_crime_types_job_dr.total_bytes_processed+10**6)
    gov_bldg_crime_types_job=client.query(gov_bldg_crime_types_query,job_config=gov_bldg_crime_types_query_config)
    gov_bldg_crime_types_results=gov_bldg_crime_types_job.to_dataframe()
    gov_bldg_crime_types_query_run=True

# %%
gov_bldg_crime_types_results

# %% [markdown]
# **How many arrests were involved for each crime type**

# %%
crime_arrests_query='''
                    SELECT primary_type,COUNT(primary_type) AS counts_of_crime,COUNTIF(arrest=True) AS arrest_count
                    FROM bigquery-public-data.chicago_crime.crime
                    GROUP BY primary_type
                    ORDER BY counts_of_crime DESC,arrest_count DESC
                    '''
crime_arrests_query_run=False

# %%
# Dry Run
crime_arrests_query_config_dr=bigquery.QueryJobConfig(dry_run=True,use_query_cache=False)
crime_arrests_job_dr=client.query(crime_arrests_query,job_config=crime_arrests_query_config_dr)
print(f'Total bytes processed={crime_arrests_job_dr.total_bytes_processed}')

# %%
# Actual Run
if crime_arrests_job_dr.total_bytes_processed<=max_bytes_limit_per_query:
    crime_arrests_query_config=bigquery.QueryJobConfig(maximum_bytes_billed=crime_arrests_job_dr.total_bytes_processed+10**6)
    crime_arrests_job=client.query(crime_arrests_query,job_config=crime_arrests_query_config)
    crime_arrests_results=crime_arrests_job.to_dataframe()
    crime_arrests_query_run=True

# %%
crime_arrests_results

# %% [markdown]
# ### **Visualization**

# %% [markdown]
# **Most commonly committed crimes in Chicago (2001-present)**

# %%
# Considering crimes which are committed >10,000
if crime_type_query_run is True:
    common_crimes=crime_type_results.loc[crime_type_results['count_of_crime_committed']>10000]

    # Bar plot of primary_type
    fig=plt.figure(figsize=(30,10))
    sns.barplot(x='primary_type',y='count_of_crime_committed',hue='primary_type',palette='pastel',data=common_crimes)
    plt.xticks(rotation=60)
    plt.xlabel('Crime')
    plt.ylabel('Count')
    plt.title('Bar plot of Crimes in Chicago (2001-present)')
    plt.show()

# %% [markdown]
# **Prevalence of Justice of each type of crime in Chicago (2001-present)**

# %%
if crime_arrests_query_run is True:
    # Scatter plot for counts_of_crime and arrest_count
    fig=plt.figure(figsize=(30,10))
    sns.barplot(x='primary_type',y='counts_of_crime',hue='primary_type',palette='husl',data=crime_arrests_results)
    sns.pointplot(x='primary_type',y='arrest_count',hue='primary_type',palette='pastel',data=crime_arrests_results)
    plt.xticks(rotation=60)
    plt.xlabel('Crime')
    plt.ylabel('Crime and Arrest count')
    plt.title('Bar plot of Crime vs Crime counts and Point plot of Crime vs Arrest counts')
    plt.show()

# %%
