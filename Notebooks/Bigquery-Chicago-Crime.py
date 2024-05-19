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
crime_type_query_config=bigquery.QueryJobConfig(maximum_bytes_billed=10**8)

# %%
crime_type_job=client.query(crime_type_query,job_config=crime_type_query_config)
crime_type_results=crime_type_job.to_dataframe()

# %%
crime_type_results

# %% [markdown]
# **Unique `location_description` where crimes were committed**

# %%
crime_locations_query='''
                      SELECT DISTINCT location_description
                      FROM bigquery-public-data.chicago_crime.crime
                      '''
crime_locations_query_config=bigquery.QueryJobConfig(maximum_bytes_billed=10**6)

# %%
crime_locations_job=client.query(crime_locations_query,job_config=crime_locations_query_config)
crime_locations_results=crime_locations_job.to_dataframe()

# %%
crime_locations_results

# %%
