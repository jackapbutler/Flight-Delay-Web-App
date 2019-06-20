
# coding: utf-8

# In[1]:


import os
import glob
import pandas as pd
import numpy as np

import time
from datetime import datetime


# In[2]:


dep_df, = pd.read_html("https://www.dublinairport.com/flight-information/live-departures", header=0)
dep_df.tail(1)


# In[3]:


# Initial Cleaning
dep1_df = dep_df.dropna()
dep1_df = dep1_df.drop('Status', axis=1)
dep1_df.columns = ['Terminal', 'Destination', 'Airline', 'Flight No.', 'Scheduled DateTime', 'Actual Departure']

# Month Column
new2 = dep1_df["Scheduled DateTime"].str.split(" ", n = 2, expand = True) 
dep1_df["Month"]= new2[1] 

# Splitting Datetime column apart to get Scheduled Time
new1 = dep1_df["Scheduled DateTime"].str.split(" ", n = 2, expand = True) 
dep1_df["Scheduled Timedelta"]= new1[2] 

# Change to 2019
dep1_df['Scheduled DateTime'] = pd.to_datetime(dep1_df["Scheduled DateTime"], format='%d %B %H:%M')
dep1_df['Scheduled DateTime'] = dep1_df['Scheduled DateTime'].apply(lambda dt: dt.replace(year=2019))

# Get Week day column
dep1_df['Weekday'] = pd.to_datetime(dep1_df["Scheduled DateTime"], format='%d %B %H:%M').dt.weekday_name

# Splitting Word from last column 
new = dep1_df["Actual Departure"].str.split(" ", n = 1, expand = True) 
dep1_df["Word"]= new[0] 
dep1_df["Real Departure Timedelta"]= new[1] 
dep1_df = dep1_df.drop('Word', axis=1)
dep1_df = dep1_df.drop('Actual Departure', axis=1)

# Get Formatted DateTime
dep1_df['Scheduled DateTime'] = dep1_df['Scheduled DateTime'].dt.strftime('%Y-%m-%d %H:%M')

# Convert Time Columns to Time Type
dep1_df['Scheduled Timedelta'] = pd.to_timedelta(dep1_df['Scheduled Timedelta']+':00')
dep1_df['Real Departure Timedelta'] = pd.to_timedelta(dep1_df['Real Departure Timedelta']+':00')

# Minutes Delayed Column and Remove Early Flights
dep1_df['Minute Delay'] = pd.to_timedelta(dep1_df['Real Departure Timedelta']-dep1_df['Scheduled Timedelta']).dt.seconds/60.0
dep1_df = dep1_df.drop(dep1_df[dep1_df['Minute Delay'] > 1000].index)

# Create Delay Type Column
dep1_df['Delay Type'] = np.where(dep1_df['Minute Delay'] >= 10, 'Short', 'None')
dep1_df['Delay Type'] = np.where(dep1_df['Minute Delay'] >= 20, 'Medium', dep1_df['Delay Type'])
dep1_df['Delay Type'] = np.where(dep1_df['Minute Delay'] >= 30, 'Long', dep1_df['Delay Type'])


dep1_df = dep1_df.dropna()
dep1_df.tail(1)


# In[4]:


os.chdir("C:\\Users\\rg654th\\Downloads\\Flight Delay\\Data")


# In[5]:


# Change Directory
dep1_df.to_csv(r"C:\Users\rg654th\Downloads\Flight Delay\Data\Departures_random_hour.csv", 
               index=False)


# In[6]:


# Combine Files
extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
combined_csv = pd.DataFrame(np.concatenate([pd.read_csv(f).values for f in all_filenames]), columns=dep1_df.columns)
combined_csv2 = combined_csv.drop_duplicates()
combined_csv2.to_csv("Historical Data.csv", index=False, encoding='utf-8-sig')


# In[7]:


# Delete random hour file
os.remove("Departures_random_hour.csv")


# In[8]:


#  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #
"Historical Raw Data Scraping Above"


# In[9]:


"New Clean Data Below"
#  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  


# In[10]:


dataframe = pd.read_csv(r"C:\Users\rg654th\Downloads\Flight Delay\Data\Historical Data.csv", parse_dates=[0], infer_datetime_format=True)


# In[11]:


# Converting data types & Drop Duplicates
dataframe['Scheduled Timedelta'] = pd.to_timedelta(dataframe['Scheduled Timedelta'])
dataframe['Real Departure Timedelta'] = pd.to_timedelta(dataframe['Real Departure Timedelta'])
dataframe['Scheduled DateTime'] = pd.to_datetime(dataframe['Scheduled DateTime'], infer_datetime_format=True)
dataframe['Minute Delay'] = pd.to_numeric(dataframe['Minute Delay'], downcast='integer')

dataframe = dataframe.drop_duplicates()
dataframe.to_csv("Historical Data.csv", index = False)


# In[12]:


dataframe = pd.read_csv(r"C:\Users\rg654th\Downloads\Flight Delay\Data\Historical Data.csv", parse_dates=[0], infer_datetime_format=True)


# In[13]:


# Converting data types 
dataframe['Scheduled Timedelta'] = pd.to_timedelta(dataframe['Scheduled Timedelta'])
dataframe['Real Departure Timedelta'] = pd.to_timedelta(dataframe['Real Departure Timedelta'])
dataframe['Scheduled DateTime'] = pd.to_datetime(dataframe['Scheduled DateTime'], infer_datetime_format=True)
dataframe['Minute Delay'] = pd.to_numeric(dataframe['Minute Delay'], downcast='integer')


# In[14]:


# New Columns
dataframe['Scheduled Date'] = dataframe['Scheduled DateTime'].dt.strftime('%d-%m-%Y')
dataframe['Scheduled Time'] = dataframe['Scheduled DateTime'].dt.strftime('%H:%M')
dataframe['Scheduled DateTime'] = pd.to_datetime(dataframe['Scheduled DateTime'], infer_datetime_format=True)

dataframe = dataframe.dropna()


# In[15]:


# format Real Departure Time
today = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
year_now = datetime.now().strftime('%Y')
year_now = int(year_now)

month_now = datetime.now().strftime('%m')
month_now = int(month_now)

day_now = datetime.now().strftime('%d')
day_now = int(day_now)

dataframe['Real Departure Time'] = pd.to_datetime(dataframe['Real Departure Timedelta'])
dataframe['Real Departure Time'] = dataframe['Real Departure Time'].apply(lambda dt: dt.replace(year=year_now, month=month_now, day=day_now, second=0, microsecond=0, nanosecond=0))

dataframe['Real Departure Time'] = dataframe['Real Departure Time'].dt.strftime('%H:%M')


# In[16]:


# dropping columns/rows
dataframe = dataframe.drop(['Scheduled Timedelta', 'Real Departure Timedelta'], axis=1)


# In[17]:


dataframe.head(1)


# In[18]:


# Save Csv
os.chdir("C:\\Users\\rg654th\\Downloads\\Flight Delay\\Clean Data")
dataframe.to_csv("Clean Historical Data.csv", index = False)


# In[ ]:





# In[ ]:




