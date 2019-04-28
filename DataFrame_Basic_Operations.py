#!/usr/bin/env python
# coding: utf-8

# # Basic Operations
# 
# This lecture will cover some basic operations with Spark DataFrames.
# 
# We will play around with some stock data from Apple.

# In[1]:


from pyspark.sql import SparkSession


# In[2]:


# May take awhile locally
spark = SparkSession.builder.appName("Operations").getOrCreate()


# In[26]:


# Let Spark know about the header and infer the Schema types!
df = spark.read.csv('appl_stock.csv',inferSchema=True,header=True)


# In[28]:


df.printSchema()


# ## Filtering Data
# 
# A large part of working with DataFrames is the ability to quickly filter out data based on conditions. Spark DataFrames are built on top of the Spark SQL platform, which means that is you already know SQL, you can quickly and easily grab that data using SQL commands, or using the DataFram methods (which is what we focus on in this course).

# In[31]:


# Using SQL
df.filter("Close<500").show()


# In[35]:


# Using SQL with .select()
df.filter("Close<500").select('Open').show()


# In[36]:


# Using SQL with .select()
df.filter("Close<500").select(['Open','Close']).show()


# Using normal python comparison operators is another way to do this, they will look very similar to SQL operators, except you need to make sure you are calling the entire column within the dataframe, using the format: df["column name"]
# 
# Let's see some examples:

# In[38]:


df.filter(df["Close"] < 200).show()


# In[39]:


# Will produce an error, make sure to read the error!
df.filter(df["Close"] < 200 and df['Open'] > 200).show()


# In[47]:


# Make sure to add in the parenthesis separating the statements!
df.filter( (df["Close"] < 200) & (df['Open'] > 200) ).show()


# In[49]:


# Make sure to add in the parenthesis separating the statements!
df.filter( (df["Close"] < 200) | (df['Open'] > 200) ).show()


# In[51]:


# Make sure to add in the parenthesis separating the statements!
df.filter( (df["Close"] < 200) & ~(df['Open'] < 200) ).show()


# In[46]:


df.filter(df["Low"] == 197.16).show()


# In[52]:


# Collecting results as Python objects
df.filter(df["Low"] == 197.16).collect()


# In[53]:


result = df.filter(df["Low"] == 197.16).collect()


# In[62]:


# Note the nested structure returns a nested row object
type(result[0])


# In[65]:


row = result[0]


# Rows can be called to turn into dictionaries

# In[64]:


row.asDict()


# In[59]:


for item in result[0]:
    print(item)


# That is all for now Great Job!
