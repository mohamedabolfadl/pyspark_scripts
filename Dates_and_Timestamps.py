#!/usr/bin/env python
# coding: utf-8

# # Dates and Timestamps
# 
# You will often find yourself working with Time and Date information, let's walk through some ways you can deal with it!

# In[1]:


from pyspark.sql import SparkSession
# May take a little while on a local computer
spark = SparkSession.builder.appName("dates").getOrCreate()


# In[3]:


df = spark.read.csv("appl_stock.csv",header=True,inferSchema=True)


# In[4]:


df.show()


# Let's walk through how to grab parts of the timestamp data

# In[44]:


from pyspark.sql.functions import format_number,dayofmonth,hour,dayofyear,month,year,weekofyear,date_format


# In[45]:


df.select(dayofmonth(df['Date'])).show()


# In[46]:


df.select(hour(df['Date'])).show()


# In[8]:


df.select(dayofyear(df['Date'])).show()


# In[11]:


df.select(month(df['Date'])).show()


# So for example, let's say we wanted to know the average closing price per year. Easy! With a groupby and the year() function call:

# In[15]:


df.select(year(df['Date'])).show()


# In[19]:


df.withColumn("Year",year(df['Date'])).show()


# In[29]:


newdf = df.withColumn("Year",year(df['Date']))
newdf.groupBy("Year").mean()[['avg(Year)','avg(Close)']].show()


# Still not quite presentable! Let's use the .alias method as well as round() to clean this up!

# In[43]:


result = newdf.groupBy("Year").mean()[['avg(Year)','avg(Close)']]
result = result.withColumnRenamed("avg(Year)","Year")
result = result.select('Year',format_number('avg(Close)',2).alias("Mean Close")).show()


# Perfect! Now you know how to work with Date and Timestamp information!
