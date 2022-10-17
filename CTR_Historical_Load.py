#!/usr/bin/env python
# coding: utf-8

# ## CTR_Historical_Load
# 
# 
# 

# In[68]:


goldcontainer_name = 'gold'
gold_path = "aws/snapshot/incremental/consumption/"
landingcontainer_name = 'landing'
landing_path = 'aws/prd_edl2_aws/aws_hive_ltc_landing_voc_2022_Jan_to_Mar'
host = '@dlsmfceus2aaihubprd01.dfs.core.windows.net/'
source_name = 'ltc'


# In[46]:


from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
from delta.tables import *
import time
from collections import Counter
import re
from datetime import datetime

current_date = time.strftime("%Y%m%d")
spark.sql("set spark.sql.caseSensitive=true")

# This function flattend the dataframe recursively (as json file contains arrays and structs)
def flatten(df):
   # compute Complex Fields (Lists and Structs) in Schema   
   complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   while len(complex_fields)!=0:
      col_name=list(complex_fields.keys())[0]
      #print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
    
      # if StructType then convert all sub element to columns.
      # i.e. flatten structs
      if (type(complex_fields[col_name]) == StructType):
         expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
         df=df.select("*", *expanded).drop(col_name)
    
      # if ArrayType then add the Array Elements as Rows using the explode function
      # i.e. explode Arrays
      elif (type(complex_fields[col_name]) == ArrayType):    
         df=df.withColumn(col_name,explode_outer(col_name))
    
      # recompute remaining Complex Fields in Schema       
      complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   return df


# In[47]:


spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true") 


# In[48]:


# Renames columns which have spaces, bad characters and renames duplicate cols
def renameColumns(col_list):
    bad_chars = ['-', ' ','?','(',')','/','\\','@','&','*','!','%','+','<','>','=','"','\'']
    #renamed_col_list = []
    renamed_col_dict = {}

    for col_string in col_list:
        colstr = col_string
        matched_list = [characters in bad_chars for characters in col_string] 
        if True in matched_list:
            for i in bad_chars :
                col_string = col_string.replace(i, '_')
                    
            col_string = col_string.replace('__', '_')    
            if col_string[-1] == '_':
                col_string = col_string[:-1:]
        elif col_string == 'References':
                col_string = 'Reference'
        else:
            if "Attributes_" in col_string:
                col_string = col_string.replace("Attributes_",'')
            else:
                col_string = col_string.replace("Attributes_",'')
        #renamed_col_list.append(col_string)
        renamed_col_dict[colstr] = col_string
    
    return renamed_col_dict

# Checks if there are any invalid or bad characters in column names
def detect_invalid_chars(colslist):
    re1 = re.compile(r"[?()-/{}[\]~` ]");
    invalidcols = []
    for colstr in colslist:
        if re1.search(colstr):
            invalidcols.append(colstr)
    return invalidcols

# Finding duplicate columns
def getduplicate_col_list(collist):
    col_dictionary = {}
    duplicates = []
    for element in collist:
        #print('in for ',element )
        if element.lower() not in col_dictionary:
            col_dictionary[element.lower()] = 1
        else:
            #print(element)
            col_dictionary[element.lower()] += 1
            duplicates.append(element)
    #for x in duplicates:
    #    print(x)
    
    return duplicates

# Gets duplicates, renames them. Clears bad characters and renames cols
def validatecols(df_ctr_flattened):
    listofcols = df_ctr_flattened.columns
    duplicatecols = getduplicate_col_list(listofcols)
    #for x in duplicatecols:
    #    print('Duplicate Col-->' ,x)
    badcharactercols = detect_invalid_chars(listofcols)
    
    #for x in badcharactercols:
    #    print('badcharactercols-->' ,x)

    invalidcolslist = duplicatecols + badcharactercols
    #for i in invalidcolslist:
    #    print('invalidcolslist---->', i)
    #cols_to_be_renamed = list(dict.fromkeys(invalidcolslist))  #removes duplicate cols
    
    newcolslist_dict = renameColumns(invalidcolslist) 
    
    for key, value in newcolslist_dict.items():
        #print(key , value)
        df_ctr_flattened = df_ctr_flattened.withColumnRenamed(key,value)

    ## Renaming duplicate cols in the dataframe
    old_col=df_ctr_flattened.schema.names
    running_list=[]
    new_col=[]
    i=0
    for column in old_col:
        if(column in running_list):
            new_col.append(column + "_"+str(i))
            i=i+1
        else:
            new_col.append(column)
            running_list.append(column)
    df_ctr_flattened=df_ctr_flattened.toDF(*new_col)
    
    return df_ctr_flattened

# Function to create dictionary for columns, which are required to be updated
def createUpdateCols_dictionary(df_ctr_colsrenamed):
    cols_str = df_ctr_colsrenamed.columns
    col_dict = {}

    for strcol in cols_str:
        col_dict['t.' + strcol] = 's.' + strcol
    return col_dict

def renaming_cols_to_lowerase(df_ctr_colsrenamed):
    for col in df_ctr_colsrenamed.columns:
            df_ctr_colsrenamed = df_ctr_colsrenamed.withColumnRenamed(col,col.lower())
        
    old_col=df_ctr_colsrenamed.schema.names
    running_list=[]
    new_col=[]
    i=0
    for column in old_col:
        if(column in running_list):
            new_col.append(column + "_"+str(i))
            i=i+1
        else:
            new_col.append(column)
            running_list.append(column)
    df_ctr_colsrenamed = df_ctr_colsrenamed.toDF(*new_col)
    return df_ctr_colsrenamed


# In[49]:


from pyspark.sql.functions import col,from_json


# In[55]:


voc_landing_path = "abfss://" + landingcontainer_name + host + landing_path + "/"  
print(voc_landing_path)
delta_table_path = "abfss://" + goldcontainer_name + host+  gold_path + source_name + "ctr/delta"
print(delta_table_path)


# In[56]:


delta_table_path = "abfss://" + goldcontainer_name + host + gold_path + source_name + "ctr/delta" + source_name
#source_name = 'annuity'
voc_landing_path = "abfss://" + landingcontainer_name + host + landing_path + "/"  
df_voc = spark.read.orc(voc_landing_path)
df_voc.createOrReplaceTempView("test_tbl")
df_voc.show(5)
voc_landing_path_1 = "abfss://" + landingcontainer_name + host + landing_path + source_name + "_voc_json_from_orc/file"  
df_tbl = spark.sql('select _col0 from test_tbl')
df_tbl.show(5,truncate=False)
df_tbl.count()


# In[39]:


from pyspark.sql.functions import col,from_json

source_name = 'life'
#####  Reads the json string column from orc file and flattens it.
#delta_table_path = "abfss://" + goldcontainer_name + host + gold_path + source_name + "ctr/delta" + source_name
delta_table_path = "abfss://" + goldcontainer_name + host+  gold_path + source_name + "ctr/delta"

#voc_landing_path = "abfss://" + landingcontainer_name + host + landing_path + source_name + "_voc_orc/"  
voc_landing_path = "abfss://" + landingcontainer_name + host + landing_path + "/"  
df_voc = spark.read.orc(voc_landing_path)
df_voc.createOrReplaceTempView("test_tbl")

voc_landing_path_1 = "abfss://" + landingcontainer_name + host + landing_path + source_name + "_voc_json_from_orc/file"  
df_tbl = spark.sql('select _col1 from test_tbl')

sanitize = True


res = df_tbl

res = (res.withColumn('_col1',concat(lit('{"data": '), '_col1', lit('}'))))


# infer schema and apply it
schema = spark.read.json(res.rdd.map(lambda x: x["_col1"])).schema
res = res.withColumn("_col1", from_json(col('_col1'), schema))

# unpack the wrapped object if needed

res = res.withColumn('_col1',col('_col1').data)

#res.printSchema()

df_ctr_flattened = flatten(res)
#df.printSchema()

lscols = df_ctr_flattened.columns

for column in lscols:
    df_ctr_flattened = df_ctr_flattened.withColumnRenamed(column,column[6:])

#df_ctr_flattened.printSchema()
df_ctr_colsrenamed = validatecols(df_ctr_flattened)

mssparkutils.fs.mkdirs(delta_table_path)
files=mssparkutils.fs.ls(delta_table_path)

# Dropping columns which appear as null in absence of any array or struct
columnstoDrop = ['Queue','Recording','Recordings','TransferredToEndpoint','Agent','Agent_HierarchyGroups']
df_ctr_colsrenamed = df_ctr_colsrenamed.drop(*columnstoDrop)

print('Number of files ',len(files))
print(delta_table_path)


# In[69]:


delta_table_path = "abfss://" + goldcontainer_name + host+  gold_path + source_name + "ctr/delta"
print(delta_table_path)
voc_landing_path = "abfss://" + landingcontainer_name + host + landing_path + "/"  
print(voc_landing_path)


# In[70]:


print(source_name)


# In[71]:


from pyspark.sql.functions import col,from_json

#####  Reads the json string column from orc file and flattens it.
#delta_table_path = "abfss://" + goldcontainer_name + host + gold_path + source_name + "ctr/delta" + source_name
#source_name = 'life'
delta_table_path = "abfss://" + goldcontainer_name + host+  gold_path + source_name + "ctr/delta"

#voc_landing_path = "abfss://" + landingcontainer_name + host + landing_path + source_name + "_voc_orc/"  
voc_landing_path = "abfss://" + landingcontainer_name + host + landing_path + "/"  
df_voc = spark.read.orc(voc_landing_path)
df_voc.createOrReplaceTempView("test_tbl")

voc_landing_path_1 = "abfss://" + landingcontainer_name + host + landing_path + source_name + "_voc_json_from_orc/file"  
df_tbl = spark.sql('select _col1 from test_tbl')

sanitize = True


res = df_tbl

res = (res.withColumn('_col1',concat(lit('{"data": '), '_col1', lit('}'))))


# infer schema and apply it
schema = spark.read.json(res.rdd.map(lambda x: x["_col1"])).schema
res = res.withColumn("_col1", from_json(col('_col1'), schema))

# unpack the wrapped object if needed

res = res.withColumn('_col1',col('_col1').data)

#res.printSchema()

df_ctr_flattened = flatten(res)
#df.printSchema()

lscols = df_ctr_flattened.columns

for column in lscols:
    df_ctr_flattened = df_ctr_flattened.withColumnRenamed(column,column[6:])

#df_ctr_flattened.printSchema()
df_ctr_colsrenamed = validatecols(df_ctr_flattened)

mssparkutils.fs.mkdirs(delta_table_path)
files=mssparkutils.fs.ls(delta_table_path)

# Dropping columns which appear as null in absence of any array or struct
columnstoDrop = ['Queue','Recording','Recordings','TransferredToEndpoint','Agent','Agent_HierarchyGroups']
df_ctr_colsrenamed = df_ctr_colsrenamed.drop(*columnstoDrop)


# Finding if any extra columns are coming for already existing records, then update delta. Also checks for duplicate columns
if len(files) > 0:
    delta_df = spark.read.format("delta").load(delta_table_path)
    delta_column_list = delta_df.columns
    #for i in delta_column_list:
    #    print('delta_column_list', i)
    new_arrived_column_list = df_ctr_colsrenamed.columns
    #for i in new_arrived_column_list:
    #    print('new_arrived_column', i)
    extra_cols = list((Counter(new_arrived_column_list) - Counter(delta_column_list)))
    #for i in extra_cols:
    #    print('extra column', i)

    #duplicate_list = getduplicate
    if len(extra_cols) > 0:
        #find duplicates by adding extra cols to delta col list
        for col in extra_cols:
            delta_column_list.append(col)

        dup_cols = getduplicate_col_list(delta_column_list)
        if len(dup_cols) > 0:
            for col in dup_cols:
                column = col
                colwithoutattribute = column.replace('Attributes_','')
                if colwithoutattribute in delta_df.columns:
                    df_ctr_colsrenamed = df_ctr_colsrenamed.withColumnRenamed(col, colwithoutattribute)
                    
        nonduplicate_cols = list((Counter(extra_cols) - Counter(dup_cols)))

        if len(nonduplicate_cols) > 0:
            for col in nonduplicate_cols:
                delta_df = delta_df.withColumn(col.lower(),lit(''))
        
        delta_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_table_path)
    
    df_ctr_colsrenamed = renaming_cols_to_lowerase(df_ctr_colsrenamed)
    #df_ctr_colsrenamed.printSchema()
    
    #diff_cols = list((Counter(df_ctr_colsrenamed.columns) - Counter(delta_column_list)))

    #df_ctr_colsrenamed = df_ctr_colsrenamed.drop(*diff_cols)

    #for i in diff_cols:
    #    print('diff_cols:',i)


    # Merges the data, when record matches, it updates else inserts record
    deltaTable = DeltaTable.forPath(spark, delta_table_path)
    column_dict = createUpdateCols_dictionary(df_ctr_colsrenamed) # Get columns, which are required to be updated in delta
    #column_dict = createUpdateCols_dictionary(delta_df) # Get columns, which are required to be updated in delta
    
    deltaTable.alias("t").merge(
        df_ctr_colsrenamed.alias("s"),
        "s.contactid = t.contactid"
    ).whenMatchedUpdate(set = column_dict).whenNotMatchedInsertAll().execute()


# if deltalakeformat doesn't exist (i.e., pipeline running the first time) , then it would would enter 'if' condition otherwise in 'else'
if len(files) == 0:
    df_ctr_colsrenamed = renaming_cols_to_lowerase(df_ctr_colsrenamed)
    #df_ctr_colsrenamed.printSchema()
    
    #df_ctr_colsrenamed.printSchema()
    df_ctr_colsrenamed.write.format("delta").mode("append").option("mergeSchema","true").save(delta_table_path)

