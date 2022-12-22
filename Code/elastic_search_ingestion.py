#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from elasticsearch import Elasticsearch, helpers
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql.functions import col
from functools import reduce
import uuid


# In[2]:


spark = SparkSession.builder.config("spark.sql.caseSensitive", "true").getOrCreate()


# In[3]:


model = ALSModel.load("/user/amm9801_nyu_edu/project/als_model_merged")


# In[4]:


itemfactors = spark.read.parquet("/user/amm9801_nyu_edu/project/itemfactors", inferSchema=True, header=True)


# In[5]:

item_df = itemfactors.select('id','features').withColumnRenamed("id", "asinIdx")

#items_frame = itemfactors.select('id','features').toPandas().rename(columns={"id": "asinIdx", "features": "features"})


# In[6]:


#item_df = spark.createDataFrame(items_frame)


# In[7]:


#item_df.show(5)


# In[8]:


merged_reviews_transformed = spark.read.csv("/user/amm9801_nyu_edu/project/merged_reviews_transformed", inferSchema=True, header=True)


# In[9]:


merged_reviews_transformed.show(5)


# In[10]:


review_joined_df = item_df.join(merged_reviews_transformed, 'asinIdx','inner').select("asin", "features")


# In[11]:


#review_joined_df.show(5)


# In[29]:


review_joined_distinct_df = review_joined_df.dropDuplicates(["asin"])


# In[30]:


#review_joined_distinct_df.show(5)


# In[12]:


# spark.conf.set("spark.sql.caseSensitive", "true")
raw_metadata_books = spark.read.json('/user/amm9801_nyu_edu/project/meta_Books.json')
book_metadata = raw_metadata_books.select('asin', 'title', 'description', 'brand', 'price')


# In[13]:


raw_metadata_clothing = spark.read.json('/user/sg6482_nyu_edu/project/meta_Clothing_Shoes_and_Jewelry.json')
clothing_metadata = raw_metadata_clothing.select('asin', 'title', 'description', 'brand', 'price')


# In[14]:


raw_metadata_electronic = spark.read.json('/user/sa6142_nyu_edu/project/electronics/meta_Electronics.json')
electronic_metadata = raw_metadata_electronic.select('asin', 'title', 'description', 'brand', 'price')


# In[15]:


def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)


# In[16]:


merged_metadata = unionAll(book_metadata, clothing_metadata, electronic_metadata)


# In[17]:


from pyspark.sql.functions import col, concat_ws
merged_metadata_string = merged_metadata.withColumn("description",concat_ws(" ",col("description")))
#merged_metadata_string.show(5)
#merged_metadata_string.printSchema()


# In[18]:


merged_metadata_string.write.mode('overwrite').option("header", True).csv("/user/sa6142_nyu_edu/project/merged_metadata")


# In[19]:


merged_metadata_string = spark.read.csv("/user/sa6142_nyu_edu/project/merged_metadata", inferSchema=True, header=True)


# In[31]:


final_data = merged_metadata_string.join(review_joined_distinct_df, "asin", "inner")


# In[23]:


final_data.write.mode('overwrite').option("header", True).parquet("/user/sa6142_nyu_edu/project/final_data")


# In[22]:


# final_data = spark.read.csv("/user/sa6142_nyu_edu/project/final_data", inferSchema=True, header=True)


# In[32]:


#final_data.show(5)


# In[33]:


from elasticsearch import Elasticsearch


# In[34]:


es_client = Elasticsearch('https://my-deployment-ccde32.es.us-east4.gcp.elastic-cloud.com', http_auth=('elastic','ibi6dHbvjbMem8xbqxMknZgA'))


# In[35]:


index_name = "amazon_product_index"
try:
    es_client.indices.delete(index=index_name)
except Exception as e:
    print(e)
index_body = {
      'settings': {
        'number_of_shards': 1,
        'number_of_replicas': 0,
        'analysis': {
          "filter":{  
            "english_stop":{
              "type":"stop",
              "stopwords":"english"
            },
            "english_stemmer":{
              "type":"stemmer",
              "language":"english"
            }
          },  
          "analyzer": {
            "stem_english": { 
              "type":"custom",
              "tokenizer":"standard",
              "filter":[
                "lowercase",
                "english_stop",
                "english_stemmer"
              ]
            }
        }
      }},
      'mappings': {
          'properties': {
              'asin' : {'type': 'text'},
              'price' : {'type': 'text'},
              'description': {
                  'type': 'text',
                  'analyzer': 'stem_english'
              },
              'title': {
                  'type': 'text',
                  'analyzer': 'stem_english'
              },
              'brand': {
                  'type': 'text',
                  'analyzer': 'stem_english'
              },
              "profile_vector": {
                "type": "dense_vector",
                "dims": 48
              }
          }
      }
    }
es_client.indices.create(index=index_name,body=index_body)


# In[36]:


es_dataset = [{"_index": index_name, "_id": uuid.uuid4(), "_source" : {"title": doc[1]["title"], "description": doc[1]["description"],"asin": doc[1]["asin"], "brand": doc[1]["brand"], "profile_vector": doc[1]["features"], "price": doc[1]["price"] }} for doc in final_data.toPandas().iterrows()]
#bulk insert them
helpers.bulk(es_client, es_dataset)


# In[ ]:




