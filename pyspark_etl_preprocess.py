
# coding: utf-8

# # ETL - Preprocesado
# 
# Utiliza Spark para procesar un dataset de Tweets en el formato original y responde a las preguntas. 
# 
# El archivo original que contiene los tweets contiene una carpeta por cada día, que a su vez contiene una carpeta por cada hora. Dentro de estas hay un archivo comprimido bz2 por cada minuto, que contiene un archivo JSON con los tweets en todos los idiomas enviados en ese intervalo. En la carpeta /files tienes los tweets del día 30/9/2011 en este formato (solo una hora, para ahorrar espacio).
# 
# Utiliza Spark para extraer los tweets, interpretar el JSON y formatear los tweets en el formato adecuado. 
# Deberás crear un array que contenga las rutas a los distintos archivos y transformarlo en un RDD con el que trabajar. 
# Después deberás escribir una función con la que mapear una ruta de archivo a un listado de tuplas de la forma (**usuario, fecha, contenido_del_tweet**). A partir de ahí podrás manejar el RDD de la misma manera que en la práctica 2. 
# Puedes coger ideas de este script que lee todos los archivos de un directorio y los descomprime del formato bz2.

# In[2]:

import sys, os, bz2, json
from pyspark import SparkContext

tweets = []
json_files_paths = []
path = './tweets/30/01/'

def has_required_properties(tweet):
    return 'user' in tweet and 'created_at' in tweet and 'text' in tweet

for(dirpath,dirnames,files) in os.walk(path):
    for filename in files: 
        filepath = os.path.join(dirpath, filename) 
        json_file_path = filepath[:-4]        
        
        zipfile = bz2.BZ2File(filepath)
        data = zipfile.read()        
        open(json_file_path, 'wb').write(data)
        json_files_paths.append(json_file_path)

        with open(json_file_path, 'r') as json_stream:
            file_line = json_stream.readline()
            while file_line:
                json_parsed = json.loads(file_line)
                if has_required_properties(json_parsed):
                    tweets.append(json_parsed)
                file_line = json_stream.readline()


# In[ ]:

# Remove json files created
for json_path in json_files_paths:
    os.remove(json_path)


# In[3]:

len(tweets)


# In[4]:

# Tweet structure sample
print json.dumps(tweets[0], indent=4, sort_keys=True)


# In[5]:

# Create context and load tweets

sc = SparkContext("local", "Tutorial")
rdd_tweets = sc.parallelize(tweets)
type(rdd_tweets)


# In[376]:

# Filter to get only tweets with language 'es'

def is_es_language(tweet):
    return tweet['user']['lang'] == 'es'

def format_tweet(tweet):
    return { 
        'user': tweet['user']['screen_name'],
        'text': tweet['text'], 
        'timestamp': tweet['created_at']
    }

rdd_tweets_es = (rdd_tweets
                 .filter(is_es_language) # Get tweets with language 'es'
                 .map(format_tweet)
                )


rdd_tweets_es.persist()
rdd_tweets_es.count()


# In[378]:

# Formatted tweet sample
print json.dumps(rdd_tweets_es.take(10), indent=4, sort_keys=True)


# ## Part 1

# In[379]:

# Sort users by tweets made

total_tweets_by_user = (rdd_tweets_es
                        .map(lambda tweet: (tweet['user'], 1)) # Tuples (user, 1)
                        .reduceByKey(lambda value1, value2: value1 + value2) # Group by key (user) and sum #value
                        .sortBy(lambda tweet: tweet[1], ascending = False) # Sort by #value
                       )


# In[380]:

# Top 10 users with more tweets
total_tweets_by_user.take(10)


# ## Part 2

# In[381]:

# Sort by most recurrent words inside tweets' text

def get_text_splited_by_word(tweet):
    return tweet['text'].split()

tweets_words_sorted = (rdd_tweets_es
                       .flatMap(get_text_splited_by_word) # Make list with words from each tweet
                       .map(lambda word: (word, 1)) # Tuples (word, 1)
                       .reduceByKey(lambda value1, value2: value1 + value2) # Group by word and sum #value
                       .sortBy(lambda _tuple: _tuple[1], ascending=False) # Sort by #value
                      )


# In[382]:

# Top common words inside tweets
tweets_words_sorted.take(10)


# ## Part 3

# In[383]:

# Sort by most recurrent words inside tweets' text without most recurrent words inside the tweets (not in stopwords)

stopwords = ['como', 'pero', 'o', 'al', 'mas', 'esta', 'le', 'cuando', 'eso', 'su', 'porque',             'd', 'del', 'los', 'mi', 'si', 'las', 'una', 'q', 'ya', 'yo', 'tu', 'el', 'ella',             'a', 'ante', 'bajo', 'cabe', 'con', 'contra', 'de', 'desde', 'en', 'entre', 'hacia',             'hasta', 'para', 'por', 'segun', 'sin', 'so', 'sobre', 'tras', 'que', 'la', 'no', 'y',             'el', 'me', 'es', 'te', 'se', 'un', 'lo']

def is_not_stopword(item):
    return item[0] not in stopwords

tweets_words_sorted.persist()
tweets_words_sorted_filtered = (tweets_words_sorted
                                .filter(is_not_stopword) # Remove stopwords from sorted tuples (word, #value)
                                .sortBy(lambda _tuple: _tuple[1], ascending=False) # Sort by #value
                               )


# In[384]:

# Top common words inside tweets (not in stopwords)
tweets_words_sorted_filtered.take(10)


# ## Part 4

# In[387]:

# Get scope (number of apperances) by hashtag

def get_hashtags(tweet):
    hashtags=tweet['text'].split('#')[1:]
    return map(lambda x: x if ' ' not in x.lower() else x.partition(' ')[0].lower(), hashtags)

def has_elements(item):
    return len(item) > 0

hashtags_sorted = (rdd_tweets_es
                   .flatMap(get_hashtags) # Extract list of hashtag from text
                   .map(lambda hashtag: (hashtag, 1)) # Tuples (hashtag, 1)
                   .reduceByKey(lambda value1, value2: value1 + value2) # Group by hashtag and compute #value
                   .sortBy(lambda _tuple: _tuple[1], ascending=False) # Sort by #value
                  )

hashtags_sorted.persist()
hashtags_sorted_list = hashtags_sorted.collect()
hashtags_sorted_list


# In[388]:

# Sort users by sum of used hashtags' scope

def get_scope(value):
    scope = [_tuple2_[1] for _tuple2_ in hashtags_sorted_list if _tuple2_[0] == value]
    return scope[0]

hashtags_grouped_by_user_sorted = (rdd_tweets_es
                                   .map(lambda tweet: (tweet['user'], get_hashtags(tweet))) # Tuple (user,[hashtag])
                                   .filter(lambda item: has_elements(item[1])) # Remove tuples without hashtags
                                   .flatMapValues(lambda value: value) # Make list of (user, hashtag)
                                   .map(lambda item: (item[0], get_scope(item[1]))) # Tuple (user, scope)
                                   .reduceByKey(lambda v1, v2: v1 + v2) # Group by user and sum #value
                                   .sortBy(lambda item: item[1], ascending=False) # Sort by #value
                                  )


# In[389]:

# Top 10 user by scope of used hashtags
hashtags_grouped_by_user_sorted.take(10)


# ## Part 5

# In[393]:

# Get useless hashtags (scope = 1)
useless_hashtags = map(lambda pair: pair[0], filter(lambda pair: pair[1] == 1,hashtags_sorted_list))
useless_hashtags


# In[396]:

# Get users sorted by more useless hashtags created

def is_useless_hashtag(item):
    return item[1] in useless_hashtags

useless_hashtags_creators = (rdd_tweets_es
                             .map(lambda tweet: (tweet['user'], get_hashtags(tweet))) # Tuple (user,[hashtag])
                             .filter(lambda item: has_elements(item[1])) # Remove tuples without hashtags
                             .flatMapValues(lambda value: value) # Make list of (user, hashtag)
                             .filter(is_useless_hashtag) # Filter to get only useless hashtags
                             .map(lambda item: (item[0], 1)) # Tuple (hashtag, 1)
                             .reduceByKey(lambda v1, v2: v1 + v2) # Group by user and sum #value
                             .sortBy(lambda item: item[1], ascending = False) # Sort by #value
                            )


# In[397]:

# Top 10 creators of useless hashtags
useless_hashtags_creators.take(10)


# ## Part 6

# In[398]:

# Users who created hashtags with more scope

def get_creator(pairs):
    pair_as_list_sorted = sorted(list(pairs), key = lambda item: item[1])
    return pair_as_list_sorted[0][0]

def format_tuples(item):
    return (item[1], (item[0]['user'], item[0]['timestamp']))

hashtag_creators_by_scope = (rdd_tweets_es
                             .sortBy(lambda tweet: tweet['timestamp'], ascending=True)  # Sort by timestamp    
                             .map(lambda tweet: (tweet, get_hashtags(tweet))) # Extract hashtags from text
                             .filter(lambda item: has_elements(item[1])) # Remove tuples without hashtags
                             .flatMapValues(lambda value: value) # Tuples (user, hashtag)
                             .map(format_tuples) # Create tuples (hashtag, (user, timestamp))
                             .groupByKey() # Group by hashtag
                             .map(lambda (key, value): (key, get_creator(value))) # Get hashtag creators
                             .map(lambda item: (item[1], get_scope(item[0]))) # Tuples (user, scope)
                             .reduceByKey(lambda v1, v2: v1 + v2) # Compute the accumulated scope from each user
                             .sortBy(lambda item: item[1], ascending=False) # Sort by accumulated scope
                            )


# In[399]:

hashtag_creators_by_scope.take(10)

