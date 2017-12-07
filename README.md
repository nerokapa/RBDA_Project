# Movie box office predication using compound data source

This is a repository of the the RBDA final Project
The three datasets we are using are as follows:
 - Movie inherent attributes including the budgets, release date etc. This data is collected from TMDb;
 - Movie production informations, including cast, director etc. This data is collected from TMDb;
 - Movie related comment. This data is collected from Twitter.

## Prediction based on movie inherent features
The Movie Database (TMDb) is a community built movie and TV database. TMDb offers up to 364904 movies data, including its budget, production crews, release date, genres, language, runtime. Also, they provide a detailed information of an actor, recording all the movies he or she has participated in. 

### Selected Features
We select following features to train our model:
 - Genre, genre is represented by a 0/1 matrix. There are dozens of genres, like Fantasy, Adventure, Fantasy etc.
 - Budget (dollar).
 - Release year.
 - Cast impression describe the box office appeal of the cast. We use an weighted average historical revenue of the all the cast members to represent the cast impression.
 - Director impression describe the box office appeal of the director. Like cast appeal, we use the average historical revenue of the director to represent the director appeal.

### Data ETL & Integration
We use Hadoop map reduce to perform data process. There are bad data records that we need to filter out from the original data source, including budget missing, cast information missing, movies that all too old that lack of statistics value. 
For the data integration stage, we need to merge all the historical revenues all a single actor, which is very complex join operation from multiple tables. We select HBase as the database because of its extraordinarily high scalability. For the cast revenue table, we use cast ID as rowkey, different movie ID as column key. 

### Calculation platform
We use PySpark as the platform to calculate regression model.

## Prediction based on real time social media reaction
Twitter is a social media site used by over three hundred million users, and movie is one of the most popular topics users are discussing about. We extract two features from the raw Twitter data, that is, popularity and subjective evaluation.

### Popularity analysis
Popularity is calculated by the count of tweets mentioning a certain movie over overall tweets. The mentioning judgement is performed by fuzzy match and hashtag match.

### subjective evaluation analysis 
Firstly we will select the relative tweets like we have done in the previous stage. Secondly We perform a sentiment analysis to give each tweet a score to indicate whether the tweet is positive or negative.
