# Movie box office predication using compound data source

This is a repository of the the RBDA final Project
The three datasets we are using are as follows:
 - Movie inherent attributes including the budgets, release date etc, which is collected from TMDb;
 - Movie production informations, including cast, director etc. which is collected from TMDb;
 - Socoal media comment, which data is collected from Twitter.

## Prediction based on movie inherent features
The Movie Database (TMDb) is a community built movie and TV database. TMDb offers up to 364904 movies data, including its budget, production crews, release date, genres, language, runtime. Also, they provide a detailed information of an actor, recording all the movies he or she has participated in. 

### Selected Features
We select following features to train our model:
 - Genre, genre is represented by a 0/1 matrix. There are totally 19 genres, like Fantasy, Adventure, Fantasy etc.
 - Lang, genre is represented by a 0/1 matrix. There are totally 27 languages. Most of languages of the movie are English.
 - Budget.
 - Release year.
 - Cast impression describe the box office appeal of the cast. We use an weighted average historical revenue of the all the cast members to represent the cast impression.

### Data ETL & Integration
We use Hadoop map reduce to perform data process. There are bad data records that we need to filter out from the original data source, including budget missing, cast information missing, movies that all too old that lack of statistics value. 
For the data integration stage, the steps are as follows:
 1. List all the movies that a actor has ever participated in;
 2. Find the renvenue of a all the movies;
 3. For each single movie, calculate the cast impression according to the cast list;
These steps need very complex join operations from multiple tables. We use HBase to store the movie infomation and cast infomation, because of its extraordinarily high scalability. We use two separate HBase tables, with one storing the movie infomation, and the other storing the cast infomation.

### Regression Platform
We use PySpark as the platform to calculate regression model.

## Prediction based on real time social media reaction
Twitter is a social media site used by over three hundred million users, and movie is one of the most popular topics users are discussing about. We extract two features from the raw Twitter data, that is, popularity and subjective evaluation.

### Selected Features
 - Popularity analysis. Popularity is calculated by the count of tweets mentioning a certain movie over overall tweets. The mentioning judgement is performed by fuzzy match and hashtag match.
 - Subjective evaluation analysis, after we have performed the tweets match in the previous stage. We can perform a sentiment analysis on the mentioned tweets to sentiment score to indicate whether a certain tweet is positive or negative, and then give a overall sentiment summary.

### Data Process
During the data filtering stage, we keep running into problems that, the way we tweet is more casual as we though it would be.
We use fuzzywuzzy to perform a tweet mentioning judgement. Fuzzy string matching like a boss. It uses Levenshtein Distance to calculate the differences between sequences in a simple-to-use package.
We use nltk to perform sentiment analysis. 

## How to run
 0. We use a virtual machine based platform to build the whole project, which needs following open source software:
 - [Virtual box](https://www.virtualbox.org/wiki/Downloads)
 - [Vagrant](https://www.vagrantup.com/downloads.html)

 1. Clone the project and start virtual machine. We have written the Vagrant script to help install all the essensial dependencies like HBase, Hadoop, Spark.
```
git git@github.com:nerokapa/RBDA_Project.git
cd RBDA_Project
vagrant up
```
 2. Process data in a map-reduce way
 
