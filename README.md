# Movie revenue predication using compound data source

This is a repository of the the RBDA final Project
The three datasets we are using are as follows:
 - Movie inherent attributes including the budgets, release date etc, which is collected from TMDb;
 - Movie production informations, including cast, director etc. which is collected from TMDb;
 - Socoal media comment, which is collected from Twitter.

## Prediction based on movie inherent features
The Movie Database (TMDb) is a community built movie and TV database. TMDb offers up to 364904 movies data, including its budget, production crews, release date, genres, language, runtime. Also, they provide a detailed information of an actor, recording all the movies he or she has participated in. 

### Data source
 - Data\/tmdb\_5000\_credits.csv - crew infomation.
 - Data\/tmdb\_5000\_movies.csv - overview infomation.

### Selected Features
We select following features to train our model:
 - Genre, genre is represented by a 0/1 matrix. There are totally 19 genres, including Fantasy, Adventure, Fantasy etc.
 - Lang, genre is represented by a 0/1 matrix. There are totally 27 languages.
 - Budget.
 - Release year.
 - Cast impression index, describing the box office appeal of the whole cast. We use an weighted average historical revenue of the all the cast members.

### Data ETL & Integration
We use Hadoop map reduce to perform data process. There are bad data records that we need to filter out from the original data source, including budget missing, cast information missing, movies that all too old that lack of statistics value. 
For the data integration stage, the steps are as follows:
 1. List all the movies that a actor has ever participated in;
 2. Find the renvenue of a all the movies;
 3. For each single movie, calculate the cast impression according to the cast list;
These steps need very complex join operations from both tables, so we use HBase to store the movie infomation and cast infomation because of its extraordinarily high scalability.

## Prediction based on social media data
Twitter is a social media site used by over three hundred million users, and movie is one of the most popular topics users are discussing about. In order to estimate a movie's social attention. We manully find the offical twitter account, extract all the data of the twitter account as features.

### Data Source
 - Data\Movie\_Twitter\_Account.csv - manually noted offical Twitter account.
 - Data\twitter\_data.json - twitter data we collected using Twitter API.

### Selected Features
 - average tweet "favorite" count.
 - maximum tweet "favorite" count.
 - overall tweet "favorite" count.
 - average tweet "retweet" count.
 - maximum tweet "retweet" count.
 - overall tweet "retweet" count.
 - follower count.

## How to run
 0. We use a virtual machine based platform to build the whole project, which needs following open source software:
 - [Virtual box](https://www.virtualbox.org/wiki/Downloads)
 - [Vagrant](https://www.vagrantup.com/downloads.html)

 1. Clone the project and start virtual machine. We have written the Vagrant script to help install all the essensial dependencies like HBase, Hadoop, Spark.
```
git git@github.com:nerokapa/RBDA_Project.git
cd RBDA_Project
vagrant up
cd /vagrant
```
 2. Process data in parallel. This procesure will produce processed\_movie\_data.dat in ./Data
```
cd ETL
# if Hadoop not installed
sh ETL_run_local.sh
# if Hadoop installed
sh ETL_run_hadoop.sh
```
 3. Process cast infomation in parallel. This procesure will produce processed\_movie\_data.dat in ./Data
 ```
cd ../Analytics
# if Hadoop not installed
sh cast_process_local.sh
# if Hadoop installed
sh cast_process_hadoop.sh
```
 4. Start HBase server and thirft service, and the data will be inserted to the hbase
 ```
 sh start_hbase.sh
 # use bellowing script to test the hbase
 python filter_test.py
 ```
 5. Model training. There is a some parameter need to filled in.
 ```
 sh run_predict.sh
 ```
 6. Data post-process
 You can use Result/harness.py to run the data post process. 
