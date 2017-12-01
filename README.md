# RBDA_Project
This is a repository of the the RBDA final Porject

We used two data sets, one is the IMDB data set, another is the twitter dataset that we collect ourself.

## Twitter Data
### Sentimental analysis 
There are one MapReduce program that do analytics on the twitter data. That is under the 
RBDA_Project/twitter_based_predict/Analytics/ folder. 
It judges whether the tweets contains a comment onto a movie and do sentimental analysis to 
give a score to indicate whether the tweet is positive or negative toward the movie.
And the reducer will compute the average sentimental score of a movie.
We can run this code using the Python Streaming API.


## IMBD part
There are two data source of IMDB, one of which is tmdb_5000_credits.csv, storing the cast infomation, another one of which is tmdb_5000_movies.csv, storing the budget, genre and the final revenue. We select following data as the training features: cast inpression index, genre and budget. 
 - genre, genre is represented by a 0/1 matrix. There are dozens of protential genres, like Fantasy/Adventure/Fantasy/..
 - budget is a integer.
 - cast impression is overal analytics of the movie dataset. Basically it's an weighted average of the movies that the a actor have ever participated in.

## Regression -- on-build
The script under RBDA_Project/twitter_based_predict/Regression/ is an on-build regression model 
that trains a box-office predicter online. We try to use the spark streaming api to get data from the Hbase to
train this model. It's currently not functional.
