import pandas as pd, tweepy, pymysql, re
from utils.config import API_KEY, SECRET_API_KEY, BEARER_TOKEN, ACCESS_TOKEN, ACCESS_SECRET_TOKEN
from nltk.corpus import stopwords

# SQL Sentences

CREATE_DATABASE = """
CREATE DATABASE Prueba_Tecnica;
"""
CREATE_TWEETS = """
CREATE TABLE IF NOT EXISTS Tweets (
    Author_id VARCHAR(255) NOT NULL,
    Message_id VARCHAR(255) NOT NULL,
    Message VARCHAR(280),
    Date VARCHAR(255),
    Replys INTEGER(255),
    Retweets INTEGER(255),
    Likes INTEGER(255),
    Quotes INTEGER(255),
    PRIMARY KEY(Message_id)
);
"""
INSERT_TWEETS = """
INSERT INTO Tweets (Author_id, Message_id, Message, Date, Replys, Retweets, Likes, Quotes)

values({}, {}, {}, {}, {}, {}, {}, {});
"""

CREATE_USERS = """
CREATE TABLE Users (
    Author_id VARCHAR(255) NOT NULL UNIQUE, 
    Creators_Username VARCHAR(255),
    PRIMARY KEY(Author_id)
);
"""

INSERT_USERS = """
INSERT INTO Users (Author_id, Creators_Username)

values({}, {});
"""

CREATE_VIEW = """
CREATE VIEW VW_Tweets_Users as
SELECT tw.Author_id, Users.Creators_Username, tw.Message_id, tw.message, 
tw.Date, tw.Replys, tw.Retweets, tw.Likes, tw.Quotes
FROM Tweets as tw
INNER JOIN Users Users ON tw.Author_id=Users.Author_id;
"""

# Clean text

class Clean():
    def _init__():
        pass


    def clean_emojis(self, text): # Clean emojis from the text
        regrex_pattern = re.compile(pattern = "["
            u"\U0001F600-\U0001F64F"  # emoticons
            u"\U0001F300-\U0001F5FF"  # symbols & pictographs
            u"\U0001F680-\U0001F6FF"  # transport & map symbols
            u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
            u"\U0001F900-\U0001F9FF"  # supplemental symbols and pictographs
                                "]+", flags = re.UNICODE)
        return regrex_pattern.sub(r'', text)


    def remove_links(self, df): # Remove links from the text
        return " ".join(['{link}' if ('http') in word else word for word in df.split()]) # Found them with regex


    def signs_tweets(self, tweet): # Clean punctuation marks from the text
        
        # Found them with regex
        punctuation_marks = re.compile("(\.)|(\;)|(\:)|(\!)|(\?)|(\¿)|(\@)|(\,)|(\")|(\()|(\))|(\[)|(\])|(\d+)")

        return punctuation_marks.sub("", tweet.lower())


    def remove_stopwords(self, df): # Remove spanish stopwords like "Y", "Si", "Ya", etc.

        spanish_stopwords = stopwords.words("spanish") # Load Stopwords

        return " ".join([word for word in df.split() if word not in spanish_stopwords]) # Split the text and remove the stopword 


    def remove_mentions_hashtags_retweets(self, text): # Clean mentions in comments

        text = re.sub(r"@[A-Za-z0-9]", "", text) # Remove mentions
        text = re.sub(r"@[A-Za-zA-Z0-9]", "", text) # Remove mentions
        text = re.sub(r"@[A-Za-z]", "", text) # Remove mentions

        text = re.sub(r"rt[\s   ]", "", text.lower()) # Remove retweets

        text = re.sub(r"#", "", text) # Remove hashtags

        return text


    def remove_no_sense_by_len(self, text):

        if len(text.split()) > 2:

            return text # Filter texts by len, if a text is like "Hello" or was no more than a link the function returns nothing

# Tweepy consultation

class GetInfo():

    def __init__(self):
        self.client = tweepy.Client(bearer_token=BEARER_TOKEN, consumer_key=API_KEY, consumer_secret=SECRET_API_KEY, 
        access_token=ACCESS_TOKEN, access_token_secret=ACCESS_SECRET_TOKEN) # Client session access


    def get_client_tweets(self, id, start_time, end_time, expansions="entities.mentions.username", 
    tweet_fields=["context_annotations", "created_at", "author_id", "public_metrics"]):

        tweets = tweepy.Paginator(self.client.get_users_mentions, id=id, start_time=start_time, end_time=end_time,
        expansions=expansions, tweet_fields=tweet_fields, max_results=100).flatten(limit=float("inf")) # Attacking Tweepy

        # Create the lists for generate the record to append to the pandas.DataFrame object
        message_id = []

        messages = []

        moment_tweet = []

        authors_id = []

        usernames = []

        replys = []

        retweets = []

        likes = []

        quotes = []

        for tweet in tweets:

            message_id.append(str(tweet.id)) # Tweet id

            messages.append(str(tweet.text)) # Content of the message

            moment_tweet.append(tweet.created_at.__str__().split()[0]) # Datetime of the tweet

            authors_id.append(str(tweet.author_id)) # Author id

            usernames.append(str(tweet.entities["mentions"][0]["username"])) # Tweet creator username

            # Metrics of the tweet

            (replys.append(tweet.public_metrics.get("reply_count")) if tweet.public_metrics.get("reply_count") != None
            else replys.append(0)) # Replys

            (retweets.append(tweet.public_metrics.get("retweet_count")) if tweet.public_metrics.get("retweet_count") != None
            else retweets.append(0))  # Retweets

            (likes.append(tweet.public_metrics.get("like_count")) if tweet.public_metrics.get("like_count") != None
            else likes.append(0)) # Likes

            (quotes.append(tweet.public_metrics.get("quote_count")) if tweet.public_metrics.get("quote_count") != None
            else quotes.append(0)) # Quotes

        return pd.DataFrame({"Message_id" : message_id, "Message" : messages,

        "Date" : moment_tweet, "Author_id" : authors_id, "Creator's_Username" : usernames, 

        "Replys": replys, "Retweets" : retweets, "Likes" : likes, "Quotes" : quotes})

# Database management

class DBController():
    def __init__(self, host, password, user):
        self.host = host
        self.password=password  # Setting host, password and username to connect with a MySQL database
        self.user=user

    def querySQL(self, db, query, parameters=[]):
        con = pymysql.connect(host=self.host, password=self.password, user=self.user, cursorclass=pymysql.cursors.DictCursor)
        cur = con.cursor()
        cur.execute(f"USE {db}", parameters)
        cur.execute(query, parameters)

        keys = [] # Getting columns names
        for item in cur.description:
            keys.append(item[0])

        rows = [] # Getting records of the database
        for row in cur.fetchall(): # Assinign a dictionary for every record in the fetch
            ix_clave = 0
            d = {}
            for column in keys: # Assigning a key for every record of the row
                d[column] = row[ix_clave]
                ix_clave += 1
            rows.append(d)

        con.close()
        return rows # Retrun a list of dictionaries


    def changeSQL(self, db, query, parameters=[], create=False):
        con = pymysql.connect(host=self.host, password=self.password, user=self.user, cursorclass=pymysql.cursors.DictCursor)
        cur = con.cursor()

        if create == False:
            cur.execute(f"USE {db}", []) # This is if not any data to insert, in case of create a table or similar things

        cur.execute(query, parameters) # Parameters is the data what you want to insert in the database

        con.commit()
        con.close()