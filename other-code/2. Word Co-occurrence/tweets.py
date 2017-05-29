'''
    A Python script to Fetch Tweets from twitter 
    on a topic/hashtag specified using arguments.

    Input: Search term - Command Line
    Output:  outputFileName (saved as outputFileName.txt) - Command Line

    Usage: python/python2.7 <SearchTerm> <OutputFile>
'''
import tweepy
import sys
import json
import os

consumer_key = 'yiYmkK3D1Lkce9KH37Q8RQXdz'
consumer_secret = 'VLNYdjRXNQ3Tp6Mo5k7YEA9x8NPHdtADIRRM8DhM6Xv8Sx8quz'
access_token = '1605909084-OcgJIf5nid4Axf6QZSbPGKIhQMyWwm9v73DjxXC'
access_secret = 'OK1I2aSsjvbjP1erIAw4a3hB9B6el9YdKoA5XYZ6zXXya'

auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)

api = tweepy.API(auth, wait_on_rate_limit=True,
                   wait_on_rate_limit_notify=True)

if not api:
    print ("Can't Authenticate")
    sys.exit(-1)

n = len(sys.argv)
searchQuery = ""
for i in xrange(1,n-1):
    searchQuery = searchQuery + sys.argv[i] + " "
print searchQuery
maxTweets = 3000 
tweetsPerQry = 100
fName =  str(sys.argv[n-1]) + ".txt"

tweetCount = 0
print("Downloading {0} tweets".format(maxTweets))

with open(fName, 'w') as f:
    while tweetCount < maxTweets:
        try:
            new_tweets = api.search(q=searchQuery, count=tweetsPerQry, lang='en')
            if not new_tweets:
                print("No more tweets found")
                break
            for tweet in new_tweets:
                f.write(tweet.text.encode("utf-8") + "\n")
            tweetCount += len(new_tweets)
            print("Downloaded {0} tweets".format(tweetCount))
            max_id = new_tweets[-1].id
        except tweepy.TweepError as e:
            # Just exit if any error
            print("some error : " + str(e))
            break

print ("Downloaded {0} tweets, Saved to {1}".format(tweetCount, fName))