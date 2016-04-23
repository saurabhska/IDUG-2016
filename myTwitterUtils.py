import tweepy
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from tweepy import Stream
import json
import myconfig
import mydbutils

#Initialize our connection to twitter
def initializeConnection():
	#Use OAuth interface to authorise the app to access Twitter
	auth = OAuthHandler(myconfig.consumer_key, myconfig.consumer_secret)
	auth.set_access_token(myconfig.access_token, myconfig.access_secret)
	api = tweepy.API(auth)
	return api,auth

#read our own timeline (i.e. our Twitter homepage)
def readTwitterTimeline(api,numTweets):
	api,auth = initializeConnection()
	for status in tweepy.Cursor(api.home_timeline).items(numTweets):
    	# Process a single status
		print(status.text)

class MyListener(StreamListener):
	def on_data(self, data):
		try:
			#insert data into MongoDB
			mydbutils.insert('rawTweetDump',json.loads(data))
			#write data into a dump file
			with open('tweets.json', 'a') as f:
				f.write(data)
				return True
		except BaseException as e:
			print("Error on_data: %s" % str(e))
		return True
 
	def on_error(self, status):
		print(status)
		return True		

#starts twitter streaming and looking for searchHashTags in the received data
def startStreaming():
	#api variable is the entry point to perform operations on Twitter. 
	api,auth = initializeConnection()
	twitter_stream = Stream(auth, MyListener())
	twitter_stream.filter(track=myconfig.searchHashTags)