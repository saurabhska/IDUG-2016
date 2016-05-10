import tweepy
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from tweepy import Stream
import json
import myconfig
import mydbutils
import re
from nltk.corpus import stopwords
import string

punctuation = list(string.punctuation)
stop = stopwords.words('english') + punctuation + ['rt', 'via']

emoticons_str = r"""
    (?:
        [:=;] # Eyes
        [oO\-]? # Nose (optional)
        [D\)\]\(\]/\\OpP] # Mouth
    )"""
 
regex_str = [
    emoticons_str,
    r'<[^>]+>', # HTML tags
    r'(?:@[\w_]+)', # @-mentions
    r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
    r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs
 
    r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
    r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
    r'(?:[\w_]+)', # other words
    r'(?:\S)' # anything else
]
    
tokens_re = re.compile(r'('+'|'.join(regex_str)+')', re.VERBOSE | re.IGNORECASE)
emoticon_re = re.compile(r'^'+emoticons_str+'$', re.VERBOSE | re.IGNORECASE)
 
def tokenize(s):
    return tokens_re.findall(s)
 
def preprocess(s, lowercase=False):
    tokens = tokenize(s)
    terms_stop = list()
    if lowercase:
        tokens = [token if emoticon_re.search(token) else token.lower() for token in tokens]
        terms_stop = [term for term in tokens if term not in stop]
    return terms_stop

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

def getHashTags(tokenList):
	terms_hash = [term for term in tokenList
              if term.startswith('#')]
	return terms_hash	

def getMentions(tokenList):
	terms_mentions = [term for term in tokenList
              if term.startswith('@')]
	return terms_mentions

def getKeywords(tokenList):
	terms_only = [term for term in tokenList
              if term not in stop and
              not term.startswith(('#', '@'))] 
	return list(set(terms_only))

def getSubsetData(cleanedData):		
	subsetData = dict()
	subsetData['text'] = cleanedData['text']
	tokens = preprocess(cleanedData['text'],True)
	subsetData['hashtags'] = getHashTags(tokens)
	subsetData['mentions'] = getMentions(tokens)
	subsetData['keywords'] = getKeywords(tokens)
	subsetData['favorite_count'] = cleanedData['favorite_count']
	subsetData['retweeted'] = cleanedData['retweeted']
	subsetData['timestamp_ms'] = cleanedData['timestamp_ms']
	subsetData['retweet_count'] = cleanedData['retweet_count']
	subsetData['favorited'] = cleanedData['favorited']
	subsetData['created_at'] = cleanedData['created_at']
	subsetData['username'] = cleanedData['user']['name']
	subsetData['screenname'] = cleanedData['user']['screen_name']

class MyListener(StreamListener):
	def on_data(self, data):
		try:
			#insert data into MongoDB
			cleanedData = json.loads(data)
			mydbutils.insert('rawTweetDump',cleanedData)
			subsetData = getSubsetData(cleanedData)
			print(subsetData)
			print(type(subsetData))
			print(cleanedData['text'])
			mydbutils.insert('subsetTweetDump',json.loads(subsetData))
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
	twitter_stream.filter(track=[myconfig.searchHashTags])