import re
import tweepy
from tweepy.errors import TweepyException
import datetime

from util.date_util import get_date_timezone, get_datetime

class TweetAPI:

    def __init__(self, consumer_key, consumer_secret, access_token, access_token_secret):
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        self.conToken = tweepy.API(auth, wait_on_rate_limit=True, retry_count=5,
                                   retry_delay=10)

    def __replace(self, text, entry=[], exit=''):
        for caracter in entry:
            text = text.replace(caracter, exit)
        return text

    def __remove_emogi(self, text):
        base = 'abcdefghijklmnopqrstuvwxyz- 1234567890'
        result = ''
        for caractere in text:
            if caractere in base:
                result += caractere
        return result

    def __clean_caracteres(self, text):
        text = self.__replace(text, 'á|à|ã|ä|â'.split("|"), 'a')
        text = self.__replace(text, 'é|è|ë|ê'.split("|"), 'e')
        text = self.__replace(text, 'í|ì|ï|î'.split("|"), 'i')
        text = self.__replace(text, 'ó|ò|ö|ô'.split("|"), 'o')
        text = self.__replace(text, 'ú|ù|ü'.split("|"), 'u')
        text = self.__replace(text, ['ç'], 'c')
        text = self.__replace(text, '!|?|"|*|°|º|ª|§|)|(|]|[|{|}'.split("|"), '')
        text = self.__replace(text, '"', '')
        text = self.__replace(text, "'", '')
        text = self.__replace(text, '|', '')
        text = self.__replace(text, "/|\\|]|[|=|+|_|)|(|&|%|$|#|!|{|}", '')
        text = self.__replace(text, '\n|\t|\a|\b|\f|\r|-'.split("|"), ' ')
        return self.__remove_emogi(text)

    def __clean_tweet(self, text):
        text = text.lower()
        text = re.sub(r'RT+', '', text)
        text = re.sub(r'@\S+', '', text)
        text = re.sub(r'https?\S+', '', text)
        text = re.sub(r'http?\S+', '', text)
        return self.__clean_caracteres(text)

    def search_by_keyword(self, keywords=[], count=10, result_type='mixed', lang='pt', tweet_mode='extended',
                          date=get_datetime()):
        result = self.conToken.search_tweets(q=' OR '.join(keywords), tweet_mode=tweet_mode,
                                      count=count, result_type=result_type,
                                      lang=lang, include_entities=True)
        tweets = []
        for tw in result:
            date_tw = tw.created_at
            if datetime.datetime(day=date_tw.day, month=date_tw.month, year=date_tw.year, hour=date_tw.hour, minute=date_tw.minute, second=date_tw.second) >= date:
                if self.__contains_keyword(keywords, tw.full_text):
                    tweets.append(tw)
        return self.__prepare_tweets_list(tweets)

    def __contains_keyword(self, keywords, text):
        if len(keywords) > 0:
            for keyword in keywords:
                if keyword.lower() in text.lower():
                    return True
            return False
        else:
            return True
    def get_friends(self, count=200):
        friends = self.conToken.get_friends(count=count)
        result = []
        for friend in friends:
            result += [friend.screen_name]
        return result

    def search_by_friends(self, count=5,
                          tweet_mode='extended',
                          keywords=[],
                          friends=[],
                          date=get_datetime()):
        tweets = []
        if len(friends) > 0:
            for friend in friends:
               try:
                    result = self.conToken.user_timeline(
                        tweet_mode=tweet_mode,
                        screen_name=friend,
                        count=count)
                    for tw in result:
                        if get_date_timezone(tw.created_at) >= get_date_timezone(date):
                            if self.__contains_keyword(keywords, tw.full_text):
                                tweets.append(tw)
                    print(f"Searching: {friend} Sucess!!")
               except TweepyException:
                   print(f"Searching: {friend} Fail!!")
        return self.__prepare_tweets_list(tweets)

    def __prepare_tweets_list(self, tweets):
        tweets_data_list = []
        for tweet in tweets:
            if not 'retweeted_status' in dir(tweet):
                tweet_text = self.__clean_tweet(tweet.full_text)
                if len(tweet_text) > 0:
                    tweets_data = {
                        'user': tweet.user.screen_name,
                        'local': tweet.user.location,
                        'text_standart': tweet_text,
                        'text': tweet.full_text,
                        'date': get_date_timezone(tweet.created_at).strftime('%d/%m/%Y %H:%M:%S'),
                        'id': tweet.id
                    }
                    tweets_data_list.append(tweets_data)
        return tweets_data_list
