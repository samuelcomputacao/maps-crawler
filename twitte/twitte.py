import os, sys

sys.path.append('%s%s%s' % (os.getcwd(), os.path.sep, 'twitte'))

from tweetAPI import TweetAPI
from datetime import datetime, timedelta

CONSUMER_KEY = 'UZLEno0H4UqDnMbzp06lAO4K4'
CONSUMER_SECRET = '8Q60W8qUu0ZqGEMO7zpeD9MoGZD6HFTTVgrJa72VVnykVCoFwv'
ACCESS_TOKEN = '1071095366-zFazweFNeItzWbqbgojoBQNEKVUfFA1GOSyRivt'
ACCESS_TOKEN_SECRET = 'YNBChOqrv805TEW8uBD5HsagHSOeX6ufZzzPFBuNPLgx5'

api = TweetAPI(consumer_key=CONSUMER_KEY, consumer_secret=CONSUMER_SECRET,
               access_token=ACCESS_TOKEN, access_token_secret=ACCESS_TOKEN_SECRET)

keywords = ['ferid', 'acidente', 'mort', 'choque', 'trajédia', 'atropel',
            'vitima', 'colisão', 'virada', 'virou', 'engavetamento', 'abalroamento', 'batida', 'capota',
            'incendio', 'capotamento'
                        'morre', 'faleceu', 'saiu da pista', 'tombamento', 'tombou', 'bateu']


def findFriends(count=200):
    return api.get_friends(count=count)


def findTwittes(friends=[]):
    date = datetime.today()
    date = datetime(year=date.year, month=date.month, day=date.day)
    date = date - timedelta(days=50)
    print(f'Buscando Twitters em {date}')
    return api.search_by_friends(keywords=keywords, date=date, friends=friends)
