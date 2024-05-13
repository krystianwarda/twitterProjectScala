def data_download():
    import pymongo
    from datetime import datetime, timedelta

    db_connect = pymongo.MongoClient()

    database_name = 'twitterDB'
    db = db_connect[database_name]

    cursor = db.tweets 

    import sqlalchemy
    database_username = 
    database_password = 
    database_ip       = 
    database_name     = 'mydb'

    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                format(database_username, database_password, 
                                                        database_ip, database_name))
    def getHashtags(row):
        if not row:
            return row
        if row:
            return [x['text'] for x in row]

    def getScreenNames(row):
        if not row:
            return row
        if row:
            return [x['screen_name'] for x in row]

    def cleanSource(row):
        return row[row.find('>')+1:row.find('</a>')]

    import pandas as pd
    rowsList = []
    for document in cursor.find():
        try:
            tempRow = [
                document['created_at'],
                cleanSource(document['source']),
                document['user']['id'],
                document['user']['screen_name'],
                document['user']['friends_count'],
                document['user']['followers_count'],
                document['user']['favourites_count'],
                document['user']['created_at'],
                getHashtags(document['entities']['hashtags']),
                getScreenNames(document['entities']['user_mentions']),
                document['text']
            ]
        except KeyError:
            print('KeyError')
        rowsList.append(tempRow)
        # print(tempRow)

    # Create the pandas DataFrame
    df = pd.DataFrame(rowsList, \
        columns = [ 
        'tweet_created_at',
        'source',
        'user_id',
        'user_screen_name',
        'user_friends_count',
        'user_followers_count',
        'favourites_count',
        'user_created_at',
        'user_mentions_hashtags',
        'user_mentions_screen_name',
        'tweet_text'
        ])
    df['tweet_created_at'] = df['tweet_created_at'].apply(lambda x: datetime.strptime(x, '%a %b %d %X %z %Y') + timedelta(hours=2)) 

    df['user_mentions_hashtags'] = [','.join(map(str, l)) for l in df['user_mentions_hashtags']]
    df['user_mentions_screen_name'] = [','.join(map(str, l)) for l in df['user_mentions_screen_name']]
    df.to_sql(con=database_connection, name='PL_001', if_exists='append', index=False)
    cursor.drop()



    