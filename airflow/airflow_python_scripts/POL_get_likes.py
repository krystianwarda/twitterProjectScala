def POL_get_likes():
    import configparser
    from tweepy import API, Cursor, OAuthHandler, TweepError
    import pandas as pd
    import numpy as np
    import time
    from datetime import datetime

    import sqlalchemy
    database_username = 'root'
    database_password = 
    database_ip       = 
    database_name     = 'mydb'

    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                format(database_username, database_password, 
                                                        database_ip, database_name))
                                                        
    cursor = database_connection.connect()

    consumer_key = 
    consumer_secret = 
    access_token = 
    access_secret = 

    auth = OAuthHandler(consumer_key, consumer_secret) 
    auth.set_access_token(access_token, access_secret)
    api = API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    # for users already in base
    df_users = pd.read_sql('SELECT id, screen_name, favourites_count FROM users', con=database_connection)
    testDict= dict(zip(df_users.screen_name, df_users.favourites_count))
    df_check = pd.read_sql('SELECT * FROM POL_accounts_ledger_likes', con=database_connection)
    df_check['user_favourites_count_now'] = df_check.user_screen_name.map(testDict)
    df_check['user_favourites_count'] = df_check['user_favourites_count'].astype(int)
    df_check['statuses_diff'] = df_check['user_favourites_count_now'] - df_check['user_favourites_count']
    df_check_temp = df_check[df_check['statuses_diff'] > 150]
    accountsToRecheckDict = dict(zip(df_check_temp.user_screen_name , df_check_temp.statuses_diff))
    print(len(accountsToRecheckDict))

    idNameDict = dict(zip(df_users['screen_name'], df_users['id']))

    i=0
    for tempName, tempStatusNumber in accountsToRecheckDict.items():
        i+=1
        try:

            # ids = []
            # for fid in Cursor(api.favorites, screen_name=tempName).items(200):
            #     ids.append(fid)
            # data = [x._json for x in ids]
            # df = pd.DataFrame(data)
            info = []
            info.extend(api.favorites(screen_name=tempName,count=190))
            data = [x._json for x in info]
            df = pd.DataFrame(data)
            print(len(df.index))

            # get user: id, name, status count
            info = []
            info.extend(api.lookup_users(user_ids=[int(idNameDict[tempName])]))
            data = [x._json for x in info]
            df_update = pd.DataFrame(data)
            df_update = df_update[['id', 'screen_name', 'favourites_count']].rename(columns={'id':'user_id', 'screen_name': 'user_screen_name', 'favourites_count':'user_favourites_count'})
            df_update.to_sql(con=database_connection, name='POL_accounts_ledger_likes', if_exists='append', index=False)

            df= df[['created_at', 'id', 'text', 'truncated', 'entities', 'source', 'in_reply_to_status_id', 'in_reply_to_user_id', 'in_reply_to_screen_name', 'user',\
                'retweet_count', 'favorite_count']]
            df['user_screen_name'] = tempName
            df['tweet_user_id'] = df['user'].apply(lambda x: x.get("id", 'NaN'))
            df['tweet_user_screen_name'] = df['user'].apply(lambda x: x.get("screen_name", 'NaN'))
            df['tweet_user_followers_count'] = df['user'].apply(lambda x: x.get("followers_count", 'NaN'))
            df['tweet_user_friends_count'] = df['user'].apply(lambda x: x.get("friends_count", 'NaN'))
            df['tweet_user_statuses_count'] = df['user'].apply(lambda x: x.get("statuses_count", 'NaN'))
            df['tweet_user_favourites_count'] = df['user'].apply(lambda x: x.get("favourites_count", 'NaN'))
            df['tweet_user_created_at'] = df['user'].apply(lambda x: x.get("created_at", 'NaN'))
            del df['user']
            df['tweet_entities_hashtags'] = df['entities'].apply(lambda x: x.get("hashtags", 'NaN'))
            df['tweet_entities_user_mentions'] = df['entities'].apply(lambda x: x.get("user_mentions", 'NaN'))
            del df['entities']
            df['tweet_entities_screen_name_list'] = df['tweet_entities_user_mentions'].apply(lambda z: [x.get("screen_name", 'NaN') for x in [singleJSON for singleJSON in z]])
            df['tweet_entities_user_id_list'] = df['tweet_entities_user_mentions'].apply(lambda z: [x.get("id", 'NaN') for x in [singleJSON for singleJSON in z]])
            df['tweet_entities_hashtags_list'] = df['tweet_entities_hashtags'].apply(lambda z: [x.get("text", 'NaN') for x in [singleJSON for singleJSON in z]])
            df['tweet_source'] = df['source'].apply(lambda s: s[s.find('>')+1:s.find('</a>')])
            del df['source']
            del df['tweet_entities_user_mentions']
            del df['tweet_entities_hashtags']
            df['tweet_entities_screen_name_list'] = [','.join(map(str, l)) for l in df['tweet_entities_screen_name_list']]
            df['tweet_entities_user_id_list'] = [','.join(map(str, l)) for l in df['tweet_entities_user_id_list']]
            df['tweet_entities_hashtags_list'] = [','.join(map(str, l)) for l in df['tweet_entities_hashtags_list']]
            df['trackDate'] = int(datetime.now().strftime('%Y%m%d%H%M%S'))
            df.to_sql(con=database_connection, name='POL_likes001', if_exists='append', index=False)
            print(f"Likes of account {tempName} has been added. ({i})")
            time.sleep(15)
        except Exception as e:
            print(f"Query for account {tempName} failed. ({i}) ({e})")


    i=0
    for tempUser, tempStatusNumber in accountsToRecheckDict.items():

        print(tempUser)
        df_userTweets = pd.read_sql(f"SELECT * FROM POL_likes001 WHERE `user_screen_name` = '{tempUser}'", con=database_connection)
        print(len(df_userTweets.index))
        time.sleep(3)
        cursor.execute(f"DELETE FROM POL_likes001 WHERE `user_screen_name` = '{tempUser}';")
        df_userTweets = df_userTweets.drop_duplicates(subset=['id', 'created_at', 'text'])
        print(len(df_userTweets.index))
        df_userTweets.to_sql(con=database_connection, name='POL_likes001', if_exists='append', index=False)

    for tempUser, tempStatusNumber in accountsToRecheckDict.items():
        print(tempUser)
        df_userTweets = pd.read_sql(f"SELECT * FROM POL_accounts_ledger_likes WHERE `user_screen_name` = '{tempUser}'", con=database_connection)
        time.sleep(3)
        cursor.execute(f"DELETE FROM POL_accounts_ledger_likes WHERE `user_screen_name` = '{tempUser}';")
        print(df_userTweets)
        df_userTweets = df_userTweets.drop_duplicates(subset=['user_id', 'user_screen_name'], keep='last')
        print(df_userTweets)
        df_userTweets.to_sql(con=database_connection, name='POL_accounts_ledger_likes', if_exists='append', index=False)