def FRA_dl_sus_tweets():
    import configparser
    from tweepy import API, Cursor, OAuthHandler, TweepError
    import pandas as pd
    import numpy as np
    import time
    from datetime import datetime



    import sqlalchemy
    database_username = 
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

    df = pd.read_sql('SELECT screen_name, statuses_count FROM FRA_users', con=database_connection)

    # try:    
    #     cursor.execute('DELETE t1 FROM mydb.accounts_ledger t1 \
    #         JOIN mydb.accounts_ledger t2 \
    #         ON t1.user_id = t2.user_id \
    #         AND t2.user_statuses_count > t1.user_statuses_count;').fetchall()
    # except:
    #     print('test1')
  
    testDict= dict(zip(df.screen_name, df.statuses_count))
    df_check = pd.read_sql('SELECT * FROM FRA_accounts_ledger', con=database_connection)
    df_check['user_statuses_count_now'] = df_check.user_screen_name.map(testDict)
    df_check['user_statuses_count'] = df_check['user_statuses_count'].astype(int)
    df_check['statuses_diff'] = df_check['user_statuses_count_now'] - df_check['user_statuses_count']
    df_check_temp = df_check[df_check['statuses_diff'] > 2500]
    accountsToRecheckDict = dict(zip(df_check_temp.user_screen_name , df_check_temp.statuses_diff))
    print(accountsToRecheckDict)
    print(len(accountsToRecheckDict))

    i=0
    for tempName, tempStatusNumber in accountsToRecheckDict.items():
        i+=1
        # if tempName in restartList:
        #     continue
        try:
            source__user_screen_name = tempName
            ids = []

            for fid in Cursor(api.user_timeline, id=source__user_screen_name, exclude='retweets').items(tempStatusNumber):
                ids.append(fid)
            data = [x._json for x in ids]
            df = pd.DataFrame(data)
            # get user: id, name, status count
            tempId  = df.iloc[-1]['user']['id']
            tempName = df.iloc[-1]['user']['screen_name']
            tempStatus = df['user'][0]['statuses_count']
            
            df_ledger = pd.DataFrame(np.array([[tempId, tempName, tempStatus]]),
                columns=['user_id', 'user_screen_name', 'user_statuses_count'])
            df_ledger.to_sql(con=database_connection, name='FRA_accounts_ledger', if_exists='append', index=False)

            if 'retweeted_status' not in df.columns:
                df['retweeted_status'] = np.nan
                
            if 'quoted_status' not in df.columns:
                df['quoted_status'] = np.nan

            if 'quoted_status_id' not in df.columns:
                df['quoted_status_id'] = np.nan

            df = df[['id','created_at', 'user', 'entities','source','text', 'is_quote_status', 'retweet_count', 'favorite_count',\
                'in_reply_to_status_id', 'in_reply_to_user_id', 'in_reply_to_screen_name' ,'quoted_status_id', 'quoted_status' ,'retweeted_status',\
                ]]

            df['quoted_status_id'] = df['quoted_status'].apply(lambda x: x.get("id", 'NaN') if (type(x) is dict)==True else x)
            df['quoted_status_created_at'] = df['quoted_status'].apply(lambda x: x.get("created_at", 'NaN') if (type(x) is dict)==True else x)
            df['quoted_status_text'] = df['quoted_status'].apply(lambda x: x.get("text", 'NaN') if (type(x) is dict)==True else x)
            df['quoted_status_user'] = df['quoted_status'].apply(lambda x: x.get("user", 'NaN') if (type(x) is dict)==True else x)
            df['quoted_status_user_id'] = df['quoted_status_user'].apply(lambda x: x.get("id", 'NaN') if (type(x) is dict)==True else x)
            df['quoted_status_user_screen_name'] = df['quoted_status_user'].apply(lambda x: x.get("screen_name", 'NaN') if (type(x) is dict)==True else x)
            df['quoted_status_user_created_at'] = df['quoted_status_user'].apply(lambda x: x.get("created_at", 'NaN') if (type(x) is dict)==True else x)


            df['retweeted_status_id'] = df['retweeted_status'].apply(lambda x: x.get("id", 'NaN') if (type(x) is dict)==True else x)
            df['retweeted_status_created_at'] = df['retweeted_status'].apply(lambda x: x.get("created_at", 'NaN') if (type(x) is dict)==True else x)
            df['retweeted_status_text'] = df['retweeted_status'].apply(lambda x: x.get("text", 'NaN') if (type(x) is dict)==True else x)
            df['retweeted_status_user'] = df['retweeted_status'].apply(lambda x: x.get("user", 'NaN') if (type(x) is dict)==True else x)
            df['retweeted_status_user_id'] = df['retweeted_status_user'].apply(lambda x: x.get("id", 'NaN') if (type(x) is dict)==True else x)
            df['retweeted_status_user_screen_name'] = df['retweeted_status_user'].apply(lambda x: x.get("screen_name", 'NaN') if (type(x) is dict)==True else x)
            df['retweeted_status_user_created_at'] = df['retweeted_status_user'].apply(lambda x: x.get("created_at", 'NaN') if (type(x) is dict)==True else x)

            del df['retweeted_status_user']
            del df['quoted_status']
            del df['retweeted_status']
            del df['quoted_status_user']

            df['user_screen_name'] = df['user'].apply(lambda x: x.get("screen_name", 'NaN'))
            df['entities_user_mentions'] = df['entities'].apply(lambda x: x.get("user_mentions", 'NaN'))
            df['entities_hashtags'] = df['entities'].apply(lambda x: x.get("hashtags", 'NaN'))
            df['entities_screen_name_list'] = df['entities_user_mentions'].apply(lambda z: [x.get("screen_name", 'NaN') for x in [singleJSON for singleJSON in z]])
            df['entities_user_id_list'] = df['entities_user_mentions'].apply(lambda z: [x.get("id", 'NaN') for x in [singleJSON for singleJSON in z]])
            df['entities_hashtags_list'] = df['entities_hashtags'].apply(lambda z: [x.get("text", 'NaN') for x in [singleJSON for singleJSON in z]])
            df['source'] = df['source'].apply(lambda s: s[s.find('>')+1:s.find('</a>')])
            del df['entities']
            del df['user']
            del df['entities_user_mentions']
            del df['entities_hashtags']
            df['entities_screen_name_list'] = [','.join(map(str, l)) for l in df['entities_screen_name_list']]
            df['entities_user_id_list'] = [','.join(map(str, l)) for l in df['entities_user_id_list']]
            df['entities_hashtags_list'] = [','.join(map(str, l)) for l in df['entities_hashtags_list']]
            df['trackDate'] = int(datetime.now().strftime('%Y%m%d%H%M%S'))
            df.to_sql(con=database_connection, name='FRA_tweets001', if_exists='append', index=False)
            print(f"Account {tempName} has been added. ({i})")
            time.sleep(120)
        except:
            print(f"Query for account {tempName} failed. ({i})")
            




