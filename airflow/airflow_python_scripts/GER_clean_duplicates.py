def GER_clean_duplicates():
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

    df = pd.read_sql('SELECT screen_name, statuses_count FROM GER_users', con=database_connection)

    # try:    
    #     cursor.execute('DELETE t1 FROM mydb.accounts_ledger t1 \
    #         JOIN mydb.accounts_ledger t2 \
    #         ON t1.user_id = t2.user_id \
    #         AND t2.user_statuses_count > t1.user_statuses_count;').fetchall()
    # except:
    #     print('test1')
  
    testDict= dict(zip(df.screen_name, df.statuses_count))
    df_check = pd.read_sql('SELECT * FROM GER_accounts_ledger', con=database_connection)
    df_check['user_statuses_count_now'] = df_check.user_screen_name.map(testDict)
    df_check['user_statuses_count'] = df_check['user_statuses_count'].astype(int)
    df_check['statuses_diff'] = df_check['user_statuses_count_now'] - df_check['user_statuses_count']
    df_check_temp = df_check[df_check['statuses_diff'] > 1500]
    accountsToRecheckDict = dict(zip(df_check_temp.user_screen_name , df_check_temp.statuses_diff))
    print(accountsToRecheckDict)
    print(len(accountsToRecheckDict))

    time.sleep(2)
    for tempUser, tempStatusNumber in accountsToRecheckDict.items():
        print(tempUser)
        time.sleep(2)
        df_userTweets = pd.read_sql(f"SELECT * FROM GER_tweets001 WHERE `user_screen_name` = '{tempUser}'", con=database_connection)
        print(len(df_userTweets.index))
        time.sleep(3)
        cursor.execute(f"DELETE FROM GER_tweets001 WHERE `user_screen_name` = '{tempUser}';")
        df_userTweets = df_userTweets.drop_duplicates(subset=['id', 'created_at'])
        print(len(df_userTweets.index))
        df_userTweets.to_sql(con=database_connection, name='GER_tweets001', if_exists='append', index=False)

    for tempUser, tempStatusNumber in accountsToRecheckDict.items():
        print(tempUser)
        time.sleep(2)
        df_userTweets = pd.read_sql(f"SELECT * FROM GER_accounts_ledger WHERE `user_screen_name` = '{tempUser}'", con=database_connection)
        time.sleep(3)
        cursor.execute(f"DELETE FROM GER_accounts_ledger WHERE `user_screen_name` = '{tempUser}';")
        print(df_userTweets)
        df_userTweets = df_userTweets.drop_duplicates(subset=['user_id', 'user_screen_name'], keep='last')
        print(df_userTweets)
        df_userTweets.to_sql(con=database_connection, name='GER_accounts_ledger', if_exists='append', index=False)





