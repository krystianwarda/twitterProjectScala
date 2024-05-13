def GER_update_user_ledger():
    import time
    import pandas as pd
    from datetime import datetime
    from tweepy import API, Cursor, OAuthHandler, TweepError
    import sqlalchemy

    
    database_username = 'root'
    database_password = 
    database_ip       =
    database_name     = 

    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                format(database_username, database_password, 
                                                        database_ip, database_name))

    consumer_key = 
    consumer_secret = 
    access_token = 
    access_secret = 

    auth = OAuthHandler(consumer_key, consumer_secret) 
    auth.set_access_token(access_token, access_secret)
    api = API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    df_ledger = pd.read_sql('SELECT user_id FROM GER_accounts_ledger', con=database_connection)
    mainAccounts = df_ledger['user_id'].tolist()


    info = []
    for i in range(0, len(mainAccounts), 100):
        if len(info) > 1000:
            data = [x._json for x in info]
            df = pd.DataFrame(data)
            df = df[['id', 'name', 'screen_name', \
            'followers_count', 'friends_count', \
            'created_at', 'statuses_count', 'favourites_count', ]]
            df['trackDate'] = int(datetime.now().strftime('%Y%m%d%H%M%S'))
            df.to_sql(con=database_connection, name='GER_users', if_exists='append', index=False)
            print(f"Dodano do bazy {len(info)} użytkowników.")
            info = []
        
        try:
            chunk = mainAccounts[i:i+100]
            info.extend(api.lookup_users(user_ids=chunk))
            print("Pobrano informację na temat kolejnych 100 użytkowników")
            time.sleep(4)
        except:
            import traceback
            traceback.print_exc()
            print('Something went wrong, skipping...')

    data = [x._json for x in info]
    df = pd.DataFrame(data)
    df = df[['id', 'name', 'screen_name', \
            'followers_count', 'friends_count', \
            'created_at', 'statuses_count', 'favourites_count', ]]
    df['trackDate'] = int(datetime.now().strftime('%Y%m%d%H%M%S'))
    df.to_sql(con=database_connection, name='GER_users', if_exists='append', index=False)
    print("Skończono")

    cursor = database_connection.connect()

    try:    
        cursor.execute('DELETE t1 FROM mydb.GER_users t1 \
            JOIN mydb.GER_users t2 \
            ON t1.id = t2.id \
            AND t2.trackDate > t1.trackDate;')
    except:
        print('test1')

    # try:    
    #     cursor.execute('DELETE t1 FROM mydb.accounts_ledger t1 \
    #         JOIN mydb.accounts_ledger t2 \
    #         ON t1.user_id = t2.user_id \
    #         AND t2.user_statuses_count > t1.user_statuses_count;').fetchall()
    # except:
    #     print('test1')


    