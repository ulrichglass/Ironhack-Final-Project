import mysql.connector
import tweepy
from tweepy.auth import OAuthHandler
import requests

def load_key(key_lookup):
    secrets_dict={}
    secrets_file = open("../keys/secrets.txt")
    for line in secrets_file:
        (key, val) = line.split(':')
        secrets_dict[key] = val[:-1]
    return secrets_dict[key_lookup]

def open_tweepy_api():
    # authenticate you as a developer 
    auth = tweepy.OAuthHandler(load_key('API key'),load_key('API secret key'))
    # create a session that authenticates you as a user (check the documentation)
    auth.set_access_token(load_key('Access token'), load_key('Access token secret'))
    # open the API - assign to variable api
    return tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

def open_sql_server():
    # sql server parameter
    sql_user = load_key('sql_user')
    sql_password = load_key('sql_password')
    sql_host = load_key('sql_host')
    sql_database = load_key('sql_database')
    cnx = mysql.connector.connect(user = sql_user,password = sql_password, host = sql_host, database = sql_database)
    cursor = cnx.cursor()
    # return curser - assign to variable cnx and cursor
    return cnx, cursor

def close_sql_server(cnx, cursor):
    cnx.commit()
    cursor.close()
    cnx.close()

def fetch_new_tweets(tweet_mention):
    # Here one can input a new twitter account
    # tweet_mention = '@Google'
    # # Connect to SQL max tweet_ID -> set to initial since_id
    tweets_ls = []
    cnx, cursor = open_sql_server()
    sql = "SELECT MAX(tweet_id) FROM tweets WHERE tweet_mention = '"+tweet_mention+"';"
    cursor.execute(sql)
    results = cursor.fetchall()
    since_id = results[0][0]
    # For new tweet_mention
    if since_id == None:
        since_id = 0
    api = open_tweepy_api()
    # Search string exluding retweets and replies
    search = tweet_mention+' -filter:retweets -filter:replies'
    counter = 0    
    while True:
        counter += 1
        if counter > 1:
            break
        else:
            try:
                for tweet in tweepy.Cursor(api.search, q=search, since_id=since_id, tweet_mode='extended', lang='en', count=2, result_type='recent').items():
                    tweet_range = tweet.display_text_range
                    tweet_content = tweet.full_text[tweet_range[0]:tweet_range[1]]
                    tweet_date = tweet.created_at
                    tweet_id = tweet.id
                    tweet_user = tweet.user.screen_name
                    tweet_lang = 'en'
                    tweet_url = 'https://twitter.com/'+str(tweet_user)+'/status/'+str(tweet_id)
                    tweet_status = 0
                    sql = "INSERT INTO tweets (tweet_id, tweet_date, tweet_user, tweet_content, tweet_lang, tweet_url, tweet_status, tweet_mention) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                    cursor.execute(sql, (tweet_id, tweet_date, tweet_user, tweet_content, tweet_lang, tweet_url, tweet_status, tweet_mention))
            except StopIteration:
                print('Iteration stopped!')
                break
    # Close SQL Connection
    close_sql_server(cnx, cursor)
    return tweets_ls
# Talking with Azure

def connect_to_azure():
    azure_url = "https://week.cognitiveservices.azure.com/"
    sentiment_url = "{}text/analytics/v3.1-preview.3/sentiment?opinionMining=true".format(azure_url)
    return sentiment_url

def azure_header(subscription_key):
    return {"Ocp-Apim-Subscription-Key": subscription_key}

def sentiment_scores(headers, sentiment_url, document_format):
    response = requests.post(
        sentiment_url, headers=headers, json=document_format)
    return response.json()

def fetch_new_sentiments(tweet_mention):
    # Run Analysis on Microsoft Azure Cognitive Services
    # Get id, tweet and language from SQL
    # Status (0=new,1=new with sentinment,2=archived)
    cnx, cursor = open_sql_server()
    sql = ("SELECT tweet_id, tweet_content, tweet_lang FROM tweets WHERE tweet_status IN (0) AND tweet_mention='"+tweet_mention+"' ORDER BY tweet_id DESC LIMIT 500;")
    cursor.execute(sql)
    results = cursor.fetchall()
    # check if there are new tweets assigned to be checked by Microsoft Azure Cognitive Services
    if len(results) > 0:
        # Making chunks of 10 for Azure
        total_tweets = len(results)
        chunks = (total_tweets - 1) // 10 + 1
        for i in range(chunks):
            tweet_data_azure_batch = results[i*10:(i+1)*10]
            sentiment_url = connect_to_azure()
            # TO DO check if read from secrets works
            headers = {'Ocp-Apim-Subscription-Key': load_key('azure_sub_key')}
            # Create format to send to Microsoft Azure Cognitive Services
            temp_dict = []
            for t in range(len(tweet_data_azure_batch)):
                temp_dict.append({'language': tweet_data_azure_batch[t][2], 'id': tweet_data_azure_batch[t][0], 'text': tweet_data_azure_batch[t][1]})
            tweet_dict = {'documents':temp_dict}
            # Get sentiment results from Microsoft Azure Cognitive Services
            sentiments = sentiment_scores(headers, sentiment_url, tweet_dict)
            # update azure sentiments results on SQL
            for i in range(len(sentiments['documents'])):
                tweet_id = sentiments['documents'][i]['id']
                print(tweet_id)
                tweet_sentiment = sentiments['documents'][i]['sentiment']
                tweet_conf_score_pos = sentiments['documents'][i]['confidenceScores']['positive']
                tweet_conf_score_neu = sentiments['documents'][i]['confidenceScores']['neutral']
                tweet_conf_score_neg = sentiments['documents'][i]['confidenceScores']['negative']
                sql = "UPDATE tweets SET tweet_status = 1, tweet_sentiment = '"+str(tweet_sentiment)+"', tweet_conf_score_pos = "+str(tweet_conf_score_pos)+", tweet_conf_score_neu = "+str(tweet_conf_score_neu)+", tweet_conf_score_neg = "+str(tweet_conf_score_neg)+", ts_sentiment = now() WHERE tweet_id="+str(tweet_id)+";"
                cursor.execute(sql)
                sentences = sentiments['documents'][i]['sentences']
                if len(sentences)>0:
                    for s in range(len(sentences)):
                        sent_no = s
                        sent_content = sentiments['documents'][i]['sentences'][s]['text']
                        sent_sentiment = sentiments['documents'][i]['sentences'][s]['sentiment']
                        sent_con_score_pos = sentiments['documents'][i]['sentences'][s]['confidenceScores']['positive']
                        sent_con_score_neu = sentiments['documents'][i]['sentences'][s]['confidenceScores']['neutral']
                        sent_con_score_neg = sentiments['documents'][i]['sentences'][s]['confidenceScores']['negative']
                        sql = "INSERT INTO tweets_items (tweet_id, sent_no, sent_content, sent_sentiment, sent_con_score_pos, sent_con_score_neu, sent_con_score_neg, tweet_mention) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                        cursor.execute(sql, (tweet_id, sent_no, sent_content, sent_sentiment, sent_con_score_pos, sent_con_score_neu, sent_con_score_neg, tweet_mention))                       
                        if 'aspects' in sentiments['documents'][i]['sentences'][s]:
                            aspects = sentiments['documents'][i]['sentences'][s]['aspects']
                            for a in range(len(aspects)):
                                aspect_no = a
                                aspect_text = sentiments['documents'][i]['sentences'][s]['aspects'][a]['text']
                                aspect_sentiment = sentiments['documents'][i]['sentences'][s]['aspects'][a]['sentiment']
                                aspect_con_score_pos = sentiments['documents'][i]['sentences'][s]['aspects'][a]['confidenceScores']['positive']
                                aspect_con_score_neg = sentiments['documents'][i]['sentences'][s]['aspects'][a]['confidenceScores']['negative']                              
                                sql = "INSERT INTO tweets_items_aspects (tweet_id, sent_no, aspect_no, aspect_text, aspect_sentiment, aspect_con_score_pos, aspect_con_score_neg, tweet_mention) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                                cursor.execute(sql, (tweet_id, sent_no, aspect_no, aspect_text, aspect_sentiment, aspect_con_score_pos, aspect_con_score_neg, tweet_mention))                                                  
                                aspect_relations = sentiments['documents'][i]['sentences'][s]['aspects'][a]['relations']
                                for r in range(len(aspect_relations)):
                                    aspect_relation_no = r
                                    aspect_relation_type = sentiments['documents'][i]['sentences'][s]['aspects'][a]['relations'][r]['relationType']
                                    aspect_relation_ref = sentiments['documents'][i]['sentences'][s]['aspects'][a]['relations'][r]['ref']
                                    aspect_relation_ref_no = aspect_relation_ref.split('/')[-1]
                                    sql = "INSERT INTO tweets_items_aspects_rel (tweet_id, sent_no, aspect_no, aspect_relation_no, aspect_relation_type, aspect_relation_ref, aspect_relation_ref_no, tweet_mention) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                                    cursor.execute(sql, (tweet_id, sent_no, aspect_no, aspect_relation_no, aspect_relation_type, aspect_relation_ref, aspect_relation_ref_no, tweet_mention))                                                  
                        if 'opinions' in sentiments['documents'][i]['sentences'][s]:
                            opinions = sentiments['documents'][i]['sentences'][s]['opinions']
                            for o in range(len(opinions)):
                                opinion_no = o
                                opinion_text = sentiments['documents'][i]['sentences'][s]['opinions'][o]['text']
                                opinion_sentiment = sentiments['documents'][i]['sentences'][s]['opinions'][o]['sentiment']
                                opinion_con_score_pos = sentiments['documents'][i]['sentences'][s]['opinions'][o]['confidenceScores']['positive']
                                opinion_con_score_neg = sentiments['documents'][i]['sentences'][s]['opinions'][o]['confidenceScores']['negative']
                                opinion_is_negated = sentiments['documents'][i]['sentences'][s]['opinions'][o]['isNegated']
                                sql = "INSERT INTO tweets_items_opinions (tweet_id, sent_no, opinion_no, opinion_text, opinion_sentiment, opinion_con_score_pos, opinion_con_score_neg, opinion_is_negated, tweet_mention) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                cursor.execute(sql, (tweet_id, sent_no, opinion_no, opinion_text, opinion_sentiment, opinion_con_score_pos, opinion_con_score_neg, opinion_is_negated, tweet_mention))                                                  
    
    # Closing connection to SQL
    close_sql_server(cnx, cursor)

def check_yesterday_sentiments():
    cnx, cursor = open_sql_server()
    sql = ("SELECT (SUM(IF(T.tweet_sentiment = 'negative', 1, 0))/COUNT(*)) FROM tweets T WHERE T.tweet_mention='@AppleSupport' AND T.tweet_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY);")
    cursor.execute(sql)
    results = cursor.fetchall()
    print(results)
    close_sql_server(cnx, cursor)
    return results[0][0]