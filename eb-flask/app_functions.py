import pandas as pd
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
    # tweet_mention = '@Google'
    # Get tweet data into dataframe
    # Initialilazation dataframe
    columns = ['tweet_id', 'tweet_date', 'tweet_user', 'tweet_content', 'tweet_lang', 'tweet_url', 'tweet_status', 'tweet_mention']
    tweets_df = pd.DataFrame(columns=columns)
    tweets_df['tweet_id'] = tweets_df['tweet_id'].astype('int64')
    tweets_df['tweet_status'] = tweets_df['tweet_status'].astype('int64')
    # Connect to SQL max tweet_ID -> set to initial since_id
    cnx, cursor = open_sql_server()
    sql = "SELECT MAX(tweet_id) FROM tweets WHERE tweet_mention = '"+tweet_mention+"';"
    cursor.execute(sql)
    results = cursor.fetchall()
    since_id = results[0][0]
    # For new tweet_mention
    if since_id == None:
        since_id = 0

    # Tweepy: load new tweets into pandas dataframe
    api = open_tweepy_api()
    search = tweet_mention+' -filter:retweets -filter:replies'
    counter = 0    
    while True:
        counter += 1
        if counter > 1:
            break
        else:
            try:
                for tweet in tweepy.Cursor(api.search, q=search, since_id=since_id, tweet_mode='extended', lang='en', result_type='recent').items():
                    tweet_range = tweet.display_text_range
                    tweet_content = tweet.full_text[tweet_range[0]:tweet_range[1]]
                    tweet_date = tweet.created_at
                    tweet_id = tweet.id
                    tweet_user = tweet.user.screen_name
                    tweet_url = 'https://twitter.com/'+str(tweet_user)+'/status/'+str(tweet_id)
                    tweet_status = 0
                    tweets_df = tweets_df.append({'tweet_id':tweet_id, 'tweet_date':tweet_date, 'tweet_user':tweet_user, 'tweet_content':tweet_content, 'tweet_lang':'en', 'tweet_url':tweet_url, 'tweet_status':tweet_status, 'tweet_mention':tweet_mention}, ignore_index = True) 
            except StopIteration:
                print('Iteration stopped!')
                break
    # Uploading to SQL
    # creating column list for insertion
    cols = "`,`".join([str(i) for i in tweets_df.columns.tolist()])
    cols
    # Move older tweets to archive
    sql = "UPDATE tweets SET tweet_status = 2 WHERE tweet_status = 1 AND tweet_mention = '"+tweet_mention+"';"
    cursor.execute(sql)
    # Insert DataFrame records one by one.
    for i,row in tweets_df.iterrows():
        sql = "INSERT INTO `tweets` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cursor.execute(sql, tuple(row))
    close_sql_server(cnx, cursor)

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

def azure_sentiment(text):

    sentiment_url = connect_to_azure()
    headers = {'Ocp-Apim-Subscription-Key': 'e15d26dd829642ca95c20c308f98b0ed'}
    text = text
    tweet_dict = {'documents':[{'id': 1, 'text': text, 'language': 'en'}]}
    sentiments = sentiment_scores(headers, sentiment_url, tweet_dict)

    # result = [tuple(('Input','',text,'','',''))]
    # result = ['Your text: '+text+'']
    text_sentiment = sentiments['documents'][0]['sentiment']
    text_conf_score_pos = sentiments['documents'][0]['confidenceScores']['positive']
    text_conf_score_neu = sentiments['documents'][0]['confidenceScores']['neutral']
    text_conf_score_neg = sentiments['documents'][0]['confidenceScores']['negative']
    # result.append('Overall Score: '+text_sentiment+' @ confidence levels of ['+str(text_conf_score_pos)+','+str(text_conf_score_neu)+','+str(text_conf_score_neg)+'] (pos,neu,neg)')
    result = [tuple(('Total','',text,text_sentiment,str(text_conf_score_pos),str(text_conf_score_neu),str(text_conf_score_neg)))]
    sentences = sentiments['documents'][0]['sentences']
    if len(sentences)>0:
        for s in range(len(sentences)):
            sent_no = s
            sent_content = sentiments['documents'][0]['sentences'][s]['text']
            sent_sentiment = sentiments['documents'][0]['sentences'][s]['sentiment']
            sent_con_score_pos = sentiments['documents'][0]['sentences'][s]['confidenceScores']['positive']
            sent_con_score_neu = sentiments['documents'][0]['sentences'][s]['confidenceScores']['neutral']
            sent_con_score_neg = sentiments['documents'][0]['sentences'][s]['confidenceScores']['negative']
            result.append(tuple(('Sentence',sent_no, sent_content, sent_sentiment,str(sent_con_score_pos),str(sent_con_score_neu),str(sent_con_score_neg))))
            # result.append('  Sentence '+str(sent_no+1)+': '+str(sent_content)+' rated '+str(sent_sentiment)+' @ confidence levels of ['+str(sent_con_score_pos)+','+str(sent_con_score_neu)+','+str(sent_con_score_neg)+'] (pos,neu,neg)')
            if 'aspects' in sentiments['documents'][0]['sentences'][s]:
                # result.append('    Within this sentence the following aspects were identified:')
                aspects = sentiments['documents'][0]['sentences'][s]['aspects']
                for a in range(len(aspects)):
                    aspect_no = a
                    aspect_text = sentiments['documents'][0]['sentences'][s]['aspects'][a]['text']
                    aspect_sentiment = sentiments['documents'][0]['sentences'][s]['aspects'][a]['sentiment']
                    aspect_con_score_pos = sentiments['documents'][0]['sentences'][s]['aspects'][a]['confidenceScores']['positive']
                    aspect_con_score_neg = sentiments['documents'][0]['sentences'][s]['aspects'][a]['confidenceScores']['negative']
                    # result.append('      '+str(aspect_no+1)+' '+str(aspect_text)+' rated '+str(aspect_sentiment)+' @ confidence levels of ['+str(aspect_con_score_pos)+','+str(aspect_con_score_neg)+'] (pos,neg)')
                    # result.append(tuple(('Aspect',aspect_no, aspect_text, aspect_sentiment,str(aspect_con_score_pos),'',str(aspect_con_score_neg))))
                    aspect_relations = sentiments['documents'][0]['sentences'][s]['aspects'][a]['relations']
                    for r in range(len(aspect_relations)):
                        # result.append('        Matched opinions:')
                        aspect_relation_no = r
                        aspect_relation_type = sentiments['documents'][0]['sentences'][s]['aspects'][a]['relations'][r]['relationType']
                        aspect_relation_ref = sentiments['documents'][0]['sentences'][s]['aspects'][a]['relations'][r]['ref']
                        aspect_relation_ref_no = aspect_relation_ref.split('/')[-1]
                        opinion_text = sentiments['documents'][0]['sentences'][s]['opinions'][int(aspect_relation_ref_no)]['text']
                        opinion_sentiment = sentiments['documents'][0]['sentences'][s]['opinions'][int(aspect_relation_ref_no)]['sentiment']
                        opinion_con_score_pos = sentiments['documents'][0]['sentences'][s]['opinions'][int(aspect_relation_ref_no)]['confidenceScores']['positive']
                        opinion_con_score_neg = sentiments['documents'][0]['sentences'][s]['opinions'][int(aspect_relation_ref_no)]['confidenceScores']['negative']
                        opinion_is_negated = sentiments['documents'][0]['sentences'][s]['opinions'][int(aspect_relation_ref_no)]['isNegated']
                        result.append(tuple(('Aspect/Opinion',aspect_relation_ref_no, aspect_text+' ('+opinion_text+') [neg: '+str(opinion_is_negated)+']', opinion_sentiment,str(opinion_con_score_pos),'',str(opinion_con_score_neg))))
                        # result.append('          '+str(aspect_relation_ref_no)+': '+opinion_text+' ('+opinion_sentiment+') @ confidence levels of ['+str(opinion_con_score_pos)+','+str(opinion_con_score_neg)+'] (pos,neg), opinion is negated: '+str(opinion_is_negated))                                
    return result

def fetch_new_sentiments(tweet_mention):
    # Run Analysis on Microsoft Azure Cognitive Services
    # Get id, tweet and language from SQL into a dataframe
    # Status (0=new,1=new with sentinment,2=archived)
    cnx, cursor = open_sql_server()
    sql = ("SELECT tweet_id, tweet_content, tweet_lang FROM tweets WHERE tweet_status IN (0) AND tweet_mention='"+tweet_mention+"' LIMIT 1000;")
    print(sql)
    cursor.execute(sql)
    results = cursor.fetchall()
    # check if there are new tweets assigned to be checked by Microsoft Azure Cognitive Services
    if len(results) > 0:

        # Dataframe with total tweets to send
        tweet_data_azure = pd.DataFrame(results)
        tweet_data_azure.columns = ['id', 'text', 'language']
        # Initialize result dataframes
        columns = ['tweet_id', 'tweet_sentiment', 'tweet_conf_score_pos', 'tweet_conf_score_neu', 'tweet_conf_score_neg', 'tweet_mention']
        tweets_azure_results = pd.DataFrame(columns=columns)
        columns = ['tweet_id', 'sent_no', 'sent_content', 'sent_sentiment', 'sent_con_score_pos', 'sent_con_score_neu', 'sent_con_score_neg', 'tweet_mention']
        tweets_items_azure_results = pd.DataFrame(columns=columns)
        columns = ['tweet_id', 'sent_no', 'aspect_no', 'aspect_text', 'aspect_sentiment', 'aspect_con_score_pos', 'aspect_con_score_neg', 'tweet_mention']
        tweets_items_aspects_azure_results = pd.DataFrame(columns=columns)
        columns = ['tweet_id', 'sent_no', 'aspect_no', 'aspect_relation_no', 'aspect_relation_type', 'aspect_relation_ref', 'aspect_relation_ref_no', 'tweet_mention']
        tweets_items_aspects_rel_azure_results = pd.DataFrame(columns=columns)
        columns = ['tweet_id', 'sent_no', 'opinion_no', 'opinion_text', 'opinion_sentiment', 'opinion_con_score_pos', 'opinion_con_score_neg', 'opinion_is_negated', 'tweet_mention']
        tweets_items_opinions_azure_results = pd.DataFrame(columns=columns)
        # Making chunks of 10 for Azure
        total_tweets = len(tweet_data_azure)
        chunks = (total_tweets - 1) // 10 + 1
        for i in range(chunks):
            tweet_data_azure_batch = tweet_data_azure[i*10:(i+1)*10]
            sentiment_url = connect_to_azure()
            headers = {'Ocp-Apim-Subscription-Key': 'e15d26dd829642ca95c20c308f98b0ed'}
            tweet_dict = {'documents':list(tweet_data_azure_batch.T.to_dict().values())}
            sentiments = sentiment_scores(headers, sentiment_url, tweet_dict)
            # update azure sentiments results on SQL
            for i in range(len(sentiments['documents'])):
                tweet_id = sentiments['documents'][i]['id']
                tweet_sentiment = sentiments['documents'][i]['sentiment']
                tweet_conf_score_pos = sentiments['documents'][i]['confidenceScores']['positive']
                tweet_conf_score_neu = sentiments['documents'][i]['confidenceScores']['neutral']
                tweet_conf_score_neg = sentiments['documents'][i]['confidenceScores']['negative']
                tweets_azure_results = tweets_azure_results.append({'tweet_id':tweet_id, 'tweet_sentiment':tweet_sentiment, 'tweet_conf_score_pos':tweet_conf_score_pos, 'tweet_conf_score_neu':tweet_conf_score_neu, 'tweet_conf_score_neg':tweet_conf_score_neg, 'tweet_mention':tweet_mention}, ignore_index = True) 
                sentences = sentiments['documents'][i]['sentences']
                if len(sentences)>0:
                    for s in range(len(sentences)):
                        sent_no = s
                        sent_content = sentiments['documents'][i]['sentences'][s]['text']
                        sent_sentiment = sentiments['documents'][i]['sentences'][s]['sentiment']
                        sent_con_score_pos = sentiments['documents'][i]['sentences'][s]['confidenceScores']['positive']
                        sent_con_score_neu = sentiments['documents'][i]['sentences'][s]['confidenceScores']['neutral']
                        sent_con_score_neg = sentiments['documents'][i]['sentences'][s]['confidenceScores']['negative']
                        tweets_items_azure_results = tweets_items_azure_results.append({'tweet_id':tweet_id, 'sent_no':sent_no, 'sent_content':sent_content, 'sent_sentiment':sent_sentiment, 'sent_con_score_pos':sent_con_score_pos, 'sent_con_score_neu':sent_con_score_neu, 'sent_con_score_neg':sent_con_score_neg, 'tweet_mention':tweet_mention}, ignore_index = True)        
                        if 'aspects' in sentiments['documents'][i]['sentences'][s]:
                            aspects = sentiments['documents'][i]['sentences'][s]['aspects']
                            for a in range(len(aspects)):
                                aspect_no = a
                                aspect_text = sentiments['documents'][i]['sentences'][s]['aspects'][a]['text']
                                aspect_sentiment = sentiments['documents'][i]['sentences'][s]['aspects'][a]['sentiment']
                                aspect_con_score_pos = sentiments['documents'][i]['sentences'][s]['aspects'][a]['confidenceScores']['positive']
                                aspect_con_score_neg = sentiments['documents'][i]['sentences'][s]['aspects'][a]['confidenceScores']['negative']
                                tweets_items_aspects_azure_results = tweets_items_aspects_azure_results.append({'tweet_id':tweet_id, 'sent_no':sent_no, 'aspect_no':aspect_no, 'aspect_text':aspect_text, 'aspect_sentiment':aspect_sentiment, 'aspect_con_score_pos':aspect_con_score_pos, 'aspect_con_score_neg':aspect_con_score_neg, 'tweet_mention':tweet_mention}, ignore_index = True)                   
                                aspect_relations = sentiments['documents'][i]['sentences'][s]['aspects'][a]['relations']
                                for r in range(len(aspect_relations)):
                                    aspect_relation_no = r
                                    aspect_relation_type = sentiments['documents'][i]['sentences'][s]['aspects'][a]['relations'][r]['relationType']
                                    aspect_relation_ref = sentiments['documents'][i]['sentences'][s]['aspects'][a]['relations'][r]['ref']
                                    aspect_relation_ref_no = aspect_relation_ref.split('/')[-1]
                                    tweets_items_aspects_rel_azure_results = tweets_items_aspects_rel_azure_results.append({'tweet_id':tweet_id, 'sent_no':sent_no, 'aspect_no':aspect_no, 'aspect_relation_no':aspect_relation_no, 'aspect_relation_type':aspect_relation_type, 'aspect_relation_ref':aspect_relation_ref, 'aspect_relation_ref_no':aspect_relation_ref_no, 'tweet_mention':tweet_mention}, ignore_index = True)
                        if 'opinions' in sentiments['documents'][i]['sentences'][s]:
                            opinions = sentiments['documents'][i]['sentences'][s]['opinions']
                            for o in range(len(opinions)):
                                opinion_no = o
                                opinion_text = sentiments['documents'][i]['sentences'][s]['opinions'][o]['text']
                                opinion_sentiment = sentiments['documents'][i]['sentences'][s]['opinions'][o]['sentiment']
                                opinion_con_score_pos = sentiments['documents'][i]['sentences'][s]['opinions'][o]['confidenceScores']['positive']
                                opinion_con_score_neg = sentiments['documents'][i]['sentences'][s]['opinions'][o]['confidenceScores']['negative']
                                opinion_is_negated = sentiments['documents'][i]['sentences'][s]['opinions'][o]['isNegated']
                                tweets_items_opinions_azure_results = tweets_items_opinions_azure_results.append({'tweet_id':tweet_id, 'sent_no':sent_no, 'opinion_no':opinion_no, 'opinion_text':opinion_text, 'opinion_sentiment':opinion_sentiment, 'opinion_con_score_pos':opinion_con_score_pos, 'opinion_con_score_neg':opinion_con_score_neg, 'opinion_is_negated':opinion_is_negated, 'tweet_mention':tweet_mention}, ignore_index = True)                   

        tweets_azure_results['tweet_id'] = tweets_azure_results['tweet_id'].astype('int64')
        tweets_items_azure_results['tweet_id'] = tweets_items_azure_results['tweet_id'].astype('int64')
        tweets_items_azure_results['sent_no'] = tweets_items_azure_results['sent_no'].astype('int64') 

        # Sent Azure results to RDS
        # creating column list for insertion
        # tweet results
        if len(tweets_azure_results)>0:
            for i,row in tweets_azure_results.iterrows():
                tweet_id = row['tweet_id']
                tweet_sentiment = row['tweet_sentiment']
                tweet_conf_score_pos = row['tweet_conf_score_pos']
                tweet_conf_score_neu = row['tweet_conf_score_neu']
                tweet_conf_score_neg = row['tweet_conf_score_neg']    
                sql = "UPDATE tweets SET tweet_status = 1, tweet_sentiment = '"+str(tweet_sentiment)+"', tweet_conf_score_pos = "+str(tweet_conf_score_pos)+", tweet_conf_score_neu = "+str(tweet_conf_score_neu)+", tweet_conf_score_neg = "+str(tweet_conf_score_neg)+", ts_sentiment = now() WHERE tweet_id="+str(tweet_id)+";"
                cursor.execute(sql)

        # tweet items results - To Do: CHECK IF EXISTS!
        if len(tweets_items_azure_results)>0:
            cols = "`,`".join([str(i) for i in tweets_items_azure_results.columns.tolist()])
            # Insert DataFrame records one by one.
            for i,row in tweets_items_azure_results.iterrows():
                sql = "INSERT INTO `tweets_items` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                cursor.execute(sql, tuple(row))
            
        # tweet aspects results - To Do: CHECK IF EXISTS!
        if len(tweets_items_aspects_azure_results)>0:
            cols = "`,`".join([str(i) for i in tweets_items_aspects_azure_results.columns.tolist()])
            # Insert DataFrame records one by one.
            for i,row in tweets_items_aspects_azure_results.iterrows():
                sql = "INSERT INTO `tweets_items_aspects` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                cursor.execute(sql, tuple(row))

        # tweet aspects relation results - To Do: CHECK IF EXISTS!
        if len(tweets_items_aspects_rel_azure_results)>0:
            cols = "`,`".join([str(i) for i in tweets_items_aspects_rel_azure_results.columns.tolist()])
            # Insert DataFrame records one by one.
            for i,row in tweets_items_aspects_rel_azure_results.iterrows():
                sql = "INSERT INTO `tweets_items_aspects_rel` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                cursor.execute(sql, tuple(row))
                
        # tweet opinions results - To Do: CHECK IF EXISTS!
        if len(tweets_items_opinions_azure_results)>0:
            cols = "`,`".join([str(i) for i in tweets_items_opinions_azure_results.columns.tolist()])
            # Insert DataFrame records one by one.
            for i,row in tweets_items_opinions_azure_results.iterrows():
                sql = "INSERT INTO `tweets_items_opinions` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                cursor.execute(sql, tuple(row))
    
    # Closing connection to SQL
    close_sql_server(cnx, cursor)