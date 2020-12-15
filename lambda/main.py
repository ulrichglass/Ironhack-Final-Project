import json
import lambda_functions as lf
import boto3
sns = boto3.client('sns')

def lambda_handler(event, context):
    mentions = ['@AppleSupport', '@SamsungSupport']
    for mention in mentions:
        # Fetch new tweets from Twitter
        lf.fetch_new_tweets(mention)
        # Fetch new sentiments from Microsoft Azure Cognitive Services
        lf.fetch_new_sentiments(mention)
        # Check if share of negative sentiments is large enough to envoke a notification
        share_neg = lf.check_yesterday_sentiments()
        # Check if share of negative sentiments on customer reviews are above threshold
        if share_neg > 0.05:    
            # trigger notification on SNS
            response = sns.publish(
                TopicArn='arn:aws:sns:us-east-2:899917070711:CustomerReviewSentiment',    
                Message='Hey Tim! Get up, customer are not satisfied!',    
                )
        
    return {
        'statusCode': 200,
        'body': json.dumps('Tweets & sentiments loaded')
    }