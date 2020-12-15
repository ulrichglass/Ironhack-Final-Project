from flask import Flask, request, render_template, url_for, redirect
from flask_mysql_connector import MySQL
import requests
import app_functions as af

application = app = Flask(__name__)

# Setting SQL Server connection
app.config['MYSQL_HOST'] = af.load_key('sql_host')
app.config['MYSQL_USER'] = af.load_key('sql_user')
app.config['MYSQL_PASSWORD'] = af.load_key('sql_password')
app.config['MYSQL_DATABASE'] = af.load_key('sql_database')
mysql = MySQL(app)
tweet_mention = ''

@app.route('/', methods=['POST', 'GET'])
def index():
    # if method is POST
    if request.method == 'POST':
        # select existing twitter accounts
     
        try:
            return redirect('/')
            # return render_template('archive.html', tweets=tweets_sql)
        except:
            return 'There was an issue.'
    # if method is GET
    else:
        return render_template('index.html')

@app.route('/testing', methods=['POST', 'GET'])
def testing():
    if request.method == 'POST': 
        text = str(request.form['test-text'])
        if len(text)>0:
            sentiment = af.azure_sentiment(text)   
        else:
            sentiment = ['']
        return render_template('testing.html', result_sentiment = sentiment)
    else:
        return render_template('testing.html')

@app.route('/archive', methods=['GET'])
def archive():
    cnx = mysql.connection
    cur = cnx.cursor()
    sql = 'SELECT * FROM tweets WHERE tweet_status = 2 ORDER BY tweet_ID DESC;'
    cur.execute(sql)
    tweets_sql = cur.fetchall()
    cur.close()
    cnx.close()
    return render_template('archive.html', tweets=tweets_sql)

@app.route('/new', methods=['POST', 'GET'])

def new():
    global tweet_mention
    if request.method == 'POST':
        tweet_mention = request.form['mention']
        new = request.form['new']
        if new == 'Fetch New Tweets':
            af.fetch_new_tweets(tweet_mention)
            print(new)
        elif new == 'Fetch Sentiments':
            af.fetch_new_sentiments(tweet_mention)
            print(new)
        return redirect('/new')   
    else:
        if not tweet_mention:
            tweet_mention = '@AppleSupport'
        cnx = mysql.connection
        cur = cnx.cursor()
        sql = "SELECT * FROM tweets WHERE tweet_status <> 2 AND tweet_mention = '"+tweet_mention+"' ORDER BY tweet_ID DESC;"
        cur.execute(sql)
        tweets_sql = cur.fetchall()
        sql = "SELECT DISTINCT tweet_mention FROM tweets WHERE tweet_mention<>'"+tweet_mention+"' ORDER BY tweet_mention;"
        cur.execute(sql)
        tweet_mention_other = cur.fetchall()
        cur.close()
        cnx.close()
        return render_template('new.html', tweets=tweets_sql, sel=tweet_mention, other=tweet_mention_other)

@app.route('/about', methods=['GET'])
def about():
    return render_template('about.html')

if __name__ == '__main__':
    app.run(debug=True)