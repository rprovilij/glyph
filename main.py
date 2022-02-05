from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime, timedelta
from pycoingecko import CoinGeckoAPI
import statistics
import schedule
import sqlite3
import praw
import time
import os


def path_prep(path):
    try:
        if os.path.isdir(path):
            # print(">>> Directory exists: ", path)
            pass
        else:
            os.makedirs(path)
            print(">>> Directory created: ", path)
    except OSError as error:
        print(">>> Failed to create directory: ", error)


def time_convert(timestamp):
    return datetime.fromtimestamp(timestamp)


def monitor_api_requests():
    if reddit.auth.limits['remaining'] < 5:
        print(">>> Approaching Reddit API request limit:", int(reddit.auth.limits['remaining']), "calls left.")
        print(">>> Reset timestamp: ", time_convert(reddit.auth.limits['reset_timestamp']))
        wait = int(timedelta.total_seconds(time_convert(reddit.auth.limits['reset_timestamp']) - datetime.now())) + 1
        print(">>> Waiting time:", wait, "second(s)")
        time.sleep(wait)
        print(">>> Continuing process...")


def get_price(token):
    response = cg.get_price(ids=token, vs_currencies='usd',
                            include_market_cap=True,
                            include_24hr_vol=True,
                            include_24hr_change=True)
    return response


def post_sentiment(subreddit):
    sentiment_stats = []
    post_title_sm   = []
    post_body_sm    = []
    comment_sm      = []
    reply_sm        = []

    for submission in subreddit:
        if not submission.stickied:
            # Number of hours look-back when subreddit is sorted by "new" is 4, else 24 (e.g., for "hot" or "top)
            t_delta = 4 if subreddit.url.strip('/').split('/')[-1] == "new" else 24
            # Only retrieve posts within a n-hour/t_delta time range.
            if time_convert(submission.created) > (datetime.now() - timedelta(hours=t_delta)):
                # print(30 * "-", "\n", submission.title, " | ", time_convert(submission.created), "| Comments: ", submission.num_comments, "\n", 30 * "-")
                vs_post_title = analyzer.polarity_scores(submission.title)['compound']
                vs_post_body = analyzer.polarity_scores(submission.selftext)['compound']
                post_title_sm.append(vs_post_title)
                post_body_sm.append(vs_post_body)
                monitor_api_requests()

                # Retrieve comments of posts
                submission.comments.replace_more(limit=0)  # Removes all "MoreComments" from the forest
                if submission.num_comments > 1:
                    for iteration, comment in enumerate(submission.comments.list()):
                        vs_comment = analyzer.polarity_scores(comment.body)['compound']
                        # print("Comment: ", comment.body, vs_comment)
                        comment_sm.append(vs_comment)
                        monitor_api_requests()
                        # Takes sample of max 100 comments
                        if iteration >= 99:
                            break

                        # Retrieve replies of those comments
                        if len(comment.replies) > 1:
                            for reply in comment.replies:
                                vs_reply = analyzer.polarity_scores(reply.body)['compound']
                                # print("Reply: ", reply.body, vs_reply)
                                reply_sm.append(vs_reply)
                                monitor_api_requests()

    # Number of posts, number of comments, number of replies
    post_stats = [len(post_title_sm), len(comment_sm), len(reply_sm)]
    sentiment_buffer = [post_title_sm, post_body_sm, comment_sm, reply_sm]

    for lst in sentiment_buffer:  # Mean needs at least 1 value, variance needs at least two.
        if len(lst) == 0:
            sent_vals = [None, None]
            # print(sent_vals)
            sentiment_stats.append(sent_vals)
        elif len(lst) == 1:
            sent_vals = [statistics.mean(lst), None]
            # print(sent_vals)
            sentiment_stats.append(sent_vals)
        elif len(lst) > 1:
            sent_vals = [statistics.mean(lst), statistics.variance(lst)]
            # print(sent_vals)
            sentiment_stats.append(sent_vals)

    output = [post_stats] + sentiment_stats                     # Output has a nested list structure
    return [item for sublist in output for item in sublist]     # Return a flattened list instead


def store(token, subreddit):
    # Brings together CoinGecko market values and Vader sentiment scores
    vals = list((get_price(token)[str(token)]).values()) + post_sentiment(subreddit)

    # E.g., subreddit.url is r/Bitcoin/new; sub = Bitcoin. For r/Cryptocurrency/search/, end "/" needs removal
    sub = subreddit.url.strip('/').split('/')[-2]
    # E.g., subreddit.url is r/Bitcoin/new; sort_by = new
    sort_by = subreddit.url.strip('/').split('/')[-1]

    # Defining path for db's. Will create if none exists using 'prep_dirs'
    path = "C:/Users/rprovilij/python_projects/GLYPH/data/{token}/{sort}/".format(token=token, sort=sort_by)
    path_prep(path)

    print(">>> Storing price and sentiment values...", sub)
    db = "{path}{token}.db".format(path=path, token=token)
    try:
        con = sqlite3.connect(db)
        c = con.cursor()
        try:
            c.execute("CREATE TABLE db (t, "
                      "price, "
                      "market_cap, "
                      "daily_vol, "
                      "daily_change, "
                      "n_posts, "
                      "mean_post_title_sentiment, "
                      "var_post_title_sentiment, "
                      "mean_post_body_sentiment, "
                      "var_post_body_sentiment, "
                      "n_comments, "
                      "mean_comment_sentiment, "
                      "var_comment_sentiment, "
                      "n_replies, "
                      "mean_replies_sentiment, "
                      "var_replies_sentiment);")
        except:
            pass
        query = "INSERT INTO db (t, " \
                "price, " \
                "market_cap, " \
                "daily_vol, " \
                "daily_change, " \
                "n_posts, " \
                "mean_post_title_sentiment, " \
                "var_post_title_sentiment, " \
                "mean_post_body_sentiment, " \
                "var_post_body_sentiment, " \
                "n_comments, " \
                "mean_comment_sentiment, " \
                "var_comment_sentiment, " \
                "n_replies, " \
                "mean_replies_sentiment, " \
                "var_replies_sentiment) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
        c.execute(query, (time.strftime('%d/%m/%Y - %H:%M:%S'),
                          # CoinGecko values (PRICE, MC, 24h-vol, 24h-change
                          vals[0], vals[1], vals[2], vals[3],
                          # Sentiment values (n_posts, p-title sm, p-title var, p-body sm, p-body var)
                          vals[4], vals[7], vals[8], vals[9], vals[10],
                          # Sentiment values (n_comments, com_sm, com_var)
                          vals[5], vals[11], vals[12],
                          # Sentiment values (n_replies, rep_sm, rep_var)
                          vals[6], vals[13], vals[14]))
        con.commit()
        c.close()
    except sqlite3.Error as error:
        print("Failed to insert data: ", error)


def new():
    print(">>> Running 'NEW' process...")
    for tok, sub in zip(crypto, subreddits):
        store(tok, reddit.subreddit(sub).new(limit=100))
    print(">>> Process complete", datetime.now(), "| API calls left: ", reddit.auth.limits['remaining'])


def hot():
    print(">>> Running 'HOT' process...")
    for tok, sub in zip(crypto, subreddits):
        store(tok, reddit.subreddit(sub).hot(limit=100))
    print(">>> Process complete", datetime.now(), "| API calls left: ", reddit.auth.limits['remaining'])


def search():
    print(">>> Running 'SEARCH' process...")
    for tok, sub in zip(crypto, subreddits):
        store(tok, reddit.subreddit("CryptoCurrency").search(tok, sort="top", syntax="cloudsearch", time_filter="day"))
    print(">>> Process complete", datetime.now(), "| API calls left: ", reddit.auth.limits['remaining'])


def main():
    schedule.every(4).hours.do(new)
    schedule.every().day.at("23:59").do(hot)
    schedule.every().day.at("23:59").do(search)


if __name__ == '__main__':
    # Fixed variables
    reddit = praw.Reddit(client_id='ID',
                         client_secret='SECRET',
                         username='USERNAME',
                         password='PASSWORD',
                         user_agent='Glyph-sentiment (u/USERNAME)',
                         )
                         
    analyzer = SentimentIntensityAnalyzer()
    cg = CoinGeckoAPI()

    # Top 20 coins (no-stable or exchange coins) with 10k community members + 5 coins of (personal) interest.
    crypto        = ["bitcoin", "ethereum", "cardano", "solana", "ripple",
                     "polkadot", "terra-luna", "dogecoin", "avalanche-2", "shiba-inu",
                     "matic-network", "cosmos", "litecoin", "chainlink", "algorand",
                     "tron", "bitcoin-cash", "fantom", "stellar", "hedera-hashgraph",
                     "decentraland", "nano",  "monero", "siacoin", "the-sandbox"]

    subreddits    = ["Bitcoin", "ethereum", "cardano", "solana", "XRP",
                     "dot", "terraluna", "dogecoin", "avax", "SHIBArmy",
                     "0xPolygon", "cosmosnetwork", "litecoin", "Chainlink", "algorand",
                     "tronix", "Bitcoincash", "FantomFoundation", "Stellar", "hbar",
                     "decentraland", "nanocurrency", "Monero", "siacoin", "TheSandboxGaming"]

    main()

    while True:
        schedule.run_pending()
        time.sleep(1)
