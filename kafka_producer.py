from kafka import KafkaProducer
import requests
from json import dumps
from time import sleep
from dotenv import load_dotenv
import os

load_dotenv()
NEWS_API_KEY = os.getenv("NEWS_API_KEY") 

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: dumps(v).encode('utf-8')
)

# ----------------------
# Config
# ----------------------
# List of categories to rotate through to ensure fresh data
CATEGORIES = ['technology', 'business', 'science', 'entertainment', 'general']
COUNTRY = 'us'
DELAY_BETWEEN_ARTICLES = 1  

# Keep track of which articles were sent (avoid duplicates across loops)
sent_article_urls = set()

# ----------------------
# Fetch and Stream
# ----------------------
def stream_articles():
    print("Starting Multi-Category News Stream...")
    while True:
        for category in CATEGORIES:
            print(f"\n--- Checking Category: {category} ---")
            url = f"https://newsapi.org/v2/top-headlines?category={category}&country={COUNTRY}&apiKey={NEWS_API_KEY}"
            
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    articles = response.json().get('articles', [])
                    
                    new_articles_count = 0
                    for article in articles:
                        url_link = article.get("url")
                        
                        # Skip if we already sent this or if it's a dead link
                        if not url_link or url_link in sent_article_urls:
                            continue

                        cur_data = {
                            "source": article.get("source", {}).get("name"),
                            "author": article.get("author") or "Unknown", # Default for missing authors
                            "title": article.get("title"),
                            "description": article.get("description"),
                            "url": url_link,
                            "publishedAt": article.get("publishedAt"),
                            "content": article.get("content")
                        }

                        producer.send('news_topic', value=cur_data)
                        sent_article_urls.add(url_link)
                        new_articles_count += 1
                        
                        print(f"[{category}] Sent: {cur_data['title'][:50]}...")
                        sleep(DELAY_BETWEEN_ARTICLES)
                    
                    if new_articles_count == 0:
                        print(f"No new articles in {category} right now.")
                else:
                    print(f"[ERROR] API returned {response.status_code} for {category}")
            
            except Exception as e:
                print(f"[ERROR] Connection error: {e}")

            # Small breather between categories
            producer.flush()
            sleep(2)

        print("\nFinished one full cycle of categories. Waiting 30s for news updates...")
        sleep(30)

if __name__ == "__main__":
    stream_articles()