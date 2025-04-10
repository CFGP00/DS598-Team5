import requests
import datetime
import json
import time

API_KEY = ''  # Replace with your GNews API key
BASE_URL = 'https://gnews.io/api/v4/search'
QUERY = 'Apple'
MAX_RESULTS = 100

# Define the date range (last 3 years)
end_date = datetime.date.today()
start_date = end_date - datetime.timedelta(days=3*365)

headlines = []

try:
    current_date = start_date
    while current_date < end_date:
        next_date = current_date + datetime.timedelta(days=7)
        params = {
            'q': QUERY,
            'from': current_date.isoformat() + 'T00:00:00Z',
            'to': next_date.isoformat() + 'T23:59:59Z',
            'max': MAX_RESULTS,
            'token': API_KEY,
            'lang': 'en'  # Ensure only English articles are retrieved
        }
        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            if 'articles' in data:
                for article in data['articles']:
                    headlines.append({
                        'title': article.get('title'),
                        'publishedAt': article.get('publishedAt'),
                        'url': article.get('url')
                    })
                print(f'Fetched {len(data["articles"])} articles from {current_date} to {next_date}')
            else:
                print(f'No articles found from {current_date} to {next_date}')
        elif response.status_code == 429:
            print('Rate limit exceeded, waiting for 60 seconds')
            time.sleep(60)
            continue
        else:
            print(f'Error fetching data: {response.status_code} - {response.text}')
        current_date = next_date

    with open('apple_news_headlines.json', 'w', encoding='utf-8') as file:
        json.dump(headlines, file, indent=4, ensure_ascii=False)
        print(f'Successfully saved {len(headlines)} articles to apple_news_headlines.json')

except Exception as e:
    print(f'An error occurred: {e}')
