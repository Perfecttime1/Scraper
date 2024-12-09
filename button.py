import requests
from datetime import datetime, timedelta

# Replace these with your actual credentials
NOTION_TOKEN = "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"
DATABASE_ID = "156e4980aef4801e9ca2c85af98b9f74"
APIFY_API_KEY = "apify_api_emSEbVRLsbmMxijLsLgYaNSeHuephg0Eg5ps"
APIFY_TASK_ID = "HZHUjIHQjnSdzPfCV"

def get_reels_posted_yesterday():
    """Fetch Reel URLs posted yesterday from the Notion database."""
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # Calculate yesterday's date
    yesterday = (datetime.now() - timedelta(days=1)).date().isoformat()
    
    # Query Notion with a filter for Date Posted = yesterday
    payload = {
        "filter": {
            "property": "Date Posted",
            "date": {
                "equals": yesterday
            }
        }
    }
    response = requests.post(url, headers=headers, json=payload)
    data = response.json()

    reels = []
    for result in data.get("results", []):
        properties = result.get("properties", {})
        reel_url = properties.get("Reel URL", {}).get("url")
        page_id = result["id"]

        if reel_url:
            reels.append({
                "page_id": page_id,
                "url": reel_url
            })
    return reels

def scrape_metrics_with_apify(reel_urls):
    """Scrape metrics for the given Reel URLs using Apify."""
    api_url = f"https://api.apify.com/v2/actor-tasks/{APIFY_TASK_ID}/run-sync-get-dataset-items"
    headers = {
        "Authorization": f"Bearer {APIFY_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "directUrls": [reel["url"] for reel in reel_urls],
        "resultsType": "posts",
        "proxy": {"useApifyProxy": True}
    }
    response = requests.post(api_url, json=payload, headers=headers)
    return response.json()  # Returns list of metrics

def update_notion_database(reel_urls, metrics):
    """Update the Notion database with the scraped metrics."""
    for reel, metric in zip(reel_urls, metrics):
        page_id = reel["page_id"]
        url = f"https://api.notion.com/v1/pages/{page_id}"
        headers = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
        payload = {
            "properties": {
                "Views": {"number": metric.get("videoViewCount", 0)},
                "Likes": {"number": metric.get("likesCount", 0)},
                "Comments": {"number": metric.get("commentsCount", 0)}
            }
        }
        response = requests.patch(url, headers=headers, json=payload)
        if response.status_code == 200:
            print(f"Updated Notion page: {page_id}")
        else:
            print(f"Failed to update Notion page: {page_id}. Error: {response.text}")

# Main Zapier Task Execution
reels = get_reels_posted_yesterday()
if reels:
    metrics = scrape_metrics_with_apify(reels)
    if metrics:
        update_notion_database(reels, metrics)
