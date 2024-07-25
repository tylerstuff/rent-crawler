import httpx
import os
from supabase import create_client, Client
import geohash
from discordwebhook import Discord
import logging

logging.basicConfig(level=logging.INFO)


url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)


HEADERS = {
    'Accept': 'application/json,*/*;q=0.01',
    'Dnt': '1',
    'Referer': 'https://www.onemap.gov.sg/',
    'Sec-Ch-Ua': '"Chromium";v="125", "Not.A/Brand";v="24"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"macOS"',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
}


def get_omitn_cookie() -> str:
    """omitn token is necessary to make requests to onemap api"""
    with httpx.Client(headers=HEADERS) as client:
        url = "https://www.onemap.gov.sg/#/PropertyQuery"
        response = client.get(url)
        if response.status_code == 200:
            # Extract cookies
            for cookie in client.cookies.jar:
                if cookie.name == 'OMITN':
                    return cookie.value
            raise Exception("OMITN cookie not found")
        else:
            raise Exception(f"Get request failed with status code: {response.status_code}")


def get_condo_ids():
    """Get all exisitng condo ids"""
    response = supabase.table("property_condo").select("id").limit(10000).execute()
    return [row["id"] for row in response.data]


def update_condo_ids(condos):
    """Add new condo ids to the database"""
    response = supabase.table("property_condo").upsert(condos, ignore_duplicates=True).execute()
    return response


def create_unique_id(lat, lon):
    """Create a geohash from the latitude and longitude"""
    lat = float(lat)
    lon = float(lon)
    unique_id = geohash.encode(lat, lon)
    return unique_id


def alert_new_condo(title: str, description: str, street_name: str, lat: str, long: str, district: str, condo_id: str):
    logging.info(f"Alerting new condo to discord: {title}")
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
    discord = Discord(url=webhook_url)
    discord.post(
        embeds=[
            {
                "title": title,
                "description": description,
                "fields": [
                    {"name": "Latitude", "value": str(lat), "inline": True},
                    {"name": "Longitude", "value": str(long), "inline": True},
                    {"name": "Street Name", "value": street_name},
                    {"name": "District", "value": str(district), "inline": True},
                    {"name": "ID", "value": condo_id, "inline": True},
                ]
            }
        ]
    )
    return
