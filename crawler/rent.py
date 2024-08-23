from typing import Dict, Union, List
import requests
from crawler.utils import post_crawl_condo_update, select_condo, HEADERS, get_omitn_cookie, upload_rent
import logging

# Constants
YEAR_PREFIX = "20"
REQUEST_TIMEOUT = 10  # seconds


def get_rents(lat: float, lon: float, yyyy_mm: str, cookie_auth: str) -> List[Dict]:
    url = "https://www.onemap.gov.sg/omapp/getPPRentalRecordsByLatLon"

    # Input validation
    if not all(isinstance(param, float) for param in (lat, lon)):
        raise ValueError("Latitude and longitude must be float values")
    if not yyyy_mm.strip() or not cookie_auth.strip():
        raise ValueError("yyyy_mm and cookie_auth must not be empty")

    # Query parameters
    params = {
        'lat': lat,
        'lon': lon,
        'startTransacDate': yyyy_mm
    }
    cookies = {
        'OMITN': cookie_auth,
    }

    try:
        response = requests.get(url, params=params, cookies=cookies, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Error fetching rent data: {e}")
        return []


def rent_transformer(data: Dict[str, str], condo: Dict[str, str]) -> Dict[str, Union[str, float, int]]:
    try:
        if data["leaseyear"] + data["leasemth"] > condo["last_update_string"]:
            # Helper function to safely convert to int
            def safe_int(value: Union[str, int]) -> int:
                return int(value.rstrip('+')) if isinstance(value, str) else int(value)

            sqm = (safe_int(data["fromareasqm"]) + safe_int(data["toareasqm"])) / 2
            sqft = (safe_int(data["fromareasqft"]) + safe_int(data["toareasqft"])) / 2
            rent = safe_int(data["rent"])
            rent_psft = round(rent / sqft, 4) if sqft != 0 else 0

            return {
                "condo_id": condo["id"],
                "lease_month": f'{YEAR_PREFIX}{data["leaseyear"]}-{data["leasemth"]}',
                "sqm": sqm,
                "sqft": sqft,
                "rent_psf": rent_psft,
                "rent": rent
            }
        else:
            return None
    except (ValueError, TypeError, ZeroDivisionError) as e:
        logging.error(f"Error transforming rent data: {e}, Data: {data}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error transforming rent data: {e}, Data: {data}")
        return None


def update_rent():
    """Updates rent prices for picked condos"""
    to_crawl = select_condo()
    cookie_auth = get_omitn_cookie()
    for condo in to_crawl:
        condo_id = condo["id"]
        latitude = condo["latitude"]
        longitude = condo["longitude"]
        logging.info(f"Updating rent for condo {condo_id}")
        logging.info(f"Latitude: {latitude}, Longitude: {longitude}")
        results = get_rents(latitude, longitude, "2021-01", cookie_auth)
        rent_data = [rent_transformer(data, condo) for data in results]
        # Remove {} from list
        rent_data = [data for data in rent_data if data]
        upload_rent(rent_data)
        post_crawl_condo_update(condo_id)


if __name__ == "__main__":
    update_rent()
