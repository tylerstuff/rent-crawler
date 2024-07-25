import requests
from crawler.utils import get_omitn_cookie, HEADERS, create_unique_id, update_condo_ids, alert_new_condo
import logging
from tenacity import retry, wait_exponential, stop_after_attempt, before_sleep_log

logging.basicConfig(level=logging.INFO)


class NoCondoDataError(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(logging, logging.WARNING)
)
def get_condos(district: int):
    """Get condos in a specific district"""
    # Base URL
    url = "https://www.onemap.gov.sg/omapp/getRentalPropertyInfo"

    # Query parameters
    params = {
        'distance': '10000',
        'oneRoomHDB': 'false',
        'twoRoomHDB': 'false',
        'threeRoomHDB': 'false',
        'fourRoomHDB': 'false',
        'fiveroomHDB': 'false',
        'multiGenHDB': 'false',
        'executiveHDB': 'false',
        'executiveCondo': 'true',
        'apartment': 'true',
        'landed': 'false',
        'condo': 'true',
        'transactionPeriod': '3',
        'minprice': '0',
        'maxprice': '20000000',
        'district': district
    }
    cookie_auth = get_omitn_cookie()
    cookies = {
        'OMITN': cookie_auth,
    }

    # Make the GET request
    response = requests.get(url, params=params, cookies=cookies, headers=HEADERS)

    # Check if the request was successful
    if response.status_code == 200:
        # Print the response content
        return response.json()  # Assuming the response is in JSON format
    else:
        logging.error(f"Request failed with status code: {response.status_code}")
        return None


def pick_condo_response(rows):
    """Parse the response from the API"""
    # Extract the relevant information
    for row in rows:
        if "Landed" in row.keys():
            return row["Landed"]["features"]


def parse_condo_response(rows, district, existing_condos):
    """Parse the response from the API"""
    # Extract the relevant information
    payload = pick_condo_response(rows)
    if not payload:
        # Ignore D24, no condo there
        if district == 24:
            return []
        else:
            raise NoCondoDataError("No condo data found")
    new_condos = []
    added_condos = []
    for condo in payload:
        name = condo["properties"]["description"]
        street_name = condo["properties"]["street_name"]
        latitude = condo["properties"]["latitude"]
        longitude = condo["properties"]["longitude"]
        id = create_unique_id(latitude, longitude)
        geom = f'POINT({longitude} {latitude})'
        if id not in existing_condos and id not in added_condos:
            new_condos.append({
                "id": id,
                "district": district,
                "name": name,
                "street_name": street_name,
                "latitude": latitude,
                "longitude": longitude,
                "geom": geom
            })
            added_condos.append(id)
    return new_condos


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(logging, logging.WARNING)
)
def process_district(district, existing_condos):
    """Process a district"""
    properties = get_condos(district)
    result = parse_condo_response(properties, district, existing_condos)
    if result:
        updated = update_condo_ids(result)
        for row in updated.data:
            logging.info(f"New condo found: {row['name']}")
            logging.info(row)
            alert_new_condo(
                title=f"New Condo: {row['name']}",
                description="New condo has been detected, check if the condo is legitimate and if not, remove.",
                street_name=row["street_name"],
                lat=row["latitude"],
                long=row["longitude"],
                district=row["district"],
                condo_id=row["id"]
            )
