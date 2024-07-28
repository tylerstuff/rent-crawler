import time
import logging
import random
import os
from discordwebhook import Discord
from crawler.condos import process_district
from crawler.utils import get_condo_ids


logging.basicConfig(level=logging.INFO)


def system_alert_discord(message: str):
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
    discord = Discord(url=webhook_url)
    discord.post(
        embeds=[
            {
                "title": "Condo Crawer Service Alert",
                "description": message,
            }
        ]
    )


def main():
    logging.info("Starting the main function")
    system_alert_discord("The condo crawler function has started")
    try:
        while True:
            existing_condos = get_condo_ids()
            for district in range(1, 29):
                logging.info(f"Processing district {district}")
                process_district(district, existing_condos)
                time.sleep(random.randint(10, 30))
            logging.info("Sleeping for 1 month")
            time.sleep(2592000)
    except Exception as e:
        system_alert_discord(f"An error occurred: {e}")
        logging.error(f"An error occurred: {e}")
    finally:
        system_alert_discord("The condo crawler function has stopped")


if __name__ == "__main__":
    main()
