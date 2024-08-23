import sys
import os
import unittest
import requests
from unittest.mock import patch, MagicMock
from crawler.rent import get_rents, rent_transformer, update_rent
import logging

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestRent(unittest.TestCase):

    def setUp(self):
        # Set up a logger for tests
        self.logger = logging.getLogger('test_logger')
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logging.StreamHandler())

    def test_get_rents_success(self):
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = [{"test": "data"}]
            mock_get.return_value = mock_response

            result = get_rents(1.0, 1.0, "2023-01", "test_cookie")
            self.assertEqual(result, [{"test": "data"}])
            self.logger.info("Test 'test_get_rents_success' passed successfully")

    def test_get_rents_failure(self):
        with patch('requests.get') as mock_get:
            mock_get.side_effect = requests.RequestException("Test error")

            result = get_rents(1.0, 1.0, "2023-01", "test_cookie")
            self.assertEqual(result, [])
            self.logger.info("Test 'test_get_rents_failure' passed successfully (Intended error scenario)")

    def test_rent_transformer_success(self):
        data = {
            "leaseyear": "23",
            "leasemth": "01",
            "fromareasqm": "100",
            "toareasqm": "120",
            "fromareasqft": "1076",
            "toareasqft": "1292",
            "rent": "3000"
        }
        condo = {
            "id": "test_id",
            "last_update_string": "2022-12"
        }
        result = rent_transformer(data, condo)
        self.assertEqual(result["condo_id"], "test_id")
        self.assertEqual(result["lease_month"], "2023-01")
        self.assertEqual(result["sqm"], 110)
        self.assertEqual(result["sqft"], 1184)
        self.assertEqual(result["rent"], 3000)
        self.assertAlmostEqual(result["rent_psf"], 2.5338, places=4)
        self.logger.info("Test 'test_rent_transformer_success' passed successfully")

    def test_rent_transformer_failure(self):
        data = {
            "leaseyear": "invalid",
            "leasemth": "01",
            "fromareasqm": "invalid",
            "toareasqm": "120",
            "fromareasqft": "1076",
            "toareasqft": "1292",
            "rent": "3000"
        }
        condo = {
            "id": "test_id",
            "last_update_string": "2022-12"
        }
        result = rent_transformer(data, condo)
        self.assertIsNone(result)
        self.logger.info("Test 'test_rent_transformer_failure' passed successfully (Intended error scenario)")

    @patch('crawler.rent.select_condo')
    @patch('crawler.rent.get_omitn_cookie')
    @patch('crawler.rent.get_rents')
    @patch('crawler.rent.upload_rent')
    @patch('crawler.rent.post_crawl_condo_update')
    def test_update_rent(self, mock_post_crawl, mock_upload, mock_get_rents, mock_cookie, mock_select):
        mock_select.return_value = [{"id": "test_id", "latitude": 1.0, "longitude": 1.0, "last_update_string": "2020-12"}]
        mock_cookie.return_value = "test_cookie"
        mock_get_rents.return_value = [{
            "leaseyear": "21",
            "leasemth": "01",
            "fromareasqm": "100",
            "toareasqm": "120",
            "fromareasqft": "1076",
            "toareasqft": "1292",
            "rent": "3000"
        }]
        mock_upload.return_value = None
        mock_post_crawl.return_value = None

        update_rent()

        mock_select.assert_called_once()
        mock_cookie.assert_called_once()
        mock_get_rents.assert_called_once_with(1.0, 1.0, "2021-01", "test_cookie")
        mock_upload.assert_called_once()
        mock_post_crawl.assert_called_once_with("test_id")
        self.logger.info("Test 'test_update_rent' passed successfully")


if __name__ == '__main__':
    unittest.main()
