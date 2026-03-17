import unittest
import requests

class TestV6DataExport(unittest.TestCase):

    def test_analytics_export_csv(self):
        response = requests.get('/v6/analytics/export/csv')
        self.assertEqual(response.status_code, 200)
        self.assertIn('text/csv', response.headers['Content-Type'])
        # Further assertions to check the content of the CSV

    def test_analytics_export_json(self):
        response = requests.get('/v6/analytics/export/json')
        self.assertEqual(response.status_code, 200)
        self.assertIn('application/json', response.headers['Content-Type'])
        # Further assertions to check the content of the JSON

    def test_analyses_export_csv(self):
        response = requests.get('/v6/analyses/export/csv')
        self.assertEqual(response.status_code, 200)
        self.assertIn('text/csv', response.headers['Content-Type'])
        # Further assertions to check the content of the CSV

    def test_analyses_export_json(self):
        response = requests.get('/v6/analyses/export/json')
        self.assertEqual(response.status_code, 200)
        self.assertIn('application/json', response.headers['Content-Type'])
        # Further assertions to check the content of the JSON

    def test_cache_stats_export_json(self):
        response = requests.get('/v6/cache/stats/export/json')
        self.assertEqual(response.status_code, 200)
        self.assertIn('application/json', response.headers['Content-Type'])
        # Further assertions to check the content of the JSON

    def test_api_requests_export_csv(self):
        response = requests.get('/v6/api/requests/export/csv')
        self.assertEqual(response.status_code, 200)
        self.assertIn('text/csv', response.headers['Content-Type'])
        # Further assertions to check the content of the CSV

if __name__ == '__main__':
    unittest.main()