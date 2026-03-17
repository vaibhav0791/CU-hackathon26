import csv
import json

class ExportService:
    def export_to_csv(data, filename):
        """Exports data to a CSV file."""
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for row in data:
                writer.writerow(row)

    def export_to_json(data, filename):
        """Exports data to a JSON file."""
        with open(filename, 'w') as jsonfile:
            json.dump(data, jsonfile)

    def export_analytics(self, analytics_data):
        self.export_to_csv(analytics_data, 'analytics_data.csv')
        self.export_to_json(analytics_data, 'analytics_data.json')

    def export_analyses(self, analyses_data):
        self.export_to_csv(analyses_data, 'analyses_data.csv')
        self.export_to_json(analyses_data, 'analyses_data.json')

    def export_cache_stats(self, cache_stats):
        self.export_to_csv(cache_stats, 'cache_stats.csv')
        self.export_to_json(cache_stats, 'cache_stats.json')

    def export_api_requests(self, api_requests):
        self.export_to_csv(api_requests, 'api_requests.csv')
        self.export_to_json(api_requests, 'api_requests.json')
