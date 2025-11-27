from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType
import json, time,requests

from config.settings import get_config

# --------------------------------------
# Set values for API calls
# --------------------------------------
OPENAQ_API_BASE_URL = get_config("OPENAQ_API_V3_BASE_URL", default="https://api.openaq.org/v3")
OPENAQ_API_KEY = get_config("OPENAQ_API_KEY", secret_scope="data-air-quality-monitor")

class OpenAqDatasource(DataSource):
    @classmethod
    def name(self) -> str:
        return "openaqdatasource"
    
    def reader(self, schema: StructType):
        return OpenAqDataSourceReader(schema, self.options)
    
class OpenAqDataSourceReader(DataSourceReader):
    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options
        self.endpoint = options.get("endpoint")
        self.headers = options.get("headers", {})
        self.params = options.get("params", {})
        self.delay_between_requests_in_seconds = options.get("delayBetweenRequestsInSeconds", 0)
        
        if not self.endpoint:
            raise ValueError("The 'endpoint' option must be provided.")
        
        def read(self):
            url = f"{OPENAQ_API_BASE_URL}/{self.endpoint}"
            headers = self.headers
            headers['x-api-key'] = OPENAQ_API_KEY
            params = self.params
            json_params = self.options.get("params")
            delay = self.delay_between_requests_in_seconds

            try:
                params = json.loads(json_params)
            except Exception as e:
                raise ValueError("Failed to parse 'params' option as JSON.")
            
            if "limit" in params:
                page = 1
                results = []
                next_page = True

                while next_page:
                    params["page"] = page
                    
                    time.sleep(delay)

                    r = requests.get(url, headers=headers, params=params)
                    r.raise_for_status()
                    # if r.status_code != 200:
                    #     raise Exception(f"Failed to fetch data: {r.status_code}")

                    payload = r.json()
                    data = payload.get("results", [])
                    results.extend(data)

                    if len(data) < params.get("limit", 100):
                        next_page = False
                    else:
                        page += 1
            else:
                time.sleep(delay)

                r = requests.get(url, headers=headers, params=params)
                r.raise_for_status()
                # if r.status_code != 200:
                #     raise Exception(f"Failed to fetch data: {r.status_code}")

                payload = r.json()
                results = payload.get("results", [])

            if isinstance(results, list):
                yield from (tuple(item.values()) for item in results)
            else:
                yield tuple(results.values())