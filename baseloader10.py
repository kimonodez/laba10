import json
import pandas as pd
from datetime import datetime
from pandas.core.api import DataFrame as DataFrame
from baseloader10 import BaseDataLoader
from enum import Enum
import asyncio

class Granularity(Enum):
    ONE_MINUTE=60,
    FIVE_MINUTES=300,
    FIFTEEN_MINUTES=900,
    ONE_HOUR=3600,
    SIX_HOURS=21600,
    ONE_DAY=86400

class CoinbaseLoader(BaseDataLoader):

    def __init__(self, endpoint="https://api.exchange.coinbase.com"):
        super().__init__(endpoint)

    async def get_pairs(self) -> pd.DataFrame:
        data = await self._get_req("/products")
        df = pd.DataFrame(json.loads(data))
        df.set_index('id', drop=True, inplace=True)
        return df

    async def get_stats(self, pair: str) -> pd.DataFrame:
        data = await self._get_req(f"/products/{pair}")
        return pd.DataFrame(json.loads(data), index=[0])

    async def get_historical_data(self, pair: str, begin: datetime, end: datetime, granularity: Granularity) -> DataFrame:
        params = {
            "start": begin,
            "end": end,
            "granularity": granularity.value
        }
        # Number of async requests
        num_requests = 10
        # Divide the time range into equal parts
        time_range = (end - begin) // num_requests

        async def fetch_data(start, end):
            params['start'] = start
            params['end'] = end
            data = await self._get_req(f"/products/{pair}/candles", params)
            return json.loads(data)

        tasks = [fetch_data(begin + i * time_range, begin + (i + 1) * time_range) for i in range(num_requests)]

        results = await asyncio.gather(*tasks)

        # Concatenate results into a single DataFrame
        df = pd.concat([pd.DataFrame(result, columns=("timestamp", "low", "high", "open", "close", "volume")) for result in results])
        df.set_index('timestamp', drop=True, inplace=True)
        return df

if __name__ == "__main__":
    async def main():
        loader = CoinbaseLoader()
        data = await loader.get_pairs()
        print(data)
        data = await loader.get_stats("btc-usdt")
        print(data)
        data = await loader.get_historical_data("btc-usdt", datetime(2023, 1, 1), datetime(2023, 6, 30), granularity=Granularity.ONE_DAY)
        print(data.head(5))

    asyncio.run(main())
