import time
import pandas as pd
import numpy as np
import clickhouse_driver
import ccxt
from crypto_data_fetch import CryptoDataFetcher

class MarketDataWarehouse():
    def __init__(self, crypto_exchange,host, port, user, password):
        self.client = clickhouse_driver.Client(host=host, port=port, user=user, password=password,
                                               database='market_data')    
        self._init_database()
        # 初始化各市场数据获取器
        self.crypto_fetcher = CryptoDataFetcher(exchange=crypto_exchange)
        


    #初始化数据库创建数据表
    def _init_database(self):
        #创建加密货币市场数据数据表
        self.client.execute('CREATE TABLE IF NOT EXISTS \
                    market_data.crpto_market_data (\
                    date Date,\
                    code String,\
                    open Float64,\
                    high Float64,\
                    low Float64,\
                    close Float64,\
                    volume Float64\
                    )\
                    ENGINE = ReplacingMergeTree() \
                    ORDER BY (date, code)')
        #创建meta元数据数据表（记录各资产最新更新时间）
        self.client.execute('CREATE TABLE IF NOT EXISTS \
                    market_data.market_metadata (\
                    asset_type String,\
                    code String,\
                    last_updated Date\
                    )\
                    ENGINE = ReplacingMergeTree() \
                    ORDER BY (asset_type, code)')
    #更新元数据记录        
    def _update_metadata(self, asset_type: str, code: str, last_updated):
        query = f'INSERT INTO market_metadata (asset_type, code, last_updated) VALUES '
        params = {
            'asset_type': asset_type,
            'code': code,
            'last_updated': last_updated
        }
        self.client.execute(query, params)
    #将数据插入数据库中
    def _insert_data(self, asset_type: str, df: pd.DataFrame):
        if df.empty:
            print('插入数据表为空')
        if asset_type=='crypto':
            data = df.to_dict('records')
            self.client.execute(
            "INSERT INTO market_data.crpto_market_data (date, code, open, high, low, close, volume) VALUES",data)
        elif asset_type=='stock':
            pass
        #获取指定资产的最新更新时间
    def _get_last_updated(self, asset_type: str, code: str):
        query = f"""
        SELECT last_updated 
        FROM market_data.market_metadata
        WHERE asset_type = '{asset_type}' AND code = '{code}'
        ORDER BY last_updated DESC
        LIMIT 1
        """
        result = self.client.execute(query)
        return result[0][0] if result else None
    #获取指定资产的数据并将其插入进数据库中
    def fetch_and_store(self, asset_type: str, code: str, timeframe: str = '1h'):
        #获取指定资产的最新更新时间
        last_updated = self._get_last_updated(asset_type,code)
        if last_updated is None:
            since = pd.Timestamp.now().floor('h') - pd.Timedelta(days=730)
        else:
            since = last_updated
        
        #获取最后数据时间
        latest_timestamp = pd.Timestamp.now().floor('h') - pd.Timedelta(hours=1)
        #获取并处理数据
        new_data = self.crypto_fetcher.fetch_ohlcv(code, timeframe, limit=100,since=since)
        new_time = new_data['timestamp'].max() + pd.Timedelta(hours=1)
        data = []
        data.append(new_data)
        while new_time <= latest_timestamp:
            new_data = self.crypto_fetcher.fetch_ohlcv(code, timeframe, limit=100,since=new_time)
            new_time = new_data['timestamp'].max() + pd.Timedelta(hours=1)
            data.append(new_data)
        result = pd.concat(data,axis=0)
        if result.empty:
            print(f'暂无如下数据 {asset_type}/{code}')
            return
        #更新元数据
        self._update_metadata(asset_type, code, latest_timestamp)
        # 存储数据
        result = result.loc[(new_data['timestamp']>=since)&(new_data['timestamp']<=latest_timestamp)]
        self._insert_data(asset_type, result)
    
    def 
 