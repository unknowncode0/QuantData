import pandas as pd
import numpy as np
import ccxt
import pytz
#创建加密货币获取类，获取相应数据
class CryptoDataFetcher:
    #属性：交易所，默认交易所为okx
    def __init__(self,exchange='okx'):
        self.exchange=getattr(ccxt,exchange)()
        self.exchange.load_markets()

    #获取行情数据
    def fetch_ohlcv(self, code, timeframe,limit,since):
        ohlcv = self.exchange.fetch_ohlcv(code, timeframe, since, limit)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms',utc=True).dt.tz_convert('Asia/Shanghai').dt.tz_localize(None)
        #df=df.sort_values(by='timestamp')
        return df
    
    def get_market_data_usdt_paris(self,type:str,usdt_pairs:bool=True) -> list[str]:
        """
        获取交易所中所有交易对（usdt_pairs为True时返回USDT交易对）
        :param type: 金融产品类型 spot表示现货，future表示到期期货，swap表示永续互换，option表示期权
        :usdt_pairs: 是否只要usdt交易对,如BTC/USDT，默认为True
        :return: 加密或交易对列表
        """
        market_data=[]
        self.exchange.load_markets()
        for symbol,market in self.exchange.markets.items():
            if market['type']==type and not usdt_pairs:
                market_data.append(symbol)
            elif market['type']==type and usdt_pairs:
                if 'USDT' in market['quote'] and market['quote'] == 'USDT':
                   market_data.append(symbol)
        return market_data
    
