import clickhouse_driver
import pandas as pd
import statsmodels.api as sm
from scipy.stats import spearmanr

class QDFeatureAnalysis():
    def __init__(self,feature_data,host, port, user, password):
        self.feature_data = feature_data.copy()
        self.database=
        #self.database=clickhouse_driver.Client(host=host, port=port, user=user, password=password)
    def make_return(self, data, model='close', time=4):
        if model == 'close':
            data['future_close'] = data.groupby(level=1)['close'].shift(-time)
            data['return'] = (data['future_close']-data['close'])/data['close']
            return data.dropna(subset='return')
        else:
            print('后续将继续添加新的收益率计算模式')

    def IC(self,df1,df2):
        retrun spearmanr(df)[0]
    def icir(self,data,holding_time=4):
        all_dates = data.index.get_level_values('date').unique()
        sorted_dates = sorted(all_dates)
        selected_dates = []
        if len(sorted_dates) > 0:
            current_date = sorted_dates[0]
            selected_dates.append(current_date)
            for date in sorted_dates[1:]:
                if int((date-current_date)/pd.Timedelta(1, 'h'))==holding_time:
                    selected_dates.append(date)
                    current_date=date
    