
import pandas as pd
from pandas.core.indexes.base import Index
from Sessions import Sessions
from Model import Model
import time




if __name__ == '__main__':
    df = pd.read_csv('sessions.csv')
    # print(df.dtypes)
    sessions = Sessions(df)

    model = Model()
    model.train_model(sessions.sessions)

    req = '126.54.10.225 [2021-5-12T7:3:17.0+0430] [Get /robots.txt] 200 4825 [[Python-urllib/3.7]] 4'
    # for i in range(100):
    t1 = time.time()
    print(model.is_session_anomaly(sessions.update_sessions(req)))
    t2 = time.time()
    print(f'model took {(t2 - t1)* 1000}')
    