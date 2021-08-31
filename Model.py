import numpy as np
from pyod.models.auto_encoder import AutoEncoder
import seaborn as sns

class Model:

    def __init__(self) -> None:
        pass


    def is_session_anomaly(self, session):
        return self.model.predict(self.get_session_features(session).values.reshape(1, -1))

    def train_model(self, sessions):
        features = self.feature_extraction(sessions)
        self.model = AutoEncoder(hidden_neurons = [ 10 , 5 ,5 , 10 ] ,preprocessing=True, contamination=0.02 , epochs=20 , batch_size = 1024)
        self.model.fit(features)


    def feature_extraction(self, sessions):
        sessions = sessions.apply(lambda row: self.normalize_types(row), axis = 1)

        sessions['TotalHits'] = np.sqrt(sessions['TotalHits'])
        sessions['Bandwith'] = np.sqrt(sessions['Bandwith'])
        sessions['AvgResponseTime'] = np.sqrt(sessions['AvgResponseTime'])
        sessions['SessionLength'] = np.sqrt(sessions['SessionLength'])

        features = sessions.copy().drop(['UserAgent' , 'IP' , 'FirstHitTime','Other', 'Unnamed: 0'] , axis = 1)
        return features

    def get_session_features(self, session):
        session = session.copy()
        self.normalize_types(session)

        session['TotalHits'] = np.sqrt(session['TotalHits'])
        session['Bandwith'] = np.sqrt(session['Bandwith'])
        session['AvgResponseTime'] = np.sqrt(session['AvgResponseTime'])
        session['SessionLength'] = np.sqrt(session['SessionLength'])
        features = session.drop(['UserAgent' , 'IP' , 'FirstHitTime','Other', 'Unnamed: 0'])

        return features


    def normalize_types(self, row):
        n = row.TotalHits/100.0

        row.Image = row.Image/n
        row.HTML = row.HTML/n
        row.Api = row.Api/n
        row.ASCII = row.ASCII/n
        row.Other = row.Other/n

        row.Get = row.Get/n
        row.Post = row.Post/n
        row.Head = row.Head/n
        row.Put = row.Put/n
        row.Options = row.Options/n

        return row