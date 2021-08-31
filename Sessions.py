import redis
import pandas as pd
import re

class Sessions:

    # r = redis.Redis(host='redis', port=6379, db=0)
    

    def __init__(self, sessions):
        self.session_columns = ['UserAgent', 'IP', 'FirstHitTime', 'TotalHits', 'Image', 'HTML', 'Api', 'ASCII', 'Other', 'Bandwith', 
                   'SessionLength', 'AvgResponseTime', 'Errors', 'Get', 'Post', 'Head', 'Put', 'Options', 'IsRobotstxt']



        # if sessions == None:
        #     self.sessions = pd.DataFrame(columns=self.session_columns)  
        # else:
        self.sessions = sessions
        self.sessions.FirstHitTime = pd.to_datetime(self.sessions.FirstHitTime)


    def update_existing_session(self, row, i):

        self.sessions.loc[i , ('TotalHits')] += 1

        if row.DataType == 'Robots': #update robots.txt
            self.sessions.loc[i , ('IsRobotstxt')] = 1
            self.sessions.loc[i , ('Other')] += 1
        else:
            self.sessions.loc[i , (row.DataType)] += 1

        self.sessions.loc[i , ('Bandwith')] = self.sessions.loc[i , ('Bandwith')] + row.ResponseLength
        self.sessions.loc[i , ('SessionLength')] = (row.Time - self.sessions.loc[i , ('FirstHitTime')]).seconds
        if row.StatusCode >= 400:
            self.sessions.loc[i , ('Errors')] += 1
        self.sessions.loc[i , ('AvgResponseTime')] = ((self.sessions.loc[i , ('AvgResponseTime')]*(self.sessions.loc[i , ('TotalHits')] - 1)) + row.ResponseTime)/self.sessions.loc[i , ('TotalHits')]
        self.sessions.loc[i , (row.MethodType)] += 1
        return 

    def update_sessions(self, row):
        # requests is a Pandas dataframe
        # self.sessions is a Pandas dataframe
        # row is a log line
        TIMEOUT_SECONDS = 1800

        # if self.process_request(row) == -1:
        #     return

        # process log to series
        row = self.process_request(row)
        
        timout_idx = int(self.sessions["FirstHitTime"].searchsorted(row.Time - pd.Timedelta(seconds = TIMEOUT_SECONDS), side = 'right'))
        found_sessions = self.sessions.iloc[timout_idx:]
        found_sessions = found_sessions.loc[(self.sessions['IP'] == row.IP) & (self.sessions['UserAgent'] == row.UserAgent)]
        if len(found_sessions) > 0:
            self.update_existing_session(row, found_sessions.iloc[-1].name)
            return self.sessions.loc[found_sessions.iloc[-1].name]
        else:
        #Creating the new session
            self.sessions.loc[len(self.sessions)] = self.create_new_session(row)
            return self.sessions.loc[len(self.sessions)]

    def create_new_session(self, row):
        # new session series
        new_session = pd.Series(index = self.session_columns, data = {'UserAgent': row.UserAgent, 'IP':row.IP, 'FirstHitTime': row.Time, 'TotalHits': 1, 
                                                                'Image':0, 'HTML':0, 'Api':0, 'ASCII':0, 'Other':0, 
                                                                'Bandwith': row.ResponseLength, 'SessionLength':0, 'AvgResponseTime': row.ResponseTime,
                                                                'Errors': 0 , 'IsRobotstxt':0, 
                                                                'Get':0, 'Post':0, 'Head':0, 'Put':0,'Options':0, '%UnassignedRefferer':0.0})

        if row.DataType == 'Robots': #update robots.txt
            new_session['IsRobotstxt'] = 1
            new_session['Other'] = 1
        else:
            new_session[row.DataType] = 1

        if row.StatusCode >= 400:
            new_session.Errors = 1

        new_session[row.MethodType] = 1

        return new_session

    def process_request(self, log):
        lst = log.split()
        user_agent_pattern = r'.*?\[\[(.*)]].*'

        dict_ = {'IP':lst[0] , 'Time':lst[1] , 'MethodType' : lst[2][1:] , 'Method' : lst[3][:-1],
        'StatusCode' : lst[4], 'ResponseLength' : lst[5] ,'UserAgent': re.search(user_agent_pattern, log).group(1) , 'ResponseTime' : lst[-1]}
        
        request = pd.Series(index=dict_.keys(), data=dict_)

        request.Time = pd.to_datetime(request.Time, format='[%Y-%m-%dT%H:%M:%S.0%z]')
        
        # if request.IP == '-':
        #     return -1

        request.ResponseTime = int(request.ResponseTime)
        request.ResponseLength = int(request.ResponseLength)
        request.StatusCode = int(request.StatusCode)
        request['DataType'] = self.get_data_type(request.Method)


        return request

    def get_data_type(self, method_):
        if method_.endswith('robots.txt'): # if robots.txt is visited
            return 'Robots' 
        elif method_.endswith(('.jpg', '.png', '.tiff', '.ico', '.gif')) or 'cdn' in method_:
            return 'Image'
        elif method_.endswith(('.html', '.php', '.htm', '.js', '.cgi', '.css')) or any(s in method_ for s in ('amp', 'pages', 'pros', 'order', 'app', 'blog')): # amp can be in a different category
            return 'HTML'
        elif method_.endswith(('.txt', '.cpp', '.java', '.xml', '.json', '.c')) or '.woff' in method_:
            return 'ASCII'
        elif 'api' in method_: # Can be ignored (redundant) 
            return 'Api'
        else:
            return 'Other'
  

    