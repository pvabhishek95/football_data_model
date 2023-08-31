import pandas as pd
import requests
import ast
from typing import List


def create_df_of_events(response: List) -> pd.DataFrame:
    df = pd.DataFrame()
    for match in response:
        match_df = pd.DataFrame.from_dict(match, orient="index")
        match_df = match_df.transpose()
        df = df.append(match_df)
    return df

def make_api_call(api_key: str, start_date: str, end_date: str) -> List:
    request_url = "https://apiv3.apifootball.com/?action=get_events&from={start_date}&to={end_date}&league_id=152&APIkey={API_KEY}".format(start_date=start_date,end_date=end_date,API_KEY=api_key)
    response = requests.get(request_url)
    response_list = ast.literal_eval(response.text)
    return response_list