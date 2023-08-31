import os
import pandas as pd
from  typing import List
from utils.api import make_api_call
from utils.api import create_df_of_events
from utils.db import load_df_to_postgress

class DataModeling:
    def __init__(self, api_key, df):
        self.api_key = api_key
        self.df = df
    
    def _generate_key_name(self, type: str) -> List[str]:
        type = type.lower().split()
        try:
            type_home = type[0]+"_"+type[1]+"_home"
            type_away = type[0]+"_"+type[1]+"_away"
        except IndexError as e:
            type_home = type[0]+"_home"
            type_away = type[0]+"_away"
        return [type_home, type_away]

    def create_fact_matches(self) -> pd.DataFrame:
        column_set_one = ["match_id",
                            "country_id",
                            "league_id",
                            "match_date",
                            "match_status",
                            "match_time",
                            "match_hometeam_id",
                            "match_hometeam_score",
                            "match_awayteam_id",
                            "match_awayteam_score",
                            "match_hometeam_halftime_score",
                            "match_awayteam_halftime_score",
                            "match_hometeam_extra_score",
                            "match_awayteam_extra_score",
                            "match_hometeam_penalty_score",
                            "match_awayteam_penalty_score",
                            "match_hometeam_system",
                            "match_awayteam_system",
                            "match_live",
                            "match_round",
                            "match_stadium",
                            "match_referee",
                            "league_year",
                            "fk_stage_key",
                            "stage_name"]
        
        column_set_two = ["match_id", "goalscorer"]
        df_1 = self.df[column_set_one]
        df_2 = self.df[column_set_two]
        exploded_df = df_2.explode("goalscorer", ignore_index=True)
        exploded_df = pd.concat([exploded_df["match_id"], pd.json_normalize(exploded_df["goalscorer"])], axis=1)
        exploded_df = exploded_df[["match_id", "time", "home_scorer_id", "away_scorer_id", "score", "info", "score_info_time"]]
        result_df = df_1.merge(exploded_df, on="match_id", how="left")
        result_df.rename(columns={"score": "current_score", "time": "score_time"}, inplace=True)
        result_df['match_hometeam_score'] = pd.to_numeric(result_df.match_hometeam_score, errors='coerce').fillna(0, downcast='infer')
        result_df['match_awayteam_score'] = pd.to_numeric(result_df.match_awayteam_score, errors='coerce').fillna(0, downcast='infer')
        result_df["total_scores"] = result_df["match_hometeam_score"]+result_df["match_awayteam_score"]
        result_df.loc[result_df["match_hometeam_score"] > result_df["match_awayteam_score"], "outcome"] = "Home Win"
        result_df.loc[result_df["match_hometeam_score"] < result_df["match_awayteam_score"], "outcome"] = "Away Win"
        result_df.loc[result_df["match_hometeam_score"] == result_df["match_awayteam_score"], "outcome"] = "Draw"
        return result_df

    def create_fact_cards(self) -> pd.DataFrame:
        column_set_one = ["match_id", "match_referee","match_hometeam_id", "match_awayteam_id"]
        column_set_two = ["match_id", "cards"]
        df_1 = self.df[column_set_one]
        df_2 = self.df[column_set_two]
        exploded_df = df_2.explode("cards", ignore_index=True)
        exploded_df = pd.concat([exploded_df["match_id"], pd.json_normalize(exploded_df["cards"])], axis=1)
        exploded_df = exploded_df[["match_id","time", "card", "info", "home_player_id", "away_player_id", "score_info_time"]]
        result_df = df_1.merge(exploded_df, on="match_id", how="left")
        return result_df

    def create_fact_lineups(self) -> List[pd.DataFrame]:
        df = self.df.reset_index(drop=True)
        
        # Iterate through each row in the DataFrame
        for index, row in df.iterrows():
            lineup_data = row["lineup"]
            lineup_list = []
            player_list = []
            
            # Iterate through home and away teams
            for team in ["home", "away"]:
                team_data = lineup_data[team]
                
                # Iterate through starting lineups
                for lineup in team_data["starting_lineups"]:
                    match_id = row["match_id"]
                    team_id = row[f"match_{team}team_id"]
                    player_name = lineup["lineup_player"]
                    player_number = lineup["lineup_number"]
                    player_position = lineup["lineup_position"]
                    isSubstitute = 0
                    isCoach = 0
                    isMissingPlayer = 0
                    isAway = 1 if team =="away" else 0
                    lineup_list.append([match_id, team_id, player_number, player_position, isSubstitute, isCoach, isMissingPlayer, isAway])
                    player_list.append([player_number, player_name, player_position, team_id ])

                # Iterate through substitutes
                for substitute in team_data["substitutes"]:
                    match_id = row["match_id"]
                    team_id = row[f"match_{team}team_id"]
                    player_name = substitute["lineup_player"]
                    player_number = substitute["lineup_number"]
                    player_position = substitute["lineup_position"]
                    isSubstitute = 1
                    isCoach = 0
                    isMissingPlayer = 0
                    isAway = 1 if team =="away" else 0
                    lineup_list.append([match_id, team_id, player_number, player_position, isSubstitute, isCoach, isMissingPlayer, isAway])
                    player_list.append([player_number, player_name,  player_position, team_id ])
                
                # Iterate through coaches
                for coach in team_data["coach"]:
                    match_id = row["match_id"]
                    team_id = row[f"match_{team}team_id"]
                    player_name = coach["lineup_player"]
                    player_number = coach["lineup_number"]
                    player_position = coach["lineup_position"]
                    isSubstitute = 0
                    isCoach = 1
                    isMissingPlayer = 0
                    isAway = 1 if team =="away" else 0
                    lineup_list.append([match_id, team_id, player_number, player_position, isSubstitute, isCoach, isMissingPlayer, isAway])
                    player_list.append([player_number, player_name, player_position, team_id ])
                
                for player in team_data["missing_players"]:
                    match_id = row["match_id"]
                    team_id = row[f"match_{team}team_id"]
                    player_name = player["lineup_player"]
                    player_number = player["lineup_number"]
                    player_position = player["lineup_position"]
                    isSubstitute = 0
                    isCoach = 0
                    isMissingPlayer = 1
                    isAway = 1 if team =="away" else 0
                    lineup_list.append([match_id, team_id, player_number, player_position, isSubstitute, isCoach, isMissingPlayer, isAway])
                    player_list.append([player_number, player_name, player_position, team_id ])

        result_df_1 = pd.DataFrame(lineup_list, columns=["match_id", "team_id", "player_number", "player_position", "isSubstitute", "isCoach", "isMissingPlayer", "isAway"])
        result_df_2 = pd.DataFrame(player_list, columns=["player_number", "player_name", "player_position", "team_id"])
        result_df_2 = result_df_2.drop_duplicates(subset=["player_number"])
        return [result_df_1, result_df_2]

    def create_dim_leagues(self) -> pd.DataFrame:
        column_set = ["league_id", "league_name", "league_logo"]
        result_df = self.df[column_set]
        result_df = result_df.drop_duplicates(subset=['league_id'])
        return result_df

    def create_dim_countries(self) -> pd.DataFrame:
        column_set = ["country_id", "country_name", "country_logo"]
        result_df = self.df[column_set]
        result_df = result_df.drop_duplicates(subset=['country_id'])
        return result_df

    def create_dim_teams(self) -> pd.DataFrame:
        column_set_one = ["match_hometeam_id", "match_hometeam_name", "team_home_badge"]
        column_set_two = ["match_awayteam_id", "match_awayteam_name", "team_away_badge"]
        df_1 = self.df[column_set_one]
        df_1.rename(columns={"match_hometeam_id": "hometeam_id", "match_hometeam_name": "hometeam_name", "team_home_badge": "home_badge"}, inplace=True)
        df_2 = self.df[column_set_two]
        df_2.rename(columns={"match_awayteam_id": "hometeam_id", "match_awayteam_name": "hometeam_name", "team_away_badge": "home_badge"}, inplace=True)
        result_df = df_1.append(df_2)
        result_df = result_df.drop_duplicates(subset=['hometeam_id'])
        return result_df

    def create_fact_substitutions(self) -> pd.DataFrame:
        df = self.df.reset_index(drop=True)
        substitute_list = []
        for index, row in df.iterrows():
            substitution_data = row["substitutions"]

            # Iterate through home and away teams
            for team in ["home", "away"]:
                substitution = substitution_data[team]

                for substitute in substitution:
                    match_id = row["match_id"]
                    team_id = row[f"match_{team}team_id"]
                    time = substitute["time"]
                    from_player_id = substitute["substitution_player_id"].split("|")[0]
                    to_player_id = substitute["substitution_player_id"].split("|")[1]
                    isAway = 1 if team =="away" else 0
                    substitute_list.append([match_id, team_id, time, from_player_id.strip(), to_player_id.strip(), isAway])

        result_df = pd.DataFrame(substitute_list, columns=["match_id", "team_id", "time", "from_player_id", "to_player_id", "isAway"])
        return result_df

    def create_fact_statistics(self) -> pd.DataFrame:
        df = self.df.reset_index(drop=True)
        all_stats_list = []
        for index, row in df.iterrows():
            statistics_data = row["statistics"]
            statistics_1half = row["statistics_1half"]

            #Iterate through "statistics"
            statistics_dic = {"match_id" : row["match_id"], "isFistHalf":0}
            for stats in statistics_data:
                type_home, type_away = self._generate_key_name(stats["type"])
                statistics_dic[type_home] = stats["home"]
                statistics_dic[type_away] = stats["away"]
            all_stats_list.append(statistics_dic )

            #Iterate through "statistics_1half"
            statistics_1half_dic = {"match_id":row["match_id"], "isFistHalf":1}
            for stats in statistics_1half:
                type_home, type_away = self._generate_key_name(stats["type"])
                statistics_1half_dic[type_home] = stats["home"]
                statistics_1half_dic[type_away] = stats["away"]
            all_stats_list.append(statistics_1half_dic)

        result_df = pd.DataFrame(all_stats_list)
        return result_df

api_key=os.getenv("API_KEY")
response_1 = make_api_call(api_key, "2022-08-11", "2022-11-11")
response_2 = make_api_call(api_key, "2022-11-12", "2023-05-29")
df_1 = create_df_of_events(response_1)
df_2 = create_df_of_events(response_2)
df = df_1.append(df_2)
model = DataModeling(api_key, df)
fact_matches = model.create_fact_matches()
load_df_to_postgress([fact_matches], ["fact_matches"])
fact_cards = model.create_fact_cards()
load_df_to_postgress([fact_cards], ["fact_cards"])
lineups_list = model.create_fact_lineups()
load_df_to_postgress(lineups_list, ["fact_lineups", "dim_players"])
fact_substitutions = model.create_fact_substitutions()
load_df_to_postgress([fact_substitutions], ["fact_substitutions"])
fact_statistics = model.create_fact_statistics()
load_df_to_postgress([fact_statistics], ["fact_statistics"])
dim_leagues = model.create_dim_leagues()
load_df_to_postgress([dim_leagues], ["dim_leagues"])
dim_countries = model.create_dim_countries()
load_df_to_postgress([dim_countries], ["dim_countries"])
dim_teams = model.create_dim_teams()
load_df_to_postgress([dim_teams], ["dim_teams"])