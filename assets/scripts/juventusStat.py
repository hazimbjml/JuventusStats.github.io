import os
import requests
import pandas as pd
from datetime import datetime
import json
from dotenv import load_dotenv


"""API Connection"""

url = "https://v3.football.api-sports.io/players"
parameters = {
    "league": "135",
    "season": "2023",
    "team": "496",
    "page": "1"
}
headers = {
    'x-rapidapi-key': '317673c93d4bba99406d683460d6276d',
    'x-rapidapi-host': 'v3.football.api-sports.io'
}

response = requests.get(url, headers=headers, params=parameters)
print(response.text)


"""Extract the Data from API (Extract)"""

def extract_data_api(url, headers, params):
    """
    Fetch the data using the API 
    """
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_error_message:
        print(f"❌ [HTTP ERROR]: {http_error_message}")
    except requests.exceptions.ConnectionError as connection_error_message:
        print(f"❌ [CONNECTION ERROR]: {connection_error_message}")
    except requests.exceptions.Timeout as timeout_error_message:
        print(f"❌ [TIMEOUT ERROR]: {timeout_error_message}")
    except requests.exceptions.RequestException as other_error_message:
        print(f"❌ [UNKNOWN ERROR]: {other_error_message}")

print(extract_data_api(url=url, headers=headers, params=parameters))


"""
Handling Pagination & API Response Code
"""

all_responses = []
parameters["page"] = "1"  # Ensure the page is reset to 1

while True:
    response = requests.get(url, headers=headers, params=parameters)
    if response.status_code == 200:
        data = response.json()
        all_responses.extend(data['response'])

        # Check if there are more pages to fetch
        current_page = int(data['paging']['current'])
        total_pages = int(data['paging']['total'])
        print(f"Retrieved page {current_page} of {total_pages}")

        if current_page < total_pages:
            # Increment the page parameter to fetch the next page
            parameters['page'] = str(current_page + 1)
        else:
            # Break the loop if all pages have been fetched
            break
    else:
        print(f"[ERROR] Failed to retrieve data from API, STATUS CODE: {response.status_code}")
        print(f"Response content: {response.content}")
        break

# Print the combined result
print(json.dumps(all_responses, indent=5))



"""Transfrom JSON Data Into Readable format (Transform)"""

def transformed_players(data):
    """
    Parse the JSON data retrieved from API 
    """
    players = []
    for players_data in data:
        statistics = players_data['statistics'][0]

        # set up constants for processing data
        player = players_data['player']
        player_id = player['id']
        player_name = player['name']
        player_age = int(player['age'])
        nationality = player['nationality']
        position = statistics['games']['position']
        rating = float(statistics['games']['rating']) if statistics['games']['rating'] else 0.0
        match_played = int(statistics['games']['appearences']) if statistics['games']['appearences'] else 0
        minutes_played = int(statistics['games']['minutes']) if statistics['games']['minutes'] else 0
        total_goals = int(statistics['goals']['total']) if statistics['goals']['total'] else 0
        total_shots = int(statistics['shots']['total']) if statistics['shots']['total'] else 0
        ontarget_shots = int(statistics['shots']['on']) if statistics['shots']['on'] else 0
        penalty_goals = int(statistics['penalty']['scored']) if statistics['penalty']['scored'] else 0
        penalty_missed = int(statistics['penalty']['missed']) if statistics['penalty']['missed'] else 0
        total_assists = int(statistics['goals']['assists']) if statistics['goals']['assists'] else 0
        total_passes = int(statistics['passes']['total']) if statistics['passes']['total'] else 0
        key_passes = int(statistics['passes']['key']) if statistics['passes']['key'] else 0
        dribble_attempts = int(statistics['dribbles']['attempts']) if statistics['dribbles']['attempts'] else 0
        dribble_success = int(statistics['dribbles']['success']) if statistics['dribbles']['success'] else 0
        tackles = int(statistics['tackles']['total']) if statistics['tackles']['total'] else 0
        interceptions = int(statistics['tackles']['interceptions']) if statistics['tackles']['interceptions'] else 0
        aerial_duel_total = int(statistics['duels']['total']) if statistics['duels']['total'] else 0
        aerial_duel_won = int(statistics['duels']['won']) if statistics['duels']['won'] else 0
        fouls = int(statistics['fouls']['committed']) if statistics['fouls']['committed'] else 0
        yellow_card = int(statistics['cards']['yellow']) if statistics['cards']['yellow'] else 0
        doubleYellowToRed = int(statistics['cards']['yellowred']) if statistics['cards']['yellowred'] else 0
        red_card = int(statistics['cards']['red']) if statistics['cards']['red'] else 0

        players.append({
            'PlayerId': player_id,
            'player': player_name,
            'age': player_age,
            'nationality': nationality,
            'position': position,
            'rating': rating,
            'match': match_played,
            'minutes': minutes_played,
            'goals': total_goals,
            'total_shots': total_shots,
            'ontarget_shots': ontarget_shots,
            'penalty_goals': penalty_goals,
            'penalty_missed': penalty_missed,
            'assists': total_assists,
            'passes': total_passes,
            'key_passes': key_passes,
            'dribble_attempts': dribble_attempts,
            'dribble_success': dribble_success,
            'tackles': tackles,
            'interception': interceptions,
            'aerial_duel_total': aerial_duel_total,
            'aerial_duel_won': aerial_duel_won,
            'fouls': fouls,
            'yellow_card': yellow_card,
            'doubleYellowToRed': doubleYellowToRed,
            'red_card': red_card
        })

    return players
players_list = transformed_players(all_responses)
print(players_list)




"""
Building SQL Server (Load)
"""

def create_df(players):
    df = pd.DataFrame(players)
    