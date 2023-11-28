import requests
import pandas as pd
import chessdotcom as chess
import chess.pgn
import os
from pandas import json_normalize


def get_url(url):
    url = url
    headers = {
	"User-Agent":'Data project student. Contact me at peremartinmoraleja@gmail.com'
	}
    response = requests.get(url, headers=headers)
    return response

def get_profiles(df,output):
    l_profiles=[]
    for index,row in df.iterrows():
        url = f"https://api.chess.com/pub/player/{row['players']}"
        response = get_url(url)
        l_profiles.append(response.json())
    df_profiles = pd.DataFrame(l_profiles)

    time_cols = ['last_online', 'joined']

    # Convertir cada columna de tiempo Unix a DateTime
    for col in time_cols:
        df_profiles[col] = pd.to_datetime(df_profiles[col], unit='s')
    df_profiles.to_csv(f"../Project IV - Chess/data/{output}.csv")
    return df_profiles


def get_stats(df,csv_name):
    l_stats=[]
    for index,row in df.iterrows():
        url = f"https://api.chess.com/pub/player/{row['players']}/stats"
        response = get_url(url)
        player_stats = response.json()
        player_stats['player'] = row['players'] 
        l_stats.append(player_stats)
        #if index == 100:
        #   break
    output = pd.DataFrame(l_stats)
    output.to_csv(f"../Project IV - Chess/data/{csv_name}.csv")
    return output


def get_player_games(df,csv_name):

    game_type_dfs = []
    for column in df.columns:
        if column != 'player': 
            df[column].dropna()
            game_type_df = pd.json_normalize(df[column])
            game_type_df['game_type'] = column 
            game_type_df['player'] = df['player']
            game_type_dfs.append(game_type_df)

    # Concatenate all
    final_df = pd.concat(game_type_dfs, ignore_index=True, sort=False)
    final_df.to_csv(f"../Project IV - Chess/data/{csv_name}.csv")
    return final_df

def get_archives_player(player_name):
    url = f"https://api.chess.com/pub/player/{player_name}/games/archives"
    response = get_url(url)
    if response.status_code == 200:
        player_archives = response.json()
        df_player_archives = pd.DataFrame({'Archive_URL': player_archives['archives']})
        df_player_archives['Player'] = player_name
        return df_player_archives
    else:
        return pd.DataFrame(columns=['Archive_URL', 'Player'])
# ------------------------------------------------------------------------------------------------------------

# Extract_yeare_month: take year and month from URL archive
def extract_year_month(url):
    year = url.split('/')[-2]  
    month = url.split('/')[-1] 
    return year, month

# ------------------------------------------------------------------------------------------------------------

def get_all_archives(df,csv_name):
    all_players_archives = []

    #for index,row in df_catalan_players.iterrows():
    for index,row in df.iterrows():
        df_archives = get_archives_player(row["players"])
        all_players_archives.append(df_archives)
        #if index == 100:
        #    break
    combined_df = pd.concat(all_players_archives, ignore_index=True)
    combined_df['Year'], combined_df['Month'] = zip(*combined_df['Archive_URL'].apply(extract_year_month))
    combined_df.to_csv(f"../Project IV - Chess/data/{csv_name}.csv")
    return combined_df


def download_pgn(player, year, month, target_folder):
    pgn_url = f"https://api.chess.com/pub/player/{player}/games/{year}/{month}/pgn"
    response = get_url(pgn_url)
    
    if response.status_code == 200:
        # Create the target folder if it doesn't exist
        os.makedirs(target_folder, exist_ok=True)
        
        # Define the filename using player's name and date
        filename = f"{player}_{year}_{month}.pgn"
        file_path = os.path.join(target_folder, filename)
        
        # Save the PGN data to a file
        with open(file_path, 'w') as file:
            file.write(response.text)
        
        return file_path  # Return the path to the saved file
    else:
        print(f"Failed to download PGN for {player} ({year}-{month}): {response.status_code}")
        return None


# ------------------------------------------------------------------------------------------------------------------------------

def read_pgn_file(file_path):
    with open(file_path) as pgn:
        while True:
            game = chess.pgn.read_game(pgn)
            if game is None:
                break
            yield game
# ------------------------------------------------------------------------------------------------------------------------------

def format_moves(game):
    moves = []
    board = game.board()
    for move in game.mainline_moves():
        if board.turn == chess.WHITE:
            moves.append(f"{board.fullmove_number}. {board.san(move)}")
        else:
            moves[-1] += f" {board.san(move)}"
        board.push(move)
    return ' '.join(moves)

# ------------------------------------------------------------------------------------------------------------------------------
def extract_game_data(game,player):
    
# Extract basic game information
    game_info = {
        'Event': game.headers.get('Event',None),
        'Site': game.headers.get('Site',None),
        'Date': game.headers.get('Date',None),
        'Round': game.headers.get('Round',None),
        'White': game.headers.get('White',None),
        'Black': game.headers.get('Black',None),
        'Result': game.headers.get('Result',None),
        'Termination': game.headers.get('Termination',None),
        'UTCDate': game.headers.get('UTCDate',None),
        'UTCTime': game.headers.get('UTCTime',None),
        'WhiteElo': game.headers.get('WhiteElo',None),
        'BlackElo': game.headers.get('BlackElo',None),
        'StartTime': game.headers.get('StartTime',None),
        'EndDate': game.headers.get('EndDate',None),
        'EndTime': game.headers.get('EndTime',None),
        'Link': game.headers.get('Link',None),
        'Player':player
    }
    try:
        game_info['Moves'] = format_moves(game)
    except:
        game_info['Moves'] = f"Error in: {game}"

    return game_info
    
# ------------------------------------------------------------------------------------------------------------------------------


import pymysql
import sqlalchemy as alch # python -m pip install --upgrade 'sqlalchemy<2.0'
from getpass import getpass

def connection_sql(l_datasets,l_names):
    password = getpass()
    dbName = "chess"
    connectionData=f"mysql+pymysql://root:{password}@localhost/{dbName}"
    engine = alch.create_engine(connectionData)
    #names = ["df_all_detailed_stats"]
    #datasets = [df_all_detailed_stats]
    for df,name in zip(l_datasets,l_names):
        df.to_sql(name, if_exists="append", con=engine, index=False)