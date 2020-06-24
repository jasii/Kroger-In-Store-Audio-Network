from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pathlib2 import Path
import ruamel.yaml as yaml
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import sys
import spotipy.util as util
import os
import json
import webbrowser
from json.decoder import JSONDecodeError

def cleanUpData(df):
        # deep copy dataframe
        _df = df.copy(deep=True)
        # remove unused / unwanted columns if they exist
        _df = _df.drop(columns=['longitude', 'latitude', 'favorite', 'id'], errors='ignore')
        # Add "Artist - SongTitle" to new artist_title column
        _df['artist_title'] = _df['artist'] + ' - ' + _df['title']
        return _df

def getUniqueArtists(df):
        # deep copy dataframe
        _df = df.copy(deep=True)
        unique_artists_df = _df.artist.unique()
        return unique_artists_df

def getNumOfUniqueArtists(df):
        return len(getUniqueArtists(df))

def getUniqueSongs(df):
        # deep copy dataframe
        _df = df.copy(deep=True)
        unique_songs_df = _df.artist_title.unique()
        return unique_songs_df

def getNumOfUniqueSongs(df):
        return len(getUniqueSongs(df))

# Needs work. Returns a list of Bools
def getRepeatedSongs(df):
        # deep copy dataframe
        #_df = df.copy(deep=True)
        #repeated_songs_df = _df.duplicated(['artist_title'])
        # deep copy dataframe
        _df = df.copy(deep=True)
        #repeated_songs = _df.duplicated(subset='artist_title', keep=False)
        #repeated_songs = getUniqueSongs(_df)
        repeated_songs = _df.loc[_df['play_count'] >= 2]
        repeated_songs = repeated_songs.duplicated(subset='artist_title', keep=False)
        #repeated_songs = getUniqueSongs(repeated_songs)
        return repeated_songs

def getNumOfRepeatedSongs(df):
        # deep copy dataframe
        _df = df.copy(deep=True)
        repeated_songs = _df.duplicated(subset='artist_title', keep=False).sum()
        return repeated_songs

def getHoursRecorded(df):
        start_end_time = df.iloc[[-1, 0]]
        hours_collected = (start_end_time.index[0] - start_end_time.index[1])/np.timedelta64(1,'h')
        return hours_collected

def printTopXSongs(df,topX):
        # deep copy dataframe
        _df = df.copy(deep=True)
        print("\nTOP " + str(topX) + " SONGS (by play count)")
        top_songs = _df.sort_values(by='play_count', ascending=False)
        top_songs = top_songs.reset_index(drop=True)
        top_songs = top_songs.drop(columns=['title','artist_play_count','artist'])
        top_songs = top_songs.drop_duplicates()
        top_songs = top_songs.reset_index(drop=True)
        print(top_songs.head(topX))
        #return top_songs

def printTopXArtists(df,topX):
        # deep copy dataframe
        _df = df.copy(deep=True)
        print("\nTOP " + str(topX) + " ARTISTS (by play count)")
        top_artists = _df.sort_values(by='artist_play_count', ascending=False)
        top_artists = top_artists.reset_index(drop=True)
        top_artists = top_artists.drop(columns=['title','play_count','artist_title'])
        top_artists = top_artists.drop_duplicates()
        top_artists = top_artists.reset_index(drop=True)
        print(top_artists.head(topX))

def generatePlayCountColumn(df):
        # deep copy dataframe
        _df = df.copy(deep=True)
        # generate 'play_count' DF
        play_count = df.artist_title.value_counts(sort=False).reset_index()
        play_count.columns = ['artist_title', 'play_count']
        # merge play count into copy of df
        _df = pd.merge(_df, play_count, on='artist_title', how='inner',right_index=True)
        # sort by datetimeindex
        _df = _df.sort_index()
        return _df

def generateArtistPlayCountColumn(df):
        # deep copy dataframe
        _df = df.copy(deep=True)
        # generate 'artist_play_count' DF
        artist_play_count = _df.artist.value_counts(sort=False).reset_index()
        artist_play_count.columns = ['artist', 'artist_play_count']
        # merge artist play count into main DF
        _df = pd.merge(_df, artist_play_count, on='artist', how='inner',right_index=True)
        # sort by datetimeindex
        _df = _df.sort_index()
        return _df

def print_df_stats(df):
        # deep copy dataframe
        _df = df.copy(deep=True)
        print("\n----------------------------------------")
        print("Hours of data recorded: \t" + str("%.2f" % getHoursRecorded(_df)))
        print("----------------------------------------")
        print("Unique artists: \t" + str(getNumOfUniqueArtists(_df)))
        print("Unique songs: \t\t" + str(getNumOfUniqueSongs(_df)))
        print("----------------------------------------")
        print("Songs recorded: \t" + str(len(_df)))
        print("Repeated songs: \t" + str(getNumOfRepeatedSongs(_df)) + "\t" + str("%.2f" % (getNumOfRepeatedSongs(_df) / len(_df) * 100)) + "%")
        print("Songs not repeated: \t" + str(len(_df) - getNumOfRepeatedSongs(_df)) + "\t" + str("%.2f" % ((len(_df) - getNumOfRepeatedSongs(_df)) / len(_df) * 100)) + "%")
        print("----------------------------------------\n")

def connectToSpotify():
        #username = ''

        # Erase cache and prompt for user permission
        try:
                token = util.prompt_for_user_token(username,scope='user-read-private',client_id='b1b31d6fe19b4852aa9ba2b7bb39d2d9',client_secret='c635dc01adb0484e890598c157aa4dff',redirect_uri='http://localhost/')
        except:
                #os.remove(f".cache-{username}")
                token = util.prompt_for_user_token(username,scope='user-read-private',client_id='b1b31d6fe19b4852aa9ba2b7bb39d2d9',client_secret='c635dc01adb0484e890598c157aa4dff',redirect_uri='http://localhost/')
        # Create our spotifyObject
        spotifyObject = spotipy.Spotify(auth=token)
        return spotifyObject

def addSpotifyData(uniqueSongs_df):
        spotifyObject = connectToSpotify()
        # deep copy dataframe
        _df = uniqueSongs_df.copy(deep=True)
        for index_label, row_series in _df.iterrows():
        # For each row update the 'Bonus' value to it's double
                _df.at[index_label , 'duration_ms'] = row_series['duration_ms'] * 2

        for i in range(len(_df)):
                artist = _df.iloc[i]['artist']
                title = _df.iloc[i]['title']
                #if '(' in title:
                #        title = _df.iloc[i]['title'].split('(')
                #        the_title = str(title[0])
                #else:
                #        the_title = _df.iloc[i]['title']
                try:
                        songSearch = spotifyObject.search(q='' + artist + ' - ' + title + '', limit=1)
                        song = songSearch['tracks']['items'][0]
                except NameError:
                        songSearch = spotifyObject.search(q='' + artist + ' ' + title + '', limit=1)
                        song = songSearch['tracks']['items'][0]
                except IndexError:
                        print(_df.iloc[i]['artist'] + ' ' + _df.iloc[i]['title'])
                        songDurationMs = song['duration_ms']
                except:
                        #songSearch = spotifyObject.search(q='' + artist + ' ' + title + '', limit=1)
                        #song = songSearch['tracks']['items'][0]
                        print("Song not found on Spotify")
                else:
                        #print(json.dumps(songSearch, sort_keys=True, indent=4))
                        #print(_df.iloc[i]['artist'] + ' ' + _df.iloc[i]['title'])
                        songDurationMs = song['duration_ms']
                        _df.iloc[i]['duration_ms'] = songDurationMs
                        songPopularity = song['popularity']
                        _df.iloc[i]['popularity'] = songPopularity
                        songExplicit = song['explicit']
                        _df.iloc[i]['explicit'] = songExplicit
                        songSpotifyId = song['id']
                        _df.iloc[i]['spotify_song_id'] = songSpotifyId
                        #songSpotifyUrl = 'https://open.spotify.com/track/' + songSpotifyId
                        songAlbumId = song['album']['id']
                        _df.iloc[i]['spotify_album_id'] = songAlbumId
                        #songAlbumUrl = 'https://open.spotify.com/album/' + songAlbumId
                        songAlbumName = song['album']['name']
                        _df.iloc[i]['album'] = songAlbumName
                        songReleaseDate = song['album']['release_date']
                        _df.iloc[i]['release_date'] = songReleaseDate
                        songAlbumImage = song['album']['images'][0]['url']
                        _df.iloc[i]['album_image'] = songAlbumImage
                        songArtistId = song['album']['artists'][0]['id']
                        _df.iloc[i]['spotify_artist_id'] = songArtistId
                        #songArtistUrl = 'https://open.spotify.com/artist/' + songArtistId
                        print(_df.iloc[i])
        return _df

def main():
        # Show max rows and columns on printing of dataframe
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        # New blank dataframe
        inStoreAudioNetwork_df = pd.DataFrame()
        # CSV file path
        my_data = Path("nowplaying_songs_export_csv_07192019_135710.csv")
        # Try to read the data and parse the dates
        try:
                csv_df = pd.read_csv(my_data, parse_dates=[6], index_col='date', keep_default_na=False)
        # File not found exception
        except Exception:
                print("File not found")
        # File found so clean up and generate play count columns
        else:
                # Clean up dataframe, remove empty and unused columns
                inStoreAudioNetwork_df = cleanUpData(csv_df)
                # Add song play count column
                
                inStoreAudioNetwork_df = generatePlayCountColumn(inStoreAudioNetwork_df)
                # Add artist play count column
                inStoreAudioNetwork_df = generateArtistPlayCountColumn(inStoreAudioNetwork_df)
                
                inStoreAudioNetwork_df = addSpotifyData(inStoreAudioNetwork_df)

                inStoreAudioNetwork_df.to_csv('spotify.csv', mode='w')
                # Print the dataframe stats
                #print_df_stats(inStoreAudioNetwork_df)
                
                #(inStoreAudioNetwork_df, 5)
                #printTopXArtists(inStoreAudioNetwork_df, 5)

        
        
        
        

        

        #uniqueSongs = getUniqueSongs(inStoreAudioNetwork_df)

        

        

        #user = spotifyObject.current_user()
        #print(json.dumps(user, sort_keys=True, indent=4))

        #print(unique_songs['artist'][0])
        
        #songSearch = spotifyObject.search(q='artist:' + unique_songs.artist[0] + ' track:' + unique_songs.title[0] + '', limit=1)
        #songSearch = spotifyObject.search(q='artist:' + uniqueSongs_df.iloc[2]['artist'] + ' track: ' + uniqueSongs_df.iloc[2]['title'] + '', limit=3)
        #########print(addSpotifyData(uniqueSongs_df))

        #print(uniqueSongs_df.head())

main()
