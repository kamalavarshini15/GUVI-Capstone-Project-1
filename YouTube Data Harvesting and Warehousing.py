import os
import pymongo
import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
import mysql.connector as sql
from sqlalchemy import create_engine
import re

# Set up your YouTube Data API key
API_KEY = "AIzaSyCCpNJiY820TBqJ1zgwGrD7EoVjKZd924E"

# MongoDB setup for data lake
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["youtube_data_lake"]
collection = db["youtube_data_collection"]


# Function to fetch channel data using the API
def fetch_channel_data(channel_id):
    youtube = build("youtube", "v3", developerKey=API_KEY)
    response = youtube.channels().list(
        part="snippet, statistics, contentDetails",
        id=channel_id
    ).execute()

    channel_data = {
        "channel_id": channel_id,
        "channel_title": response["items"][0]["snippet"]["title"],
        "subscribers": int(response["items"][0]["statistics"]["subscriberCount"]),
        "total_videos": int(response["items"][0]["statistics"]["videoCount"]),
        "playlist_id": response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
        "Views": response['items'][0]['statistics']['viewCount'],
        "Description": response['items'][0]['snippet']['description'],
        "Country": response['items'][0]['snippet'].get('country')
    }

    return channel_data

# Function to fetch video data using the API
def fetch_video_data(video_id):
    youtube = build("youtube", "v3", developerKey=API_KEY)
    response = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id
    ).execute()

    video_stats = []
    for video in response['items']:
        video_details = dict(
            Channel_name=video['snippet']['channelTitle'],
            Channel_id=video['snippet']['channelId'],
            Vd_id=video['id'],
            Title=video['snippet']['title'],
            Tags=video['snippet'].get('tags'),
            Thumbnail=video['snippet']['thumbnails']['default']['url'],
            Description=video['snippet']['description'],
            Published_date=video['snippet']['publishedAt'],
            Duration=video['contentDetails']['duration'],
            Views=video['statistics']['viewCount'],
            Dislikes = video['statistics'].get('dislikeCount'),
            Likes=video['statistics'].get('likeCount'),
            Comments=video['statistics'].get('commentCount'),
            Favorite_count=video['statistics']['favoriteCount'],
            Definition=video['contentDetails']['definition'],
            Caption_status=video['contentDetails']['caption']
        )
        
        video_stats.append(video_details)

    return video_stats

# Function to fetch comments data using the API
def fetch_comments_data(video_id):
    youtube = build("youtube", "v3", developerKey=API_KEY)
    response = youtube.commentThreads().list(
        part="snippet,replies",
        videoId=video_id,
        maxResults=100
    ).execute()

    comment_data = []
    try:
        next_page_token = None
        while True:
            for cmt in response['items']:
                data = dict(
                    Comment_id=cmt['id'],
                    Vd_id=cmt['snippet']['videoId'],
                    Comment_text=cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_author=cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_posted_date=cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                    Like_count=cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                    Reply_count=cmt['snippet']['totalReplyCount']
                )
                comment_data.append(data)

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break

            response = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id,
                maxResults=100,
                pageToken=next_page_token
            ).execute()
    except:
        pass

    return comment_data



# Function to store channel, video, and comments data in MongoDB data lake
def store_data_in_mongodb(channel_id, video_id):
    channel_data = fetch_channel_data(channel_id)
    video_data = fetch_video_data(video_id)
    comments_data = fetch_comments_data(video_id)

    videos_to_store = []
    comments_to_store = []
    for comments_info in comments_data:
       """ comments_to_store = [{
                "Comment_id" : comments_info["Comment_id"],
                 "Vd_id" : comments_info["Vd_id"],
                 "Comment_text" : comments_info["Comment_text"],
                 "Comment_author" : comments_info["Comment_author"] ,
                 "Comment_posted_date" : comments_info["Comment_posted_date"],
                 "Like_count" : comments_info["Like_count"],
                 "Reply_count" : comments_info["Reply_count"] 
        }]"""
       comments_to_store = comments_data

    hour_pattern = re.compile(r'(\d+)H')
    minutes_pattern = re.compile(r'(\d+)M')
    seconds_pattern = re.compile(r'(\d+)S')
    

    for video_info in video_data:
        t = video_info["Duration"]
        hours = hour_pattern.search(t)
        minutes  = minutes_pattern.search(t)
        seconds = seconds_pattern.search(t)

        hours = int(hours.group(1)) if hours else 0
        minutes = int(minutes.group(1)) if minutes else 0
        seconds = int(seconds.group(1)) if seconds else 0

        total_seconds = seconds + (minutes*60) +(hours * 3600)
        
        print( total_seconds)
        video_to_store = {
            "video_id": video_info["Vd_id"],
            "title": video_info["Title"],
            "channel_title": video_info["Channel_name"],
            "views": video_info["Views"],
            "likes": video_info["Likes"],
            "dislikes": video_info["Dislikes"],
            "published_date" : video_info["Published_date"],
            "duration" : total_seconds,
            "comments" :   comments_to_store
        }   
            #"comments": [comment["Comment_text"] for comment in comments_data],
            
       
        videos_to_store.append(video_to_store)

    data_to_store = {
        "channel_id": channel_data["channel_id"],
        "channel_title": channel_data["channel_title"],
        "subscribers": channel_data["subscribers"],
        "total_videos": channel_data["total_videos"],
        "playlist_id": channel_data["playlist_id"],
        "videos": videos_to_store
    }

    collection.insert_one(data_to_store)



#Builting Streamlit Application for Data Harvesting

st.title("YouTube Data Harvesting")
data_type = st.selectbox("Select Data Type", ["Channel Data"])

if data_type == "Channel Data":
        st.header("YouTube Channel Data Harvesting")
        channel_id = st.text_input("Enter YouTube Channel ID")
        video_id = st.text_input("Enter YouTube Video ID")
        
        if st.button("Fetch Channel Data and store it!"):
            channel_data = fetch_channel_data(channel_id)
            st.table(channel_data)
            store_data_in_mongodb(channel_id, video_id)
            st.success("Data stored in MongoDB data lake!")




# Function to fetch data from MongoDB data lake
def fetch_data_from_mongodb():
    data = []
    for doc in collection.find():
        data.append(doc)
    return data

# Streamlit application
st.title("YouTube Data Lake Viewer")

data = fetch_data_from_mongodb()

if st.checkbox("Show raw data"):
    st.write(data)

if data:
    # Extract channel titles for the selectbox
    channel_titles = [item["channel_title"] for item in data]



if data:
    # Display channel data
    st.header("Channel Data")
    channel_data = []
    for item in data:
        channel_data.append({
            "Channel ID": item["channel_id"],
            "Channel Title": item["channel_title"],
            "Subscribers": item["subscribers"],
            "Total Videos": item["total_videos"],
            "Playlist ID": item["playlist_id"]
        })
    st.table(pd.DataFrame(channel_data))

    # Display video data
    st.header("Video Data")
    video_data = []
    for item in data:
        for video in item["videos"]:
            video_data.append({
                "Video ID": video["video_id"],
                "Title": video["title"],
                "Channel Title": video["channel_title"],
                "Views": video["views"],
                "Likes": video["likes"],
                "Dislikes": video["dislikes"],
                "Published_date" : video["published_date"],
                "Duration" : video["duration"]
            })
    st.table(pd.DataFrame(video_data))


# Display comments data
st.header("Comments Data")
comments_data = []
for item in data:
    for video in item["videos"]:
        for comment in video["comments"]:
            comments_data.append({
                "Video ID": video["video_id"],  # Accessing the video ID from the 'video' dictionary
                "Comment Text": comment,  # Comment is now a string, not a dictionary
                # You can add additional fields here if needed
            })
st.table(pd.DataFrame(comments_data).head(10))




# Function to create MySQL tables for channels and videos
def create_mysql_tables():
    mysql_connection = sql.connect(
        host="localhost",
        user="root",
        password="Ponni2001$",
        database="youtube_data"
    )
    mysql_cursor = mysql_connection.cursor()

    create_channel_table_query = """
        CREATE TABLE IF NOT EXISTS channels (
            channel_id VARCHAR(50) PRIMARY KEY,
            channel_title VARCHAR(255),
            subscribers INT,
            total_videos INT,
            playlist_id VARCHAR(50)
        )
    """

    create_video_table_query = """
        CREATE TABLE IF NOT EXISTS videos (
            video_id VARCHAR(50) PRIMARY KEY,
            title VARCHAR(255),
            channel_title VARCHAR(100),
            views INT,
            likes INT,
            dislikes INT,
            published_date DATETIME,
            duration INT            
        )
    """
    create_comments_table_query = """
        CREATE TABLE IF NOT EXISTS comments (                                
            Comment_id VARCHAR(255) PRIMARY KEY,
            Vd_id VARCHAR(255),
            Comment_text TEXT,
            Comment_author VARCHAR(255),
            Comment_posted_date DATETIME
        )
    """






    mysql_cursor.execute(create_channel_table_query)
    mysql_cursor.execute(create_video_table_query)
    mysql_cursor.execute(create_comments_table_query)
    
    mysql_cursor.close()
    mysql_connection.close()





# Function to migrate data from MongoDB data lake to MySQL database
def migrate_data_to_mysql():
    # Connect to the MySQL database
    mysql_connection = sql.connect(
        host="localhost",
        user="root",
        password="Ponni2001$",
        database="youtube_data"
    )
    mysql_cursor = mysql_connection.cursor()

    # Create MySQL tables for channels and videos
    create_mysql_tables()

    # Fetch data from MongoDB data lake
    data = fetch_data_from_mongodb()

    # Migrate channel data
    for item in data:
        channel_data = (
            item["channel_id"],
            item["channel_title"],
            item["subscribers"],
            item["total_videos"],
            item["playlist_id"]
        )
        insert_channel_query = """
            INSERT INTO channels (channel_id, channel_title, subscribers, total_videos, playlist_id)
            VALUES (%s, %s, %s, %s, %s)
        """
        mysql_cursor.execute(insert_channel_query, channel_data)
    from datetime import datetime
    # Migrate video data
    for item in data:
        for video in item["videos"]:
            video_data = (
                video["video_id"],
                video["title"],
                video["channel_title"],
                video["views"],
                video["likes"],
                video["dislikes"],
                datetime.strptime(video["published_date"],'%Y-%m-%dT%H:%M:%S%z'), 
                video["duration"]           
            )
            insert_video_query = """
                INSERT INTO videos (video_id, title, channel_title, views, likes, dislikes,published_date,duration)
                VALUES (%s, %s, %s, %s, %s, %s,%s,%s)
            """
            mysql_cursor.execute(insert_video_query, video_data)
    from datetime import datetime
    for item in data:
        for video in item["videos"]:
            for comments in video["comments"]:
                comments_data = (
                    comments["Comment_id"],
                    comments["Vd_id"],
                    comments["Comment_text"],
                    comments["Comment_author"],
                    datetime.strptime(comments["Comment_posted_date"],'%Y-%m-%dT%H:%M:%S%z')
                )
                insert_comments_query = """
                    INSERT INTO comments (Comment_id, Vd_id, Comment_text, Comment_author, Comment_posted_date)
                    VALUES (%s, %s, %s, %s, %s)
                """
                mysql_cursor.execute(insert_comments_query, comments_data)
    mysql_connection.commit()
    mysql_cursor.close()
    mysql_connection.close()

# Execute the data migration to MySQL function when the Streamlit app starts
st.title("Migrate to SQL")

if st.button("Store data in SQL datbase"):
    migrate_data_to_mysql()
    st.success("Data stored in SQL DB!")

#engine= create_engine('mysql+pymysql://root:Ponni2001$@localhost:3306/youtube_data')

def sql_query(query):
    mysql_connection = sql.connect(
        host="localhost",
        user="root",
        password="Ponni2001$",
        database="youtube_data"
    )
    result_df = pd.read_sql_query(query,mysql_connection)
    mysql_connection.close()
    return result_df



analysis = st.selectbox("Select the analysis you need!",[
"1. What are the names of all the videos and their corresponding channels?",
"2. Which channels have the most number of videos, and how many videos do they have?",
"3. What are the top 10 most viewed videos and their respective channels?",
"4. How many comments were made on each video, and what are their corresponding video names?",
"5. Which videos have the highest number of likes, and what are their corresponding channel names?",
"6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
"7. What is the total number of views for each channel, and what are their corresponding channel names?",
"8. What are the names of all the channels that have published videos in the year 2022?",
"9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
"10. Which videos have the highest number of comments, and what are their corresponding channel names?"
])

if st.button("Run Analysis"):
    if analysis == "1. What are the names of all the videos and their corresponding channels?":
        query = """
            SELECT v.Title AS Video_Name, v.channel_title AS Channel_Name
            FROM videos v;
            """
    elif analysis == "2. Which channels have the most number of videos, and how many videos do they have?":
        query =  """select channel_title , total_videos from channels where total_videos = (select max(total_videos) from channels )"""

    elif analysis == "3. What are the top 10 most viewed videos and their respective channels?":
        query =  """select channel_title , title, views from videos order by views DESC"""

    elif analysis == "4. How many comments were made on each video, and what are their corresponding video names?":
        query = """select   title , count(comment_id) as "Count of comments"  from videos inner join comments on videos.video_id = comments.Vd_id
                   group by title """       

    elif analysis == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
        query ="""select  channels.channel_title ,title , max(likes) as "Highest likes"  
                  from videos inner join channels on channels.channel_title = videos.channel_title
                  group by channels.channel_title ,title
                  order by 3 desc"""
    
    elif analysis == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":  
        query ="""select title, likes, dislikes  from videos"""
    
    elif analysis == "7. What is the total number of views for each channel, and what are their corresponding channel names?":    
        query = """select channels.channel_title,views  from videos inner join channels
                   on videos.channel_title = channels.channel_title """

    elif analysis == "8. What are the names of all the channels that have published videos in the year 2022?":
        query = """SELECT title , published_date from videos  
                   where year(published_date) = "2022" """
    
    elif analysis == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        query = """SELECT title , avg(duration) from videos  
                   group by title"""
        
    elif analysis == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":

        query = """select channels.channel_title, title  ,  count(Comment_id) as "Highest commnets" from channels inner join 
        videos  on channels.channel_title = videos.channel_title inner join comments on videos.video_id = comments.Vd_id group by channels.channel_title, title  """
       
    else:
        st.error("Invalid Analysis")
    
    result_df = sql_query(query)
    st.table(result_df)
