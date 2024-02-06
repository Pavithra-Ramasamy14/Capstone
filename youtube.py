from googleapiclient.discovery import build
from pymongo import MongoClient
import mysql.connector
import pandas as pd
import streamlit as st
import re

def Api_connect():
    api_id="AIzaSyB1P7CxC4hu1xNUFmzfhDUkdg8qKoo8cVE"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name,api_version,developerKey=api_id)
    return youtube
youTube=Api_connect()

#get channel details
def get_channel_details(channel_id):
    request=youTube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
    response = request.execute()
    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
            Channel_Id= i['id'],
            Subscription_Count= i['statistics']['subscriberCount'],
            Channel_Views= i['statistics']['viewCount'],
            Total_Videos=i['statistics']['videoCount'],
            Channel_Description= i['snippet']['description'],
            Playlist_Id= i['contentDetails']['relatedPlaylists']['uploads'])
    return data  


#get video ids
def get_video_details(channel_id):
    video_ids=[]
    response=youTube.channels().list(
                part="contentDetails",
                id=channel_id).execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None
    while True:
        response1=youTube.playlistItems().list(part='snippet',
                                            playlistId=Playlist_Id,maxResults=50,pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')
        
        if next_page_token is None:
            break   
    return video_ids  


#get video details
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youTube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        def convert_duration(duration):
            regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
            match = re.match(regex, duration)
            if not match:
                return '00:00:00'
            hours, minutes, seconds = match.groups()
            hours = int(hours[:-1]) if hours else 0
            minutes = int(minutes[:-1]) if minutes else 0
            seconds = int(seconds[:-1]) if seconds else 0
            total_seconds = hours * 3600 + minutes * 60 + seconds
            return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds / 3600), int((total_seconds % 3600) / 60), int(total_seconds % 60))
    
        for item in response['items']:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                Channel_Id=item['snippet']['channelId'],
                video_Id=item['id'],
                Title=item['snippet']['title'],
                Tags=item['snippet'].get('tags'),
                Thumbnail=item['snippet']['thumbnails']['default']['url'],
                Description=item['snippet'].get('description',0),
                Published_Date=item['snippet']['publishedAt'],
                Duration=convert_duration(item['contentDetails']['duration']),
                Views=item['statistics'].get('viewCount'),
                Comments=item['statistics'].get('commentCount',0),
                Like_count=item['statistics'].get('likeCount',0),
                Dislike_count=item['statistics'].get('dislikecount',0),
                Favourite_count=item['statistics']['favoriteCount'],
                Definition=item['contentDetails']['definition'],
                Caption_status=item['contentDetails']['caption'])
            video_data.append(data)
            
    return video_data        

#get comment_details
def get_comment_info(video_ids):
    try:
        comment_data=[]
        for video_id in video_ids:
            request=youTube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50)
            response=request.execute()
            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt']
                        )
                comment_data.append(data)
    except:
        pass
    return comment_data

#get playlist details
def get_playlist_details(channel_id):
    playlist_data=[]
    next_page_token=None
    while True:
        request=youTube.playlists().list(
                        part="snippet,contentDetails",
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token)
        response=request.execute()

        for item in response['items']:
            data=dict(Playlist_Id=item['id'],
                    Playlist_name=item['snippet']['title'],
                    Channel_Id=item['snippet']['channelId'],
                    Channel_Name=item['snippet']['channelTitle'],
                    PublishedAt=item['snippet']['publishedAt'],
                    )
            playlist_data.append(data)
        next_page_token=response.get('nextPageToken')
                
        if next_page_token is None:
            break
    return playlist_data

#upload mongodb
client = MongoClient('mongodb://localhost:27017/')
db=client["Youtube_data"]

def channel_info(channel_id):
    ch_details=get_channel_details(channel_id)
    vi_ids=get_video_details(channel_id)
    vi_details=get_video_info(vi_ids) 
    com_details=get_comment_info(vi_ids)  
    pl_details=get_playlist_details(channel_id)
    collection=db["channel_details"]
    collection.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                           "video_information":vi_details,"comment_information":com_details})
    return "uploaded successfully"


#create channel table in mysql
def channel_table():
    config = {
        'user':'root', 'password':'pavi',
        'host':'127.0.0.1', 'database':'youtube_data'
    }
    connection=mysql.connector.connect(**config)
    cursor=connection.cursor()

    drop_query="""drop table if exists channels"""
    cursor.execute(drop_query)
    connection.commit()

    try:
        Create_Query = """Create table channels(Channel_Name varchar(255),
        Channel_Id varchar(255) PRIMARY KEY, 
        Subscription_Count bigint,
        Channel_Views bigint,
        Total_Videos bigint,
        Channel_Description text,
        Playlist_Id varchar(255));"""
        cursor.execute(Create_Query)
        connection.commit()
    except:
        pass
    channel_list=[]
    db=client["Youtube_data"]
    collection=db["channel_details"]
    for channel_data in collection.find({},{"_id":0,"channel_information":1}):
        channel_list.append(channel_data["channel_information"])
    df=pd.DataFrame(channel_list) 

    #insert channel values in channels table
    for index,row in df.iterrows():
        insert_Query="""insert into channels(Channel_Name,
        Channel_Id , 
        Subscription_Count ,
        Channel_Views ,
        Total_Videos,
        Channel_Description,
        Playlist_Id) values(%s,%s,%s,%s,%s,%s,%s);"""
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Channel_Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        try:
            cursor.execute(insert_Query,values)
            connection.commit()
        except:
            pass
            
def playlist_table():
    config = {
            'user':'root', 'password':'pavi',
            'host':'127.0.0.1', 'database':'youtube_data'
        }
    connection=mysql.connector.connect(**config)
    cursor=connection.cursor()

    drop_query="""drop table if exists playlists"""
    cursor.execute(drop_query)
    connection.commit()

    try:
        Create_Query = """Create table playlists(Playlist_Id varchar(100) PRIMARY KEY,
        Playlist_name varchar(255),
        Channel_Id varchar(255),
        Channel_Name varchar(255),
        PublishedAt DATETIME);"""
        cursor.execute(Create_Query)
        connection.commit()
    except:
        pass
    playlist_list=[]
    db=client["Youtube_data"]
    collection=db["channel_details"]
    for playlist_data in collection.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(playlist_data["playlist_information"])):
            playlist_list.append(playlist_data["playlist_information"][i])
    df1=pd.DataFrame(playlist_list) 

    #insert values in playlist table
    for index,row in df1.iterrows():
        row['PublishedAt']=row['PublishedAt'].replace('T'," ")
        row['PublishedAt']=row['PublishedAt'].replace('Z',"")

        insert_Query="""insert into playlists(Playlist_Id,
        Playlist_name,
        Channel_Id,
        Channel_Name,
        PublishedAt) values(%s,%s,%s,%s,%s);"""
        
        values=(row['Playlist_Id'],
                row['Playlist_name'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['PublishedAt']
                );

        try:     
            cursor.execute(insert_Query,values)
            connection.commit()
            
        except:
            pass

#create video table in mysql
def video_table():
    config = {
        'user':'root', 'password':'pavi',
        'host':'127.0.0.1', 'database':'youtube_data'
    }
    connection=mysql.connector.connect(**config)
    cursor=connection.cursor()

    drop_query="""drop table if exists videos"""
    cursor.execute(drop_query)
    connection.commit()

    try:
        Create_Query = """Create table videos(Channel_Name varchar(255),
        Channel_Id varchar(255),
        video_Id varchar(255) PRIMARY KEY,
        Title varchar(255),
        Tags text,
        Thumbnail varchar(255),
        Description text,
        Published_Date datetime,
        Duration time,
        Views bigint,
        Comments int,
        Like_count bigint,
        Dislike_count bigint,
        Favourite_count int,
        Definition varchar(255),
        Caption_status varchar(255));"""
        cursor.execute(Create_Query)
        connection.commit()
    except:
        pass

    video_list=[]
    db=client["Youtube_data"]
    collection=db["channel_details"]
    for video_data in collection.find({},{"_id":0,"video_information":1}):
        for i in range(len(video_data["video_information"])):
            video_list.append(video_data["video_information"][i])
    df2=pd.DataFrame(video_list)

    #insert values in video table
    for index,row in df2.iterrows():
        row['Published_Date']=row['Published_Date'].replace('T'," ")
        row['Published_Date']=row['Published_Date'].replace('Z',"")
        if(type(row['Tags'])==list):
            row['Tags']=",".join(str(element) for element in row['Tags'])
        
        insert_Query="""insert into videos(Channel_Name,
        Channel_Id,
        video_Id,
        Title,
        Tags,
        Thumbnail,
        Description,
        Published_Date,
        Duration,
        Views,
        Comments,
        Like_count,
        Dislike_count,
        Favourite_count,
        Definition,
        Caption_status) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
        
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnail'],
                row['Description'],
                row['Published_Date'],
                row['Duration'],
                row['Views'],
                row['Comments'],
                row['Like_count'],
                row['Dislike_count'],
                row['Favourite_count'],
                row['Definition'],
                row['Caption_status']);

        try:     
            cursor.execute(insert_Query,values)
            connection.commit()
                
        except:
            pass

def comment_table():
    config = {
    'user':'root', 'password':'pavi',
    'host':'127.0.0.1', 'database':'youtube_data'
    }
    connection=mysql.connector.connect(**config)
    cursor=connection.cursor()

    drop_query="""drop table if exists comments"""
    cursor.execute(drop_query)
    connection.commit()
    try:
        Create_Query = """Create table comments(Comment_Id varchar(255) PRIMARY KEY,
        Video_Id varchar(255),
        Comment_Text text,
        Comment_Author varchar(255),
        Comment_Published DATETIME);"""

        cursor.execute(Create_Query)
        connection.commit()
    except:
        pass

    comment_list=[]
    db=client["Youtube_data"]
    collection=db["channel_details"]
    for comment_data in collection.find({},{"_id":0,"comment_information":1}):
        if(comment_data["comment_information"]!=[]):
            for i in range(0,len(comment_data["comment_information"])):
                comment_list.append(comment_data["comment_information"][i])
            
    df3=pd.DataFrame(comment_list)

    #insert values in comment table
    for index,row in df3.iterrows():
        row['Comment_Published']=row['Comment_Published'].replace('T'," ")
        row['Comment_Published']=row['Comment_Published'].replace('Z',"")

        insert_Query="""insert into comments(Comment_Id,
        Video_Id,
        Comment_Text,
        Comment_Author,
        Comment_Published) values(%s,%s,%s,%s,%s);"""

        values=(row['Comment_Id'],
            row['Video_Id'],
            row['Comment_Text'],
            row['Comment_Author'],
            row['Comment_Published']
            );
        try:         
            cursor.execute(insert_Query,values)
            connection.commit()
            
        except:
            pass

def tables():
     channel_table()
     playlist_table()
     video_table()
     comment_table()
     return "Data's successfully migrated to sql"

#To show tables in streamlit
def show_channel_table():
    channel_list=[]
    db=client["Youtube_data"]
    collection=db["channel_details"]
    for channel_data in collection.find({},{"_id":0,"channel_information":1}):
        channel_list.append(channel_data["channel_information"])
    df=st.dataframe(channel_list)
    return df

def show_playlist_table():
    playlist_list=[]
    db=client["Youtube_data"]
    collection=db["channel_details"]
    for playlist_data in collection.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(playlist_data["playlist_information"])):
            playlist_list.append(playlist_data["playlist_information"][i])
    df1=st.dataframe(playlist_list)
    return df1 

def show_video_table():
    video_list=[]
    db=client["Youtube_data"]
    collection=db["channel_details"]
    for video_data in collection.find({},{"_id":0,"video_information":1}):
        for i in range(len(video_data["video_information"])):
            video_list.append(video_data["video_information"][i])
    df2=st.dataframe(video_list)
    return df2

def show_comments_table():
    comment_list=[]
    db=client["Youtube_data"]
    collection=db["channel_details"]
    for comment_data in collection.find({},{"_id":0,"comment_information":1}):
        if(comment_data["comment_information"]!=[]):
            for i in range(0,len(comment_data["comment_information"])):
                comment_list.append(comment_data["comment_information"][i])
    df3=st.dataframe(comment_list)
    return df3

#stramlit code
st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
Channel_Id=st.text_input("Enter the channel id")
if st.button("Collect and store data"):
    db=client["Youtube_data"]
    collection=db["channel_details"]
    insert=channel_info(Channel_Id)
    st.success("Channel details extracted and successfully stored in Mongodb")
if st.button("Migrate to sql"):
    Table=tables()
    st.success(Table)

show_table=st.radio("SELECT THE TABLE TO VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))
if show_table=="CHANNELS":
    show_channel_table()
elif show_table=="PLAYLISTS":
    show_playlist_table()
elif show_table=="VIDEOS":
    show_video_table()
elif show_table=="COMMENTS":
    show_comments_table()

#sql connection
config = {
    'user':'root', 'password':'pavi',
    'host':'127.0.0.1', 'database':'youtube_data'
    }
connection=mysql.connector.connect(**config)
cursor=connection.cursor()
questions=st.selectbox("SELECT YOUR QUESTION",("1.What are the names of all the videos and their corresponding channels?",
                                                "2.Which channels have the most number of videos, and how many videos do they have?",
                                                "3.What are the top 10 most viewed videos and their respective channels?",
                                                "4.How many comments were made on each video, and what are their corresponding video names?",
                                                "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                                "8.What are the names of all the channels that have published videos in the year 2022?",
                                                "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))
if questions == '1.What are the names of all the videos and their corresponding channels?':
    query1 = "select Title as videos, Channel_Name as ChannelName from videos;"
    cursor.execute(query1)
    t1=cursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))
elif questions =="2.Which channels have the most number of videos, and how many videos do they have?":
    query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
    t2=cursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))
elif questions == "3.What are the top 10 most viewed videos and their respective channels?":
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))
elif questions == "4.How many comments were made on each video, and what are their corresponding video names?":
    query4 = '''select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;'''
    cursor.execute(query4)
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))
elif questions == "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Like_count as LikesCount from videos 
                       where Like_count is not null order by Like_count desc;'''
    cursor.execute(query5)
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","Like count"]))
elif questions == '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    query6 = '''select Like_count as likeCount,Dislike_count as DislikeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["Like count","Dislike count","video title"]))
elif questions == '7.What is the total number of views for each channel, and what are their corresponding channel names?':
    query7 = "select Channel_Name as ChannelName,Channel_Views as Channelviews from channels;"
    cursor.execute(query7)
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

elif questions == "8.What are the names of all the channels that have published videos in the year 2022?":
    query8 = '''select Channel_Name as ChannelName,Published_Date as VideoRelease from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["ChannelName", "Video Publised On"]))
elif questions == "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    t9=cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))

elif questions == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10 = '''select Channel_Name as ChannelName, Comments as comments from videos ORDER BY Comments DESC limit 1;'''
    cursor.execute(query10)
    t10=cursor.fetchall()
    st.write(pd.DataFrame(t10, columns=['Channel Name', 'NO Of Comments']))
    

