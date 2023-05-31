import pymongo
import mysql.connector
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

st.set_page_config(page_title='Youtube_data_scraping', layout="wide")
st.header('YOUTUBE SCRAPING')  # title

col0, col1, col2, col3 = st.columns([15, 1, 1, 1])

api_key = "AIzaSyCO4R7JU8L3M_zU3VW-lixJcd4fosZjVMw"
api_service_name = 'youtube'
api_version = 'v3'
youtube = build(api_service_name, api_version, developerKey=api_key)


def channel_selection():
    channel_id = st.sidebar.text_input('Please enter the channel ID')

    return channel_id


channel_id = channel_selection()

try:
    if channel_id is not None:
        def youtube_channel_id(channel_id):
            raw_data = []
            get_data = youtube.channels().list(part="snippet,contentDetails,statistics", id=channel_id)
            request = get_data.execute()
            for item in request["items"]:
                data = {'channelName': item["snippet"]["title"],
                        'subscription': item["statistics"]["subscriberCount"],
                        'views': item["statistics"]["viewCount"],
                        'total_videos': item["statistics"]["videoCount"],
                        'playlist_id': item["contentDetails"]["relatedPlaylists"]["uploads"]}
                raw_data.append(data)

            return pd.DataFrame(raw_data)


        raw_data = youtube_channel_id(channel_id)

        channel_name = raw_data['channelName'].values[0]


        def get_channel_details():
            get_data = youtube.channels().list(part="snippet,contentDetails,statistics", id=channel_id)
            request = get_data.execute()
            for item in request["items"]:
                raw_data1 = []
                data = {'channel_id': item["id"],
                        'channelName': item["snippet"]["title"],
                        'subscription': item["statistics"]["subscriberCount"],
                        'views': item["statistics"]["viewCount"],
                        'total_videos': item["statistics"]["videoCount"],
                        'playlist_id': item["contentDetails"]["relatedPlaylists"]["uploads"],
                        'description': item["snippet"]["description"],
                        'publishedAt': item["snippet"]["publishedAt"]}

                raw_data1.append(data)

                return pd.DataFrame(raw_data1)


        raw_data1 = get_channel_details()

        playlist_id = raw_data['playlist_id'].values[0]

        with col0:
            st.write('Channel Details')
            st.dataframe(raw_data1)


        def channel_vid(youtube, playlist_id):
            vid_data=[]
            get_data=youtube.playlistItems().list(part="snippet,contentDetails",
                                    playlistId=playlist_id, maxResults=40)
            request=get_data.execute()
            for item in request["items"]:
                vid_data.append(item["contentDetails"]["videoId"])
            next_page=request.get("nextPageToken")
            while next_page is not None:
                get_data=youtube.playlistItems().list(
                        part='contentDetails',
                        playlistId=playlist_id,
                        maxResults=40,
                        pageToken = next_page)
                request=get_data.execute()
                for item in request["items"]:
                    vid_data.append(item["contentDetails"]["videoId"])
                next_page=request.get("nextPageToken")
            return vid_data


        vid_data = channel_vid(youtube, playlist_id)


        def video_info(youtube, vid_data):
            vid_info=[]
            for i in range(0, len(vid_data),40):
                get_data=youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(vid_data[i:i+40]))
                value=get_data.execute()
                for vid in value['items']:
                    details={'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt'],
                             'statistics': ['viewCount', 'likeCount', 'commentCount'],
                             'contentDetails': ['duration', 'definition']}
                    info={}
                    info['video_id']= vid['id']
                    for j in details.keys():
                        for k in details[j]:
                            try:
                                info[k] = vid[j][k]
                            except:
                                info[k] = None
                    vid_info.append(info)
            return pd.DataFrame(vid_info)


        vid_df = video_info(youtube, vid_data)

        with col0:
            st.write('Channel video details')
            st.dataframe(vid_df)


        def vid_comments(youtube, vid_data):
            all_com = []
            com = ["Comments not available"]
            for i in vid_data:
                try:
                    val = youtube.commentThreads().list(
                        part="snippet,replies",
                        videoId=i,
                        maxResults=10)
                    data = val.execute()
                    if 'items' in data:
                        all_comments = [j['snippet']['topLevelComment']['snippet']['textOriginal'] for j in data['items']]
                    else:
                        all_comments = ["Comments Not Available"]
                    comments_data = {'video_id': i, 'comments': all_comments}
                    all_com.append(comments_data)
                except HttpError as e:
                    if e.resp.status in [403, 400]:
                        comments_data1 = {'video_id': i, 'comments': com}
                        all_com.append(comments_data1)
                    else:
                        comments_data1 = {'video_id': i, 'comments': com}
                        all_com.append(comments_data1)
            return pd.DataFrame(all_com)


        comments = vid_comments(youtube, vid_data)

        with col0:
            st.write('video Comments')
            st.dataframe(comments)


        def database_mdb():  # function to import df to mongodb

            client = pymongo.MongoClient(
                    "mongodb+srv://Baskar:Baskar123@cluster0.v4dvebx.mongodb.net/?retryWrites=true&w=majority")
            channel = channel_name.replace(" ", "_")
            db = client[channel]
            collection1 = db["Channel_details"]
            channel_details = raw_data1.to_dict(orient="records")
            collection2 = db["Channel_videos"]
            channel_videos = vid_df.to_dict(orient="records")
            collection3 = db["Channel_comments"]
            video_comments = comments.to_dict(orient="records")
            if st.sidebar.button("Import to Mongodb"):
                collection1.insert_many(channel_details)
                collection2.insert_many(channel_videos)
                collection3.insert_many(video_comments)
                st.sidebar.write("uploaded")
            else:
                st.sidebar.write("☝ Click here to upload")
            client.close()

        database_mdb()


except Exception as e:
    if channel_id is not None:
        st.sidebar.write()


def fetch_data():
    client = pymongo.MongoClient(
            "mongodb+srv://Baskar:Baskar123@cluster0.v4dvebx.mongodb.net/?retryWrites=true&w=majority")
    a = [raw for raw in client.list_database_names()]
    chan_nam = a[:-10]
    channel_nam = st.sidebar.selectbox("Please select channel name to migrate to SQL", chan_nam)
    data = client.get_database(channel_nam)
    channel_details = []
    video_details = []
    channel_com = []
    cursor = data.Channel_details.find({})
    cursor1 = data.Channel_videos.find({})
    cursor2 = data.Channel_comments.find({})
    for i in cursor:
        channel_details.append(i)
    for j in cursor1:
        video_details.append(j)
    for k in cursor2:
        channel_com.append(k)
    det = pd.DataFrame(channel_details)
    channel_det = det.drop('_id', axis=1)
    channel_det['publishedAt'] = pd.to_datetime(channel_det['publishedAt'])
    vid = pd.DataFrame(video_details)
    video_det = vid.drop('_id', axis=1)
    video_det['publishedAt'] = pd.to_datetime(video_det['publishedAt'])
    chan = pd.DataFrame(channel_com)
    channel_com = chan.drop('_id', axis=1)

    return channel_nam, channel_det, video_det, channel_com


channel_nam, channel_details, video_details, channel_comments = fetch_data()

with col0:
    if st.sidebar.button("click here to view the channel details"):
        st.write("Channel Details from MongoDB ")
        st.dataframe(channel_details)
        st.write("Video Details from MongoDB ")
        st.dataframe(video_details)
        st.write("Channel comments from MongoDB ")
        st.dataframe(channel_comments)


def create_database():

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Baskar@123")

    mycursor = mydb.cursor()
    database_name = channel_nam
    create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
    mycursor.execute(create_database_query)

    mydb.commit()
    mycursor.close()
    mydb.close()


create_database()


def sql_database():

    if st.sidebar.button("Import to SQL"):
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Baskar@123",
            database=f"{channel_nam}")

        mycursor = mydb.cursor()
        mycursor.execute("CREATE TABLE IF NOT EXISTS Channel_Details (channel_id VARCHAR(255), "
                         "channelName VARCHAR(255), subscription INT, views INT, total_videos INT, "
                         "playlist_id VARCHAR(255),description TEXT, publishedAt DATETIME)")
        for _, row in channel_details.iterrows():
            values = (
                str(row['channel_id']),
                str(row['channelName']),
                int(row['subscription']),
                int(row['views']),
                int(row['total_videos']),
                str(row['playlist_id']),
                str(row['description']),
                str(row['publishedAt'])
            )
            sql = "INSERT INTO Channel_Details (channel_id, channelName, subscription, views, total_videos, playlist_id, description, publishedAt) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            mycursor.execute(sql, values)

        mycursor1 = mydb.cursor()
        mycursor1.execute("CREATE TABLE IF NOT EXISTS Video_Details (video_id VARCHAR(255), channelTitle VARCHAR(255),"
                            "title VARCHAR(255), description TEXT, tags TEXT , publishedAt DATETIME,"
                            "viewCount INT, likeCount INT,  commentCount INT, duration TEXT, definition TEXT)")
        video_details['commentCount'] = video_details['commentCount'].fillna('Comments Not available')
        for _, row in video_details.iterrows():
            values = (
                str(row['video_id']),
                str(row['channelTitle']),
                str(row['title']),
                str(row['description'])[:255],
                str(row['tags']),
                str(row['publishedAt']),
                int(row['viewCount']),
                int(row['likeCount']),
                int(row['commentCount']),
                str(row['duration']),
                str(row['definition'])
            )
            sql = "INSERT INTO Video_Details (video_id, channelTitle, title, description, tags, publishedAt, viewCount, likeCount, commentCount, duration, definition) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            mycursor1.execute("ALTER TABLE Video_Details MODIFY description TEXT")
            mycursor1.execute("ALTER TABLE Video_Details MODIFY tags TEXT")
            mycursor1.execute(sql, values)

        mycursor2 = mydb.cursor()
        mycursor2.execute("CREATE TABLE IF NOT EXISTS Video_Comments (video_id VARCHAR(255), comments TEXT)")
        channel_comments['comments'] = channel_comments['comments'].fillna('Comments Not available')

        for _, row in channel_comments.iterrows():
            values = (
                str(row['video_id']),
                ','.join(row['comments'])
            )

            sql = "INSERT INTO Video_Comments (video_id, comments) VALUES (%s, %s)"
            mycursor2.execute("ALTER TABLE Video_Comments MODIFY comments TEXT")
            mycursor2.execute(sql, values)

        st.sidebar.write("Uploated to SQL")
        mydb.commit()
        mycursor.close()
        mycursor1.close()
        mycursor2.close()
        mydb.close()
    else:
        st.sidebar.write()


sql_database()


def sql_query():

    Query = st.sidebar.selectbox("Select Query to fetch the data from SQL", (
            "Query1. What are the names of all the videos?",
            "Query2. Channel name and total videos?",
            "Query3. What are the top 10 most viewed videos?",
            "Query4. How many comments were made on each video and video names?",
            "Query5. Which videos have the highest number of likes and video names?",
            "Query6. What is the total number of likes for each video, and video names?",
            "Query7. What is the total number of views for channel, and channel names?",
            "Query8. How many videos have published videos in the year 2023?",
            "Query9. What is the longest duration video and video names?",
            "Query10.What is the Videos with highest number of comments?"
            ))
    return Query


Query = sql_query()


def sql_database():

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Baskar@123"
    )
    mycursor = mydb.cursor()
    mycursor.execute("SHOW DATABASES")
    databases = mycursor.fetchall()
    a = []
    for db in databases:

        a.append(db)
    del a[1]
    del a[1]
    del a[-2]
    a = ["".join(item) for item in a]
    mydb.close()

    return a


database_name = sql_database()


def query_data():
    database = st.sidebar.selectbox("please select SQL database", database_name)
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Baskar@123",
        database=f"{database}")

    mycursor = mydb.cursor()
    mycursor.execute("SELECT title, channelTitle FROM Video_Details")
    query1_result = mycursor.fetchall()

    mycursor.execute(
        "SELECT channelTitle, COUNT(*) AS videoCount FROM Video_Details GROUP BY channelTitle ORDER BY videoCount DESC")
    query2_result = mycursor.fetchall()

    mycursor.execute("SELECT title, channelTitle FROM Video_Details ORDER BY viewCount DESC LIMIT 10")
    query3_result = mycursor.fetchall()

    mycursor.execute("SELECT title, channelTitle, commentCount FROM Video_Details")
    query4_result = mycursor.fetchall()

    mycursor.execute("""SELECT channelTitle, title, likeCount
        FROM Video_Details
        WHERE likeCount = (
            SELECT MAX(likeCount)
            FROM Video_Details
        )
    """)
    query5_result = mycursor.fetchall()

    mycursor.execute(
        "SELECT video_id, SUM(likeCount) AS totalLikes FROM Video_Details GROUP BY video_id")
    query6_result = mycursor.fetchall()

    mycursor.execute("SELECT channelTitle, SUM(viewCount) AS totalViews FROM Video_Details GROUP BY channelTitle")
    query7_result = mycursor.fetchall()

    mycursor.execute("SELECT DISTINCT channelTitle FROM Video_Details WHERE YEAR(publishedAt) = 2022")
    query8_result = mycursor.fetchall()

    mycursor.execute("SELECT channelTitle, duration FROM Video_Details ORDER BY duration DESC LIMIT 1;")
    query9_result = mycursor.fetchall()

    mycursor.execute("SELECT channelTitle, title, commentCount FROM Video_Details ORDER BY commentCount DESC LIMIT 5")
    query10_result = mycursor.fetchall()

    # Display query results as tables
    if 'Query1' in Query:
        st.write("1. Names of all videos and their corresponding channels:")
        query1_df = pd.DataFrame(query1_result, columns=["Video Title", "Channel Name"])
        st.dataframe(query1_df)

    if 'Query2' in Query:
        st.write("2. Channels with the most number of videos and their video count:")
        query2_df = pd.DataFrame(query2_result, columns=["Channel Name", "Video Count"])
        st.dataframe(query2_df)

    if 'Query3' in Query:
        st.write("3. Top 10 most viewed videos and their respective channels:")
        query3_df = pd.DataFrame(query3_result, columns=["Video Title", "Channel Name"])
        st.dataframe(query3_df)

    if 'Query4' in Query:
        st.write("4. Number of comments made on each video and their corresponding video names:")
        query4_df = pd.DataFrame(query4_result, columns=["Channel Title", "Channel Name", "Comment Count"])
        st.dataframe(query4_df)

    if 'Query5' in Query:
        st.write("5. Videos with the highest number of likes and their corresponding channel names:")
        query5_df = pd.DataFrame(query5_result, columns=["Channel Name", "Video Title", "LikesCount"])
        st.dataframe(query5_df)

    if 'Query6' in Query:
        st.write("6. Total number of likes for each video and their corresponding video names:")
        query6_df = pd.DataFrame(query6_result, columns=["Video ID", "Total Likes"])
        st.dataframe(query6_df)

    if 'Query7' in Query:
        st.write("7. Total number of views for each channel and their corresponding channel names:")
        query7_df = pd.DataFrame(query7_result, columns=["Channel Name", "Total Views"])
        st.dataframe(query7_df)

    if 'Query8' in Query:
        st.write("8. Names of all channels that have published videos in the year 2022:")
        query8_df = pd.DataFrame(query8_result, columns=["Channel Name"])
        st.dataframe(query8_df)

    if 'Query9' in Query:
        st.write("9. What is the longest duration video and video names:")
        query9_df = pd.DataFrame(query9_result, columns=["Channel Name", "Average Duration"])
        st.dataframe(query9_df)

    if 'Query10' in Query:
        st.write("10.Videos with highest number of comments:")
        query10_df = pd.DataFrame(query10_result, columns=["Channel Name", "Video Title", "Comments Count"])
        st.dataframe(query10_df)


query_data()
