# GUVI-Capstone-Project-1

**Project Title** : YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit

**Skills take away From This Project** : Python scripting, Data Collection,MongoDB, Streamlit, API integration, Data Managment using MongoDB (Atlas) and SQL

**Domain** : Social Media

------------------------------------------------------------------------------------------------

**Problem Statement:**

The problem statement is to create a Streamlit application that allows users to access
and analyze data from multiple YouTube channels. The application should have the
following features:

1. Ability to input a YouTube channel ID and retrieve all the relevant data
(Channel name, subscribers, total video count, playlist ID, video ID, likes,
dislikes, comments of each video) using Google API.
2. Option to store the data in a MongoDB database as a data lake.
3. Ability to collect data for up to 10 different YouTube channels and store them in
the data lake by clicking a button.
4. Option to select a channel name and migrate its data from the data lake to a
SQL database as tables.
5. Ability to search and retrieve data from the SQL database using different
search options, including joining tables to get channel details.

------------------------------------------------------------------------------------------------

**Approach:**


**>**Set up a streamlit application for Analysis of the YOUTUBE Data.
**>** Set up your Youtube Data API key with the YOUTUBE API DATA to fetch the resources Youtube API provides.
**>**Store Data in MongoDB data lake, by writing functions for fetching channel, video and comments data and connect to MongoDB server. 
**>**Then, migrate it to  the SQL Data warehouse, write a query for retrieving the data and do the analysis.
**>**Finally, Display the data in the streamlit app.

--------------------------------------------------------------------------------------------------

**Displaying the working Model**

**Enter the Channel ID and Video ID**
![image](https://github.com/kamalavarshini15/GUVI-Capstone-Project-1/assets/119718578/90f81b80-07b3-4a2c-bb59-e107d5957795)
![image](https://github.com/kamalavarshini15/GUVI-Capstone-Project-1/assets/119718578/83d37074-14b0-436c-8cde-8a49015a0938)

**Channel data, videos data and comments data displayed in the streamlit app**
![image](https://github.com/kamalavarshini15/GUVI-Capstone-Project-1/assets/119718578/cdab081e-2999-408e-bc4a-b0b30ee59d05)
![image](https://github.com/kamalavarshini15/GUVI-Capstone-Project-1/assets/119718578/55de2812-9ec4-4ea1-bc27-c4037f98e263)

**Now data stored in MongoDB is migrated to SQL by clicking the "Store data in SQL DB" button**
![image](https://github.com/kamalavarshini15/GUVI-Capstone-Project-1/assets/119718578/fdcd4a13-d5aa-4875-8d10-5afbae272f60)

**Now finally the analysis can be performed for the given 10 questions:**

**SQL Query Output need to displayed as table in Streamlit Application:**
1. What are the names of all the videos and their corresponding channels?
2. Which channels have the most number of videos, and how many videos do
they have?
3. What are the top 10 most viewed videos and their respective channels?
4. How many comments were made on each video, and what are their
corresponding video names?
5. Which videos have the highest number of likes, and what are their
corresponding channel names?
6. What is the total number of likes and dislikes for each video, and what are
their corresponding video names?
7. What is the total number of views for each channel, and what are their
corresponding channel names?
8. What are the names of all the channels that have published videos in the year
2022?
9. What is the average duration of all videos in each channel, and what are their
corresponding channel names?
10.Which videos have the highest number of comments, and what are their
corresponding channel names?

![image](https://github.com/kamalavarshini15/GUVI-Capstone-Project-1/assets/119718578/69fdda24-11b4-4600-b7b2-2645972abcd7)
![image](https://github.com/kamalavarshini15/GUVI-Capstone-Project-1/assets/119718578/eb133923-5110-4730-9a80-715da5a3d86a)

------------------------------------------------------------------------------------------------------------------------------------------------------------------

**Now let's see how Data is stored inside Mongo DB**
![image](https://github.com/kamalavarshini15/GUVI-Capstone-Project-1/assets/119718578/0256d4c4-98e5-4085-a1f8-c4c428b9a92f)

------------------------------------------------------------------------------------------------------------------------------------------------------------------

**Now let's see how Data is stored inside SQL Database**
![image](https://github.com/kamalavarshini15/GUVI-Capstone-Project-1/assets/119718578/83881e4a-f653-4d25-9346-4e024123e2e9)

------------------------------------------------------**PROJECT-END**---------------------------------------------------------------------------------------------





















