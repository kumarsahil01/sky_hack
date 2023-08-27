import csv
import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF
import nltk
import string
print(nltk.__version__)
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('vader_lexicon')
from collections import Counter
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.sentiment import SentimentIntensityAnalyzer
# MySQL database connection parameters
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Sahil@123',
    'database': 'sky_hack'
}

def analyze_sentiments(data_frame):
    # Create a SentimentIntensityAnalyzer instance
    sia = SentimentIntensityAnalyzer()

    # Create lists to hold the count of comments with different sentiments
    positive_count = 0
    negative_count = 0
    neutral_count = 0

    # Define lists of keywords for positive, negative, and neutral sentiments
    positive_keywords = ["good", "great", "excellent", "delicious", "satisfying"]
    negative_keywords = ["bad", "poor", "disappointing", "unpleasant", "worst"]
    neutral_keywords = ["okay", "average", "ok", "adequate", "normal"]

    # Loop through each comment and analyze its sentiment
    for comment in data_frame['verbatim_text']:
        sentiment_score = sia.polarity_scores(comment)['compound']
        
        if any(keyword in comment.lower() for keyword in positive_keywords) or sentiment_score > 0.2:
            positive_count += 1
        elif any(keyword in comment.lower() for keyword in negative_keywords) or sentiment_score < -0.2:
            negative_count += 1
        elif any(keyword in comment.lower() for keyword in neutral_keywords):
            neutral_count += 1

    # Calculate total count of comments
    total_count = positive_count + negative_count + neutral_count
    
    # Calculate percentages
    positive_percent = (positive_count / total_count) * 100
    negative_percent = (negative_count / total_count) * 100
    neutral_percent = (neutral_count / total_count) * 100

    # Create a pie chart
    labels = ['Positive', 'Negative', 'Neutral']
    sizes = [positive_percent, negative_percent, neutral_percent]
    colors = ['green', 'red', 'gray']
    explode = (0.1, 0, 0)  # explode the 1st slice (Positive)

    plt.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%', shadow=True, startangle=140)
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.title('Sentiment Distribution of Comments')
    plt.show()


def plot_most_common_complaint_words(data_frame, num_words=10):
    # Combine all verbatim_text comments into a single string
  
    all_comments = ' '.join(data_frame['verbatim_text'])
   
    if all_comments:
       # Tokenize the text into words
        words = word_tokenize(all_comments)

        # Filter out stopwords and punctuation
        stop_words = set(stopwords.words('english'))
        filtered_words = [word for word in words if word.lower() not in stop_words and word not in string.punctuation]

        # Count the frequency of each word
        word_counts = Counter(filtered_words)

        # Get the most common complaint words
        most_common_words = word_counts.most_common(num_words)

        # Prepare data for plotting
        words, counts = zip(*most_common_words)

        # Create a bar graph to visualize the most common complaint words
        plt.bar(words, counts)
        plt.xlabel('Complaint Words')
        plt.ylabel('Frequency')
        plt.title(f'Top {num_words} Most Common Complaint Words')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
    else:
        print("No comments found")  


def plot_bad_words(data_frame):
    # Combine all verbatim_text comments into a single string
    all_comments = ' '.join(data_frame['verbatim_text'])

    if all_comments:
        # Tokenize the text into words
        words = word_tokenize(all_comments)

        # Filter out stopwords and punctuation
        stop_words = set(stopwords.words('english'))
        filtered_words = [word for word in words if word.lower() not in stop_words and word not in string.punctuation]

        # Count the frequency of each word
        word_counts = Counter(filtered_words)

        # Get the list of bad words (you can customize this list)
        bad_words = ["bad", "poor", "disappointing", "unpleasant", "worst"]

        # Prepare data for plotting
        bad_word_counts = [word_counts[word] for word in bad_words]

        # Create a bar graph to visualize the bad words and their frequency
        plt.bar(bad_words, bad_word_counts)
        plt.xlabel('Bad Words')
        plt.ylabel('Frequency')
        plt.title('Bad Words in Complaints')
        plt.xticks(rotation=45)
        for i, count in enumerate(bad_word_counts):
            plt.text(i, count + 1, str(count), ha='center', va='bottom')
        plt.tight_layout()
        plt.show()
    else:
        print("No comments found")
 
try:
    # Establish a connection to the database
    connection = mysql.connector.connect(**db_config)
    print("Connection created successfully")

    # Create a cursor
    cursor = connection.cursor()

    flight_number = '23'
    query = f"""
        SELECT verbatim_text
        FROM CustomerCommentsFeedback
        ;
    """

    # Execute the query
    cursor.execute(query)

    # Fetch all the rows
    rows = cursor.fetchall()

    # Process and print the results
    # for row in rows:
    #    print(row)
    columns = [
    'verbatim_text'
    ]
    # Create DataFrame
    df_comments = pd.DataFrame(rows, columns=columns)
    analyze_sentiments(df_comments)
    plot_most_common_complaint_words(df_comments, num_words=10)
    plot_bad_words(df_comments)
    
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Access denied error")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(f"Error: {err}")
finally:
    # Close the cursor and connection
    if 'cursor' in locals() and cursor is not None:
        cursor.close()
    if 'connection' in locals() and connection.is_connected():
        connection.close()
        print("Connection closed")
            