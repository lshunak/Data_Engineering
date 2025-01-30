from flask import Flask, render_template, request
import requests
from datetime import datetime
import time
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

API_KEY = os.getenv('API_KEY') # Replace with your API key

#Data Base Connection

def get_db_connection():
    retries = 5
    while retries > 0:
        try:
            conn = mysql.connector.connect(
                host=os.getenv('DB_HOST'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                database=os.getenv('DB_NAME')
            )
            if conn.is_connected():
                print('Connected to MySQL database')
                return conn
        except Error as e:
            print(f"Error connecting to database: {e}")
            retries -= 1
            time.sleep(5)
    raise Exception("Database connection failed after multiple retries")

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS search_history (
            id INT AUTO_INCREMENT PRIMARY KEY,
            location VARCHAR(255) NOT NULL,
            country VARCHAR(255) NOT NULL,
            date_searched TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()

init_db()



def get_weather_data(location):

      # Get weather data using 5-day/3-hour forecast API
    weather_url = f'https://api.openweathermap.org/data/2.5/forecast?q={location}&units=metric&appid={API_KEY}'
    response = requests.get(weather_url)

    if response.status_code == 200:
        weather_data = response.json()
        print(weather_data)  # Debugging: Print the entire weather data

        city_name = weather_data['city']['name']
        country_name = weather_data['city']['country']
        
        # Return necessary weather data
        return {
            'location': city_name,
            'country': country_name,
            'forecast': weather_data['list']
        }
    else:
        print("Error fetching weather data:", response.status_code)
        return None
   
       
 

def save_search_history(location, country):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO search_history (location, country) 
        VALUES (%s, %s)
    """, (location, country))
    conn.commit()
    cursor.close()
    conn.close()


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        location = request.form['location']
        data = get_weather_data(location)
        
        if not data:
            return render_template('index.html', error="Location not found.")
        
        save_search_history(data['location'], data['country'])
        return render_template('index.html', 
                             location=data['location'],
                             country=data['country'],
                             forecast=data['forecast'],
                             datetime=datetime)  # Pass datetime to template

    return render_template('index.html')

if __name__ == '__main__':
    print("API Key:", API_KEY)
    port = int(os.environ.get('PORT', 9000))
    app.run(debug=True, host='0.0.0.0', port=port)
