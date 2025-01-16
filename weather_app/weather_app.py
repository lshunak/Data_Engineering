from flask import Flask, render_template, request
import requests
from datetime import datetime

app = Flask(__name__)

API_KEY = '03f4dac72542608ad7cf1c7fe47e3108'  # Replace with your API key

def get_weather_data(location):
    # Get coordinates
    geo_url = f'http://api.openweathermap.org/geo/1.0/direct?q={location}&limit=1&appid={API_KEY}'
    geo_response = requests.get(geo_url)
    geo_data = geo_response.json()
    
    if not geo_data:
        return None
        
    lat = geo_data[0]['lat']
    lon = geo_data[0]['lon']
    
    # Get weather data using 5-day/3-hour forecast API
    weather_url = f'https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&units=metric&appid={API_KEY}'
    response = requests.get(weather_url)
    weather_data = response.json()
    
    if 'list' not in weather_data:
        return None

    # Process data to get daily forecasts
    daily_data = []
    seen_dates = set()
    
    for item in weather_data['list']:
        date = datetime.fromtimestamp(item['dt']).date()
        if date not in seen_dates:
            seen_dates.add(date)
            daily_data.append({
                'dt': item['dt'],
                'temp': {
                    'day': item['main']['temp'],
                    'night': item['main']['temp_min']
                },
                'humidity': item['main']['humidity']
            })
    
    return {
        'location': geo_data[0].get('name', location),
        'country': geo_data[0].get('country', ''),
        'list': daily_data[:7]  # Limit to 7 days
    }

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        location = request.form['location']
        data = get_weather_data(location)
        
        if not data:
            return render_template('index.html', error="Location not found.")
        
        return render_template('index.html', 
                             location=data['location'],
                             country=data['country'],
                             forecast=data['list'],
                             datetime=datetime)  # Pass datetime to template
    
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True, port=5000)
