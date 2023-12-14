# Import necessary libraries
from flask import Flask, render_template
from kafka import KafkaConsumer, KafkaProducer
import json
import requests
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from io import BytesIO
import base64
import threading
import time

# Create Flask app
app = Flask(__name__)

# Kafka bootstrap servers
bootstrap_servers = "localhost:9092"

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Create Kafka consumer
consumer = KafkaConsumer("weathertopic", bootstrap_servers=bootstrap_servers, group_id="my_consumer_group")

# Function to create a Kafka topic
def create_topic(topic_name):
    producer.send(topic_name, b"Creating topic")
    producer.flush()

    print(f"Topic {topic_name} created successfully")

# OpenWeather API key
api_key = "ee323b4d86b54775f0ab5f64c9df788f"

# List of cities
cities = ["Mumbai", "London", "Ahmedabad", "Kolkata", "Chandigarh"]

# Function to get weather data from OpenWeather API
def get_weather(api_key, city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
    response = requests.get(url).json()
    return response

# Function to send data to Kafka topic
def send_to_kafka(topic, data):
    producer.send(topic, json.dumps(data).encode('utf-8'))
    producer.flush()

# Function to consume data from Kafka for all cities
def consume_from_kafka():
    data_list = []
    messages_to_consume = len(cities)

    try:
        for i, city in enumerate(cities):
            weather_data = get_weather(api_key, city)

            # Ensure the expected data structure is present
            if 'name' not in weather_data or 'main' not in weather_data or 'temp' not in weather_data['main'] or 'humidity' not in weather_data['main']:
                print(f"Unexpected data structure for {city}. Skipping.")
                continue
            
            # Extract relevant information
            city_name = cities[i]
            temperature_kelvin = weather_data['main']['temp']
            temperature_celsius = temperature_kelvin - 273.15  # Convert Kelvin to Celsius
            humidity = weather_data['main']['humidity']

            # Store information in the data_list
            data_list.append({'city_name': city_name, 'temperature': temperature_celsius, 'humidity': humidity})

            # Send to Kafka
            send_to_kafka("weathertopic", weather_data)

    except Exception as e:
        print(f"Unexpected error: {e}")

    finally:
        # Close the consumer to release resources
        consumer.close()

    # Convert the data list to a DataFrame
    df = pd.DataFrame(data_list)
    return df

# Function to generate temperature and humidity visualizations
def generate_visualizations(df):
    plt.switch_backend('agg')
    plt.figure(figsize=(12, 6))

    # Temperature Visualization
    plt.subplot(1, 2, 1)
    ax1 = df.plot(kind='bar', x='city_name', y='temperature', color='skyblue')
    plt.title('Temperature Visualization')
    plt.xlabel('City')
    plt.ylabel('Temperature (°C)')
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Annotate with temperature values on top of each bar
    for p in ax1.patches:
        ax1.annotate(f'{p.get_height():.2f}', (p.get_x() + p.get_width() / 2., p.get_height()),
                     ha='center', va='center', xytext=(0, 10), textcoords='offset points')

    temperature_plot = BytesIO()
    plt.savefig(temperature_plot, format='png')
    temperature_plot.seek(0)
    temperature_img = base64.b64encode(temperature_plot.getvalue()).decode('utf-8')
    plt.clf()

    # Humidity Visualization
    plt.subplot(1, 2, 2)
    ax2 = df.plot(kind='bar', x='city_name', y='humidity', color='lightgreen')
    plt.title('Humidity Visualization')
    plt.xlabel('City')
    plt.ylabel('Humidity (%)')
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Annotate with humidity values on top of each bar
    for p in ax2.patches:
        ax2.annotate(f'{p.get_height()}%', (p.get_x() + p.get_width() / 2., p.get_height()),
                     ha='center', va='center', xytext=(0, 10), textcoords='offset points')

    humidity_plot = BytesIO()
    plt.savefig(humidity_plot, format='png')
    humidity_plot.seek(0)
    humidity_img = base64.b64encode(humidity_plot.getvalue()).decode('utf-8')

    return temperature_img, humidity_img

# Function to continuously update the DataFrame and generate visualizations
def update_dataframe_and_visualizations(interval_seconds):
    while True:
        # Consume data from Kafka and send to Kafka for each city
        df = consume_from_kafka()

        # Generate visualizations
        temperature_img, humidity_img = generate_visualizations(df)

        # Store visualizations in global variables
        app.temperature_img = temperature_img
        app.humidity_img = humidity_img

        time.sleep(interval_seconds)

# Flask route to render visualizations
@app.route('/')
def render_visualizations():
    return render_template('visualizations.html', temperature_img=app.temperature_img, humidity_img=app.humidity_img)

if __name__ == "__main__":
    # Example usage
    create_topic("weathertopic")

    # Set the update interval (e.g., 60 seconds)
    update_interval_seconds = 60

    # Start continuous updates and visualization generation in a separate thread
    update_thread = threading.Thread(target=update_dataframe_and_visualizations, args=(update_interval_seconds,))
    update_thread.start()

    # Run Flask app
    app.run(debug=True)


