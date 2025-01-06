import requests
def get_supported_languages():
    url = "https://google-translate113.p.rapidapi.com/api/v1/translator/support-languages"
    headers = {
        "x-rapidapi-key": "f4ef073a94msh87fdfe933cbf12bp1eca3cjsn4272baa9dfcd",  # Replace with your API key
        "x-rapidapi-host": "google-translate113.p.rapidapi.com",
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        print("Supported Languages:")
        print(response.json())  # Prints the response in JSON format
    else:
        print(f"Error: {response.status_code}")
        print(response.text)


def translate_text(text, target_language="en"):
    url = "https://google-translate113.p.rapidapi.com/api/v1/translator/text"
    payload = {"from": "auto", "to": target_language, "text": text}
    headers = {
        "x-rapidapi-key": "f4ef073a94msh87fdfe933cbf12bp1eca3cjsn4272baa9dfcd",
        "x-rapidapi-host": "google-translate113.p.rapidapi.com",
        "Content-Type": "application/json",
    }
    response = requests.post(url, json=payload, headers=headers)
    
    # Print response details for debugging
    print(f"Status Code: {response.status_code}")
    print(f"Response Text: {response.text}")
    
    if response.status_code == 200:
        return response.json().get("translated_text", "Translation not available")
    else:
        return f"Error: {response.status_code}, {response.text}"

# Example Usage
# Example Usage

get_supported_languages()
print(translate_text("Bonjour, comment ça va?", "en"))
