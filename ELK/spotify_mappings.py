"""
Defines the Elasticsearch mappings for the Spotify songs dataset
"""

def get_spotify_mappings():
    """
    Returns Elasticsearch mapping definition for Spotify songs
    """
    mappings = {
        "mappings": {
            "properties": {
                "id": {"type": "integer"},
                "track_id": {"type": "keyword"},  # Using keyword for exact matches
                "artists": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},  # Both full-text and keyword
                "album_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "track_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "popularity": {"type": "integer"},
                "duration_ms": {"type": "integer"},
                "explicit": {"type": "boolean"},
                "danceability": {"type": "float"},
                "energy": {"type": "float"},
                "key": {"type": "integer"},
                "loudness": {"type": "float"},
                "mode": {"type": "integer"},
                "speechiness": {"type": "float"},
                "acousticness": {"type": "float"},
                "instrumentalness": {"type": "float"},
                "liveness": {"type": "float"},
                "valence": {"type": "float"},
                "tempo": {"type": "float"},
                "time_signature": {"type": "integer"},
                "track_genre": {"type": "keyword"}  # Using keyword for exact matches and aggregations
            }
        },
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }
    return mappings