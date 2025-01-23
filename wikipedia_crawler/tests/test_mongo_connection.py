from pymongo import MongoClient

def test_connection():
    client = MongoClient('mongodb://root:example@localhost:27017')
    try:
        # Test connection
        client.admin.command('ping')
        print("MongoDB connection successful!")
        
        # List databases
        print("Databases:", client.list_database_names())
        
    except Exception as e:
        print(f"Connection failed: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    test_connection()