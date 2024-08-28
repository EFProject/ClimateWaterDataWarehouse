import json
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

def connection_handler():
    try:
        with open('postgres_credentials.json', 'r') as f:
            postgres_credentials = json.load(f)
            connection_string = f"postgresql://{postgres_credentials.get('user')}:{postgres_credentials.get('password')}@{postgres_credentials.get('host')}:{postgres_credentials.get('port')}/{postgres_credentials.get('dbname')}"

            engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
            connection = engine.connect()
            connection.execute(text("SELECT 1;"))
            print("Connection to Postgresql successful!")

            return connection
    except FileNotFoundError:
        print("File postgres_credentials.json not found.")
        return None
    except json.JSONDecodeError:
        print("Error decoding JSON in credentials")
        return None
    except OperationalError:
        print(f"Authentication failed for user {postgres_credentials.get('user')} at {postgres_credentials.get('host')}")
        return None