import psycopg2
import json
import os
from datetime import datetime, timedelta, timezone
import asyncio
from discord_manager import send_message as discord_send_message

# Path to the statistics files
STATS_FILE = 'table_statistics.json'
LOG_FILE = 'statistics_log.json'
MANAGER_USER = os.environ['MANAGER_USER']
MANAGER_PASS = os.environ['MANAGER_PASS']


def load_previous_statistics():
    if os.path.exists(STATS_FILE):
        with open(STATS_FILE, 'r') as file:
            return json.load(file)
    return {}


def save_statistics(stats):
    with open(STATS_FILE, 'w') as file:
        json.dump(stats, file, indent=4)


def log_statistics(current_time, stats):
    log_data = {}
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as file:
            log_data = json.load(file)
    log_data[current_time] = stats
    with open(LOG_FILE, 'w') as file:
        json.dump(log_data, file, indent=4)


def get_table_row_counts(connection):
    cursor = connection.cursor()

    # Fetch all table names and their schemas from the current database, excluding pg_catalog and information_schema
    cursor.execute("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_type = 'BASE TABLE'
        AND table_schema NOT IN ('pg_catalog', 'information_schema');
    """)

    tables = cursor.fetchall()
    new_stats = {}

    print("Row count for each table:")
    for schema, table_name in tables:
        # Construct the table name with schema
        qualified_table_name = f"{schema}.{table_name}"
        cursor.execute(f"SELECT COUNT(*) FROM {qualified_table_name};")
        row_count = cursor.fetchone()[0]
        new_stats[f"{schema}.{table_name}"] = row_count
        info = f"Schema: {schema}, Table: {table_name}, Row Count: {row_count}"
        # print(info)
        print(info)

    cursor.close()
    return new_stats


def format_statistics_for_discord(prev_stats, new_stats):
    # Get current server time and adjust by adding 2 hours
    server_time = datetime.now(timezone.utc) + timedelta(hours=2)
    server_time_str = server_time.strftime('%Y-%m-%d %H:%M:%S')

    message = f"-- {server_time_str} --\n"

    for table, new_count in new_stats.items():
        prev_count = prev_stats.get(table, 0)
        if new_count > prev_count:
            emoji = "🟢"
            change = f"+ {new_count - prev_count}"
        elif new_count < prev_count:
            emoji = "🟡"
            change = f"- {prev_count - new_count}"
        elif new_count == 0:
            emoji = "🔴"
            change = ""
        else:
            emoji = "⚪"
            change = ""

        message += f"Schema: {table.split('.')[0]}, Table: {table.split('.')[1]}, Row Count: {new_count} {emoji} {change}\n"

    return message


async def send_discord_message(message: str):
    await discord_send_message(message)


async def main():
    # Database connection parameters
    connection_params = {
        'dbname': 'NovaLend',
        'user': MANAGER_USER,
        'password': MANAGER_PASS,
        'host': 'localhost',  # e.g., 'localhost' or an IP address
        'port': '5432'  # Default PostgreSQL port
    }

    # Load previous statistics
    prev_stats = load_previous_statistics()

    # Get current time for logging
    current_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    # Establish a connection to the PostgreSQL database
    try:
        connection = psycopg2.connect(**connection_params)
        new_stats = get_table_row_counts(connection)
        save_statistics(new_stats)

        # Log the current statistics
        log_statistics(current_time, new_stats)

        # Format and send the statistics to Discord
        message = format_statistics_for_discord(prev_stats, new_stats)
        await send_discord_message(message)

    except Exception as error:
        print(f"Error: {error}")
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    asyncio.run(main())
