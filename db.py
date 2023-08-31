import psycopg2

def get_log_entry(conn, id):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT ip, timestamp, url, response, size, referer, client
        FROM access_logs
        WHERE id = %s
    ''', (id,))
    
    row = cursor.fetchone()
    if row:
        columns = ('ip', 'timestamp', 'url', 'response', 'size', 'referer', 'client')
        log_entry = dict(zip(columns, row))
        return log_entry
    else:
        return None

def insert_log_entry(conn, ip, timestamp, url, response, size, referer, client):
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO access_logs (ip, timestamp, url, response, size, referer, client)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    ''', (
        ip, timestamp, url, response, size, referer, client
    ))
    conn.commit()
    inserted_id = cursor.fetchone()[0]
    return inserted_id  #primary-key



def insert_tags(conn, tags):
    cursor = conn.cursor()
    insert_tags_query = '''
        INSERT INTO tags(req_id, tag_type, value)
        VALUES (%s, %s, %s)
    '''
    tag_data = [(tag['req_id'], tag['tag_type'], tag['value']) for tag in tags]
    cursor.executemany(insert_tags_query, tag_data)
    conn.commit()
    cursor.close()



if __name__ == '__main__':
    conn = psycopg2.connect(
        dbname='vedas',
        user='postgres',
        password='sac123',
        host='localhost',
        port='5432'
    )
    
    while True:
        try:
            action = input("Enter 'r' to retrieve, 'i' to insert, or 'exit' to quit: ")
            if action.lower() == 'exit':
                break
            
            if action.lower() == 'r':
                log_id = int(input("Enter log entry ID: "))
                log_entry = get_log_entry(conn, log_id)
                if log_entry:
                    print("Retrieved Data:")
                    for key, value in log_entry.items():
                        print(f"{key}: {value}")
                else:
                    print("Log entry not found.")
            
            elif action.lower() == 'i':
                ip = input("Enter IP: ")
                timestamp = input("Enter Timestamp: ")
                url = input("Enter URL: ")
                response = int(input("Enter Response: "))
                size = int(input("Enter Size: "))
                referer = input("Enter Referer: ")
                client = input("Enter Client: ")
                
                inserted_id = insert_log_entry(conn, ip, timestamp, url, response, size, referer, client)
                print(f"Inserted new log entry with ID: {inserted_id}")
            
            else:
                print("Invalid action. Please enter 'r', 'i', or 'exit'.")
        except ValueError:
            print("Invalid input. Please enter valid values.")
    
    conn.close()
