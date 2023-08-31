import psycopg2

def connect_to_database():
    conn = psycopg2.connect(
        dbname='vedas',
        user='postgres',
        password='sac123',
        host='localhost',
        port='5432'
    )
    return conn

def generate_tags1(log_info):
    conn = connect_to_database()
    cur = conn.cursor()

    query = """
        SELECT
            id AS req_id,
            'params' AS tag_type,
            CAST(SUBSTRING(url FROM 'params=(\d+)') AS INTEGER) AS value
        FROM access_logs
        WHERE url LIKE '/geoentity-services%%params/values%%';
    """

    cur.execute(query)

    rows = cur.fetchall()
    print(rows)
    cur.close()
    conn.close()

    tags = [{"req_id": log_info[0], "tag_type": "params", "value": log_info[1]}]
    print(tags)

    return tags

def generate_tags2(log_info):
    # This is a placeholder for your custom logic
    tags2 = [{"req_id": log_info[0], "tag_type": "custom", "value": log_info[1]}]

    return tags2

tag_generators = [generate_tags1, generate_tags2]
