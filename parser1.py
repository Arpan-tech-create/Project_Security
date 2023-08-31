import psycopg2
import re
import db  
from tag_generators import tag_gen




def extract_log_info(log_line):
    pattern = r'^(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] "\w+ (.*?) HTTP\/\d\.\d" (\d+) (\d+) "(.*?)" "(.*?)"$'
    match = re.match(pattern, log_line)
    if match:
        ip = match.group(1)
        timestamp = match.group(2)
        url = match.group(3)
        response = int(match.group(4))
        size = int(match.group(5))
        referer = match.group(6)
        client = match.group(7)

        return ip, timestamp, url, response, size, referer, client
    else:
        return None

def process_file(log_file_path):
    with open(log_file_path, 'r') as f:
        conn = psycopg2.connect(
            dbname='vedas',
            user='postgres',
            password='sac123',
            host='localhost',
            port='5432'
        )

        cursor = conn.cursor()

        for line in f:
            log_info = extract_log_info(line)
            if log_info:
                db.insert_log_entry(
                    conn, *log_info
                )
        conn.commit()
        generated_tags = []
        for tag_generator in tag_gen:
            generated_tags.extend(tag_generator(log_info))
        db.insert_tags(conn, generated_tags)
        cursor.close()
        conn.close()




if __name__ == '__main__':
    log_file_path = 'access.log.2.log'
    process_file(log_file_path)
