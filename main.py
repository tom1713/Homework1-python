# python version: 3.12.9

import requests
import warnings
from bs4 import BeautifulSoup
import json
import sqlite3
from datetime import datetime
import time
from apscheduler.schedulers.blocking import BlockingScheduler

def format_datetime(date):
    try:
        dt = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        print (f"Invalid date format: {e}")
        return None

def crawler(url):
    articles = []
    warnings.filterwarnings('ignore')
    se = requests.Session()
    se.verify = False

    res = se.get(url)
    data = parser_html(res)
    
    for curations in data['pageData']['curations']:
        if 'summaries' in curations:
            for summary in curations['summaries']:
                articles.append({
                    'type': summary['type'],
                    'title': summary['title'],
                    'description': summary['description'],
                    'link': summary['link'],
                    'firstPublished': format_datetime(summary['firstPublished']),
                    'lastPublished': format_datetime(summary['lastPublished']),
                })
    return articles

def parser_html(res):
    soup = BeautifulSoup(res.text, "html.parser")
    script_tag = soup.find("script", string=lambda text: text and "window.SIMORGH_DATA" in text)
    if script_tag:
        text_json = script_tag.string.strip().replace("window.SIMORGH_DATA=", "", 1)
        data = json.loads(text_json)
    return data

class DatabaseSqlite:
    def __init__(self):
        self.db = 'BBC_news.db'
    
    def connect(self):
        try:
            return sqlite3.connect(self.db)
        except sqlite3.Error as e:
            print(f"Connection failed: {e}")
            return None
        
    
    def create_table(self):
        conn = self.connect()
        if conn:
            c = conn.cursor()
            try:
                c.execute('''
                    CREATE TABLE IF NOT EXISTS articles (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        type TEXT,
                        title TEXT,
                        description TEXT,
                        link TEXT UNIQUE,
                        firstPublished DATETIME,
                        lastPublished DATETIME
                    )
                ''')
                conn.commit()
            except sqlite3.Error as e:
                print(f"Create table failed:{e}")
        conn.close()

    def insert(self, article):
        conn = self.connect()
        if conn:
            try:
                c = conn.cursor()
                c.execute('''
                    SELECT COUNT(*) FROM articles WHERE link = ?''', (article['link'],))
                count = c.fetchone()[0]
                if count == 0:
                    c.execute('''
                        INSERT INTO articles (type, title, description, link, firstPublished, lastPublished)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (article['type'], article['title'], article['description'], article['link'], article['firstPublished'], article['lastPublished']))
                    conn.commit()
                else:
                    print(f"{article['title']} already exists.")
            except sqlite3.Error as e:
                print(f"Insert failed:{e}")
        conn.close()
    
    def fetch_all(self):
        conn = self.connect()
        if conn:
            try:
                c = conn.cursor()
                c.execute('SELECT * FROM articles')
                return c.fetchall()
            except sqlite3.Error as e:
                print(f"Fetch failed:{e}")
        conn.close()
        return None

def schedule_task():
    db = DatabaseSqlite()
    news = crawler('https://www.bbc.com/zhongwen/topics/c83plve5vmjt/trad')
    for new in news:
        db.insert(new)
    articles = db.fetch_all()
    print (articles)

if __name__ == '__main__':
    db = DatabaseSqlite()
    db.create_table()

    scheduler = BlockingScheduler(timezone="Asia/Shanghai")
    scheduler.add_job(schedule_task, 'cron', day_of_week='0-6', hour=18)
    scheduler.start()