from requests_html import HTMLSession
import pandas as pd
import time
import re, string, random
import csv, sqlite3
from bs4 import BeautifulSoup
import time, re, urllib
# Scan the webpage
def scanpage(url: str) -> tuple:
    html = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(html, 'html.parser')
    pageurls = []
    links = []
    relativetime = []
    for k in soup.find_all('div', class_='flush-left'):
        url_num = 0  
        for ind in k.find_all('span'):    
            if ind.get('title') is not None and ind.get('title').startswith('2'):
                relativetime.append(ind.get('title'))
                url_num += 1
        for link in k.find_all('a', class_ = 's-link'):
            links.append('https://stackoverflow.com' + link.get('href'))
        links = links[:url_num] 
    return links, relativetime
"""
other options: 
https://stackoverflow.com/questions
https://stackoverflow.com/questions?tab=Newest
https://stackoverflow.com/questions?tab=Active
https://stackoverflow.com/questions?tab=Bounties
https://stackoverflow.com/questions?tab=Unanswered
https://stackoverflow.com/questions?tab=Frequent
https://stackoverflow.com/questions?tab=Votes
https://stackoverflow.com/search?tab=newest&q=bing
"""
url = 'https://stackoverflow.com/search?tab=newest&q=bing' 
output = scanpage(url = url) 

# 3) merge all data as dataframe
df = pd.DataFrame(columns=['question_link', 'creation_timestamp', 'post_key'])  
df['question_link'] = output[0]
df['creation_timestamp'] = output[1]
timestamp = [time.strftime("%Y%m%dT%H%M%S", time.localtime())] * len(output[0])
# generate primary key
ind = ["".join(random.choices(string.ascii_letters, k = 8)) for i in range(len(output[0]))]
while len(list(set(ind))) != len(ind):
    ind = ["".join(random.choices(string.ascii_letters, k = 8)) for i in range(len(output[0]))]
post_key = []
for i in range(len(output[0])):
    post_key.append(timestamp[i][:8] + ind[i])
df['post_key'] = post_key
### Import df to sql

conn= sqlite3.connect("/Users/mikexie/pipeline/stackoverflow_questions.db")

df.to_sql("question_list", conn, if_exists='append', index=False)
conn.close()