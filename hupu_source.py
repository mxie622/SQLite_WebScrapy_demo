# ******* Make RDB for hupu
"""
Input -> https://bbs.hupu.com/all-gambia
Ouput -> Append sqlite3 hupu.db: 
         subject, subject_topic, liang_cnt, comment_cnt, timestamp
"""

from requests_html import HTMLSession
import pandas as pd
import time
import re, string, random
import csv, sqlite3
session = HTMLSession()
url = "https://bbs.hupu.com/all-gambia" # 爬取路径
r = session.get(url)
# 获取页面对应的css方法：1）F12 / inspector 2) 找到要截取的页面的部分 3）右键copy->copy Selector
selector = '#container > div > div.bbs-index-web-holder > div > div.bbs-index-web-middle > div > div.text-list-model'
# 1) Obtain subject + subject_topic
def get_text_link_from_sel_hupu(sel:str, url:str):
    """
    sel = selector
    url = url to be fetched
    """
    subject_topic = []
    subject = []
    r = session.get(url)
    results = r.html.find(sel)
    mylist = []
    try: 
        file = results[0].text
        for i in range(len(file.split('\n'))): 
            if i % 2 != 0:
                subject_topic.append(file.split('\n')[i])
            else:
                subject.append(file.split('\n')[i])
        mylist = [subject, subject_topic]
        return mylist   
    except:    
        return None

# 2) Obtain liang + comment from subject's strings
output = get_text_link_from_sel_hupu(selector, url)

# 3) merge all data as dataframe
df = pd.DataFrame(columns=['subject', 'subject_topic', 'timestamp', 'post_key'])  
df['subject'] = output[0]
df['subject_topic'] = output[1]
timestamp = [time.strftime("%Y%m%dT%H%M%S", time.localtime())] * len(output[0])
df['timestamp'] = timestamp
# generate primary key
ind = ["".join(random.choices(string.ascii_letters, k = 8)) for i in range(len(output[0]))]
while len(list(set(ind))) != len(ind):
    ind = ["".join(random.choices(string.ascii_letters, k = 8)) for i in range(len(output[0]))]
post_key = []
for i in range(len(output[0])):
    post_key.append(timestamp[i][:8] + ind[i])
df['post_key'] = post_key
### Import df to sql

conn= sqlite3.connect("/Users/mikexie/pipeline/hupu.db")

df.to_sql("hupu_buxingjie", conn, if_exists='append', index=False)
conn.close()