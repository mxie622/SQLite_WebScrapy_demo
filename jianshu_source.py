
# ******* Make RDB for jianshu
"""
Input -> https://www.jianshu.com/
Ouput -> Append sqlite3 jianshu.db: 
         subject, subject_content, views, author, comment_cnt, like_cnt, reward_cnt, timestamp, post_key
"""
from requests_html import HTMLSession
import pandas as pd
import time
import re, random, string
import csv, sqlite3
session = HTMLSession()
url = "https://www.jianshu.com/" # 爬取路径
r = session.get(url)

# 获取页面对应的css方法：1）F12 / inspector 2) 找到要截取的页面的部分 3）右键copy->copy Selector
selector = '#list-container'

# 1) Obtain subject + subject_topic
def get_text_link_from_sel_jianshu(sel:str, url:str):
    """
    sel = selector
    url = url to be fetched
    """
    subject = []
    short_content = []
    view_author_comment_like_reward = []
    r = session.get(url)
    results = r.html.find(sel)
    mylist = []
    try: 
        file = results[0].text
        for i in range(len(file.split('\n'))): 
            if i % 3 == 0:
                subject.append(file.split('\n')[i])
            elif i % 3 == 1:
                short_content.append(file.split('\n')[i])
            else:
                view_author_comment_like_reward.append(file.split('\n')[i])
        mylist = [subject, short_content, view_author_comment_like_reward]
        return mylist   
    except:    
        return None
output = get_text_link_from_sel_jianshu(selector, url)

# 2) Obtain views, author, comment_cnt, like_cnt, reward_cnt = [] from view_author_comment_like_reward's strings
def view_author_comment_like_reward(s):
    """
    s = get_text_link_from_sel_jianshu(selector, url)
    """
    views = []
    author = []
    comment_cnt = []
    like_cnt = []
    reward_cnt = []
    for i in s[2]:
        if len(i.split()) == 1:
            reward_cnt.append(0)     
            views.append(0)
            author.append("others")
            comment_cnt.append(0)
            like_cnt.append(0)
        elif len(i.split()) == 4:
            reward_cnt.append(0)     
            views.append(i.split()[0])
            author.append(i.split()[1])
            comment_cnt.append(i.split()[2])
            like_cnt.append(i.split()[3])
        elif len(i.split()) == 5:
            reward_cnt.append(i.split()[4])
            views.append(i.split()[0])
            author.append(i.split()[1])
            comment_cnt.append(i.split()[2])
            like_cnt.append(i.split()[3])
        else:
            reward_cnt.append(0)     
            views.append(0)
            author.append("/")
            comment_cnt.append(0)
            like_cnt.append(0)
        
    return views, author, comment_cnt, like_cnt, reward_cnt

# 3) get input all data
s = get_text_link_from_sel_jianshu(selector, url)
views = []
author = []
comment_cnt = []
like_cnt = []
reward_cnt = []

views, author, comment_cnt, like_cnt, reward_cnt = view_author_comment_like_reward(s) 

# 4) merge all data as dataframe
df = pd.DataFrame(columns=['subject', 'subject_content', 'views', 'author', 'comment_cnt', 'like_cnt', 'reward_cnt', 'timestamp', 'post_key'])  
df['subject'] = s[0]
df['subject_content'] = s[1]
df['views'] = views
df['author'] = author
df['comment_cnt'] = comment_cnt
df['like_cnt'] = like_cnt
df['reward_cnt'] = reward_cnt
timestamp = [time.strftime("%Y%m%dT%H%M%S", time.localtime())] * len(views)
df['timestamp'] = timestamp
df['post_key'] = [time.strftime("%Y%m%dT%H%M%S", time.localtime()) + "".join(random.choices(string.ascii_letters, k = 8))] * len(views)
# ### Import df to sql

conn= sqlite3.connect("/Users/mikexie/pipeline/jianshu.db")

df.to_sql("jianshu_homepage", conn, if_exists='append', index=False)
conn.close()