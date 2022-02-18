# ******* Make RDB for hupu
"""
Input -> https://bbs.hupu.com/all-gambia
Ouput -> Append sqlite3 hupu.db: 
         subject, subject_topic, liang_cnt, comment_cnt, timestamp
"""

from requests_html import HTMLSession
import pandas as pd
import time
import re
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
def obtain_liang_comment(comment_cnt):
    """
    comment = get_text_link_from_sel_hupu(selector, url)
    """
    tmp = comment_cnt[::-1]
    import re
    ind_word = 0
    comment_cnt = ''
    liang_cnt = ''
    i = 0
    while ind_word < 3:
        while tmp[i] not in '1234567890':
            i += 1
        while tmp[i] in '1234567890':
            ind_word = 2
            comment_cnt += tmp[i]
            i += 1
        if tmp[i] == "亮":
            i += 1
            ind_word = 3
        while tmp[i] in '1234567890':
            liang_cnt += tmp[i]
            i += 1
    while int(liang_cnt[::-1]) > int(comment_cnt[::-1]):
        liang_cnt = str(list(liang_cnt).pop(0))
    return [{"liang_cnt": int(liang_cnt[::-1]), "comment_cnt": int(comment_cnt[::-1])}, liang_cnt[::-1]+'亮'+comment_cnt[::-1]+'回复']
output = get_text_link_from_sel_hupu(selector, url)

# 3) remove liang & comment from the string + input all data
comment_cnt = []
subject = []
liang_cnt = []
for ind, value in enumerate(output[0]):
    result = obtain_liang_comment(output[0][ind])
    pattern = result[1]
    liang_cnt.append(result[0].get('liang_cnt'))
    comment_cnt.append(result[0].get('comment_cnt'))
    comment_tmp = re.sub(pattern, "", value)
    subject.append(comment_tmp)
subject_topic = output[1] 

# 4) merge all data as dataframe
df = pd.DataFrame(columns=['subject', 'subject_topic', 'liang_cnt', 'comment_cnt', 'timestamp'])  
df['subject'] = subject
df['subject_topic'] = subject_topic
df['liang_cnt'] = liang_cnt
df['comment_cnt'] = comment_cnt
timestamp = [time.strftime("%Y%m%dT%H%M%S", time.localtime())] * len(subject)
df['timestamp'] = timestamp

### Import df to sql

conn= sqlite3.connect("/Users/mikexie/pipeline/hupu.db")

df.to_sql("hupu_buxingjie", conn, if_exists='append', index=False)
conn.close()