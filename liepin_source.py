# ******* Make RDB for hupu
"""
Input -> https://www.liepin.com/
Ouput -> Append sqlite3 liepin.db: 
         company, job_title, salary, location, experience, degree, industry_class, ipo_status, scale, timestamp
"""


# ******* Make RDB
from requests_html import HTMLSession
import pandas as pd
import time
import csv, sqlite3
session = HTMLSession()
url = "https://www.liepin.com/" # 爬取路径
r = session.get(url)

# 获取页面对应的css方法：1）F12 / inspector 2) 找到要截取的页面的部分 3）右键copy->copy Selector
selector = 'body > section.home-industry-container.common-margin-top.common-page-container > div > div.home-industry-content-container'
# raw results
raw_results = r.html.find(selector)
# print(raw_results[0].text)

def get_text_link_from_sel_liepin(sel:str, url:str , n = 8):
    """
    sel = selector
    url = url to be fetched
    """
    company = []
    job_title_salary = []
    location = []
    experience_degree = []
    industry = []
    r = session.get(url)
    results = r.html.find(sel)
    mylist = []
    try: 
        file = results[0].text
        for i in range(2, len(file.split('\n')) - 2): 

            if i % 5 == 0:
                print('company:',file.split('\n')[i])
                company.append(file.split('\n')[i])
            elif i % 5 == 1:
                print('industry:',file.split('\n')[i])
                industry.append(file.split('\n')[i])
            elif i % 5 == 2:
                print('job_title_salary:',file.split('\n')[i])
                job_title_salary.append(file.split('\n')[i])

            elif i % 5 == 3:
                print('location:', file.split('\n')[i])
                location.append(file.split('\n')[i])   
            else:
                print('experience_degree:',file.split('\n')[i])
                experience_degree.append(file.split('\n')[i])
            if len(company) == len(job_title_salary) == len(location) == len(experience_degree) == len(industry) == n:
                break
        job_title = []
        salary = []
        experience = []
        degree = []
        industry_class = []      
        scale = []
        ipo_status = []
        for j in range(0, n):
            job_title.append(job_title_salary[j].split(" ")[0]) 
            salary.append(job_title_salary[j].split(" ")[1])
            experience.append(experience_degree[j].split(" ")[0])
            degree.append(experience_degree[j].split(" ")[1])
            industry_class.append(industry[j].split(" ")[0])
            if industry[j].split(" ") == 2:
                ipo_status.append('unknown')
                scale.append(industry[j].split(" ")[1])
            elif industry[j].split(" ") == 3:
                ipo_status.append(industry[j].split(" ")[1])
                scale.append(industry[j].split(" ")[2])
                    
        mylist = [company, job_title, salary, location, experience, degree, industry]
        return mylist   
    except:    
        return None
output = get_text_link_from_sel_liepin(selector, url = url)
print("example outcome:", output) # example
# 1) Obtain subject + subject_topic
# 2) Obtain industry_class = [] scale = [] ipo_status = [] from view_author_comment_like_reward's strings
def industry(s):
    """
    s = get_text_link_from_sel_liepin(selector, url)[-1]
    """
    tmp = s.split(" ")
    industry_class = tmp[0]
    if len(tmp) == 2:
        ipo_status = "unknown"
        scale = tmp[1]
    else:                   
        ipo_status = tmp[1]
        scale = tmp[2]
       
    return industry_class, ipo_status, scale

# # 3) get input all data
industry_class = []      
ipo_status = []
scale = []
for s in get_text_link_from_sel_liepin(selector, url)[-1]:
    industry_class_tmp, ipo_status_tmp, scale_tmp = industry(s)[0], industry(s)[1], industry(s)[2]
    industry_class.append(industry_class_tmp)  
    ipo_status.append(ipo_status_tmp)
    scale.append(scale_tmp)

# 4) merge all data as dataframe
df = pd.DataFrame(columns=['company', 'job_title', 'salary', 'location', 'experience', 'degree', 'industry_class', 'ipo_status', 'scale', 'timestamp'])  
df['company'] = output[0]
df['job_title'] = output[1]
df['salary'] = output[2]
df['location'] = output[3]
df['experience'] = output[4]
df['degree'] = output[5]
df['industry_class'] = industry_class
df['ipo_status'] = ipo_status
df['scale'] = scale
timestamp = [time.strftime("%Y%m%dT%H%M%S", time.localtime())] * len(output[0])
df['timestamp'] = timestamp

### Import df to sql

conn= sqlite3.connect("/Users/mikexie/pipeline/liepin.db")

df.to_sql("liepin_homepage", conn, if_exists='append', index=False)
conn.close()
    