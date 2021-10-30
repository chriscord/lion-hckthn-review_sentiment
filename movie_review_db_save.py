# 현재 네이버에서 조회되는 영화 리뷰 감정 분석
# https://movie.naver.com/movie/sdb/rank/rmovie.naver?sel=cnt&date=20211023 (조회순)

import requests
import json
from bs4 import BeautifulSoup
from requests.exceptions import SSLError
from urllib import parse 
from pymongo.errors import BulkWriteError
from pymongo import MongoClient

DATE=20211023 #type해서 날짜로 조회하도록
review_url = f"https://movie.naver.com/movie/sdb/rank/rmovie.naver?sel=cnt&date={DATE}"

main_page=requests.get(review_url)
soup=BeautifulSoup(main_page.text, "html.parser")

page=soup.find_all("td", {"class": "title"})


movies_list=[]

def sentiment(comment):
    #sentiment id, secret
    client_id="4ygmfbte6g"
    client_secret="gzxCoxvKL3N86HFQLjO8XQYErDaIBDK9Fpjx2QiD"
    
    headers = {"X-NCP-APIGW-API-KEY-ID": client_id,
               "X-NCP-APIGW-API-KEY": client_secret,
               "Content-Type": "application/json"}

    content = {"content": comment}
	
    r = requests.post("https://naveropenapi.apigw.ntruss.com/sentiment-analysis/v1/analyze",
                      headers=headers, data=json.dumps(content))

    sentiment_result = {}
    if r.status_code == requests.codes.ok:
        result_response = json.loads(r.content)
        sentiment_result = result_response["document"]["confidence"]

    return sentiment_result




def extract_list():
    for i in range(len(page)):
        route=page[i].find("a")["href"]
        movie_link=f"https://movie.naver.com/{route}"
        movie_title=page[i].find("a")["title"] 
        movies_list.append([movie_title, movie_link])
        # print(route)
        # print(movie_title)
        
extract_list()
#movies_list[0][0]=영화제목, movies_list[0][1]=링크



def get_reviews():
    doc=[]
    for i in range(len(page)):
        CODE=parse.urlparse(movies_list[i][1]).query

        m_link=f"https://movie.naver.com/movie/bi/mi/pointWriteFormList.naver?{CODE}&type=after&isActualPointWriteExecute=false&isMileageSubscriptionAlready=false&isMileageSubscriptionReject=false"
        m_page=requests.get(m_link)
        m_soup=BeautifulSoup(m_page.text, "html.parser")
        m_main = m_soup.find("div", {"class":"input_netizen"}).find_all("div", {"class":"score_reple"})
        # print(m_main)
        #영화 타이틀
        # print("\n", movies_list[i][0], "\n")  #{"영화": "베놈2" }
        object=dict()
        movie_title=movies_list[i][0]
        object['title']=f"{movie_title}"
        
        doc.append(object)
        for j in range(len(m_main)-5):
            comment=m_main[j].find("span", {"id": f"_filtered_ment_{j}"}).text.strip()
            # print(comment)
            if(comment):
                doc[i][f"베댓{j}"]=comment
            else:
                doc[i][f"베댓{j}"]=""
    return doc
     

# def save_to_db(my_ip, username, password, db_name, collection_name, docs):
def save_to_db(my_ip, username, password, db_name, collection_name, docs):
 
    db_result = {'result': 'success'}

    client = MongoClient(host=my_ip, port=27017,
                         username=username, password=password)
    db = client[db_name]
    collection = db[collection_name]  # unique key 설정할 collection
    collection.drop() #collection 초기화
    

    try:
        collection.insert_many(docs, ordered=False)

    except BulkWriteError as bwe:
        db_result['result'] = 'Insert and Ignore duplicated data'

    return db_result

# return db_result

docs=[]
docs=get_reviews()
# print(docs)


# =======DB 저장
my_ip = '49.50.164.119'
username = 'likelion'
password = 'wearethefuture'

client = MongoClient('mongodb://likelion:wearethefuture@' + my_ip, 27017)

db_name='likelion'
collection_name='hackathon'

result = save_to_db(my_ip, username, password, db_name, collection_name, docs)
print(result)

comment=docs[0]['베댓0']

# sentiment(txt, client_id, client_secret)

for i in range(5):
    comment=docs[0][f"베댓{i}"]
    senti_result=sentiment(comment)
    print(docs[0]['title'])
    print(docs[0][f"베댓{i}"])
    print(senti_result)


# for i in range(len(page)):
#     for j in range(5):
#         comment=docs[i][f"베댓{j}"]
#         senti_result=sentiment(comment)
#         print(senti_result)


        
# {'negative': 0.12924366, 'positive': 99.84436, 'neutral': 0.026396887}
