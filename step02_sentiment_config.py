# -*- coding: utf-8 -*-
import requests
import json
from pymongo import MongoClient
import pytz
from datetime import datetime, timedelta

# ===== 필요 정보 가져오기 =====

with open('config.json', 'r') as f:
    config = json.load(f)
    api_info = config['SENTIMENT_API']
    db_info = config['DB']
    before_date = config['BEFORE_DATE']
# ====TEST : 설정 불러오는지 확인========
# print(db_info, api_info, before_date)


# ===== DB access =====

client = MongoClient(host=db_info['my_ip'], port=27017,
                     username=db_info['username'], password=db_info['password'])
db = client[db_info['db_name']]
collection = db[db_info['collection_name']]


# =====Add Date field to DB if not exist=======

records = collection.find()

for record in records:
    d_str = record.get('pubDate')
    if d_str is not None:
        naive_d = datetime.strptime(d_str, '%a, %d %b %Y %H:%M:%S %z')
        collection.update_one({'_id': record['_id']}, {
                              '$set': {'date': naive_d}})


# ===== Date function =====

def cal_datetime_utc(before_date, timezone='Asia/Seoul'):
    '''
    현재 일자에서 before_date 만큼 이전의 일자를 UTC 시간으로 변환하여 반환
    :param before_date: 이전일자
    :param timezone: 타임존
    :return: UTC 해당일의 시작시간(date_st)과 끝 시간(date_end)
    :rtype: dict of datetime object
    :Example:
    2021-09-13 KST 에 get_date(1) 실행시,
    return은 {'date_st': datetype object 형태의 '2021-09-11 15:00:00+00:00'), 'date_end': datetype object 형태의 '2021-09-12 14:59:59.999999+00:00'}
    '''
    today = pytz.timezone(timezone).localize(datetime.now())
    target_date = today - timedelta(days=before_date)

    # 같은 일자 same date 의 00:00:00 로 변경 후, UTC 시간으로 바꿈
    start = target_date.replace(hour=0, minute=0, second=0,
                                microsecond=0).astimezone(pytz.UTC)

    # 같은 일자 same date 의 23:59:59 로 변경 후, UTC 시간으로 바꿈
    end = target_date.replace(
        hour=23, minute=59, second=59, microsecond=999999).astimezone(pytz.UTC)

    return {'date_st': start, 'date_end': end}

# ===== Sentiment function =====


def sentiment(txt, client_id, client_secret):
    """
    텍스트를 입력 받아서 clova sentiment api 로 요약
    :param: 감정 분석 대상 텍스트 content 본문
    :param: client_id 클로바 api 클라이언트 id
    :param: client_secret 클로바 api 클라이언트 secret
    :return: 중립, 긍정, 부정 confidence 수치 dict
    :rtype: json
    """

    headers = {"X-NCP-APIGW-API-KEY-ID": client_id,
               "X-NCP-APIGW-API-KEY": client_secret,
               "Content-Type": "application/json"}

    content = {"content": txt}

    r = requests.post("https://naveropenapi.apigw.ntruss.com/sentiment-analysis/v1/analyze",
                      headers=headers, data=json.dumps(content))

    sentiment_result = {}
    if r.status_code == requests.codes.ok:
        result_response = json.loads(r.content)
        sentiment_result = result_response["document"]["confidence"]

    return sentiment_result


# ===== Read from date =====

target_date = cal_datetime_utc(before_date)

# === pagenation 을 통해 일부만 조금씩 가져오기 ===
# 전체 회수 확인하기
cnt_items = collection.count_documents({})
print(cnt_items)
page_num = (cnt_items // 30) + 1
page_size = 30

# 데이터 일부씩 끊어서 반복 loop 업데이트
for i in range(page_num):
    skip_num = i * page_size
    print(i, skip_num)
    limit_items = list(collection.find(
        {}, {'_id': False}
    ).skip(skip_num).limit(page_size))

    # print(f'limit_items: {len(limit_items)}')

    for item in limit_items:
        if len(item['content']) < 1000:
            result = sentiment(
                item['content'], client_id=api_info['client_id'], client_secret=api_info['client_secret'])
        else:
            result = sentiment(
                item['description'], client_id=api_info['client_id'], client_secret=api_info['client_secret'])

        collection.update_one(
            {'link': item['link']}, {'$set': {'sentiment': result}}
        )
