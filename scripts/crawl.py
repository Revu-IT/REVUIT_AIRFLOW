from google_play_scraper import reviews, Sort
import pandas as pd
import time
from datetime import datetime
import os

app_id = 'com.ebay.kr.gmarket'
file_path = '/opt/airflow/data/G_review.csv'
BATCH_SIZE = 200
TARGET_COUNT = 30_500

if os.path.exists(file_path):
    df_old = pd.read_csv(file_path)
    if not df_old.empty and 'date' in df_old.columns:
        df_old['date'] = pd.to_datetime(df_old['date'], errors='coerce')
        start_date = df_old['date'].max()
    else:
        start_date = datetime(2023, 1, 1)
else:
    start_date = datetime(2023, 1, 1)
    df_old = pd.DataFrame()

all_reviews = []
token = None

while len(all_reviews) < TARGET_COUNT:
    result, token = reviews(
        app_id,
        lang='ko',
        country='kr',
        sort=Sort.NEWEST,
        count=BATCH_SIZE,
        continuation_token=token
    )

    filtered = []
    for r in result:
        if r.get('at') is not None and r['at'] > start_date:
            filtered.append(r)

    if not filtered:
        break

    all_reviews.extend(filtered)
    print(f"{len(all_reviews)} / {TARGET_COUNT} 리뷰 수집됨")

    if token is None:
        break

    time.sleep(3)

if not all_reviews:
    print("🚫 새로운 리뷰 없음. 종료")
else:
    df_new = pd.DataFrame(all_reviews)
    df_new = df_new.rename(columns={
        'score': 'score',
        'at': 'date',
        'content': 'content',
        'thumbsUpCount': 'like',
    })
    df_new = df_new[['score', 'date', 'content', 'like']]
    df_new['type'] = 1

    df_all = pd.concat([df_old, df_new], ignore_index=True)
    df_all.drop_duplicates(subset=['content', 'date'], inplace=True)
    df_all.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"✅ 새 리뷰 {len(df_new)}개 추가 저장 완료: {file_path}")
