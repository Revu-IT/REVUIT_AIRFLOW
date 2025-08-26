from google_play_scraper import reviews, Sort
import os
import time
import pandas as pd
import numpy as np
from datetime import datetime

COMPANY_NAME = os.getenv("COMPANY_NAME", "gmarket").lower()
APP_ID = os.getenv("APP_ID", "com.ebay.kr.gmarket")
DATA_FOLDER = "/opt/airflow/data"
file_path = os.path.join(DATA_FOLDER, f"{COMPANY_NAME}_review_result.csv")
BATCH_SIZE = 200
TARGET_COUNT = 30_500

# 기존 파일 로드 및 날짜 파싱
if os.path.exists(file_path):
    df_old = pd.read_csv(file_path, encoding='utf-8-sig')

    if 'date' in df_old.columns:
        df_old['date'] = pd.to_datetime(df_old['date'], errors='coerce')
        valid_dates = df_old['date'].dropna()

        if not valid_dates.empty:
            start_date = valid_dates.max()
        else:
            print("🚫 유효한 날짜 없음. 기본값 사용")
            start_date = datetime(2023, 1, 1)
    else:
        print("🚫 date 컬럼 없음. 기본값 사용")
        start_date = datetime(2023, 1, 1)
        df_old['date'] = np.nan
else:
    start_date = datetime(2023, 1, 1)
    df_old = pd.DataFrame(columns=['date'])

print(f"✅ 크롤링 시작 기준일: {start_date}")

# 크롤링 시작
all_reviews = []
token = None

while len(all_reviews) < TARGET_COUNT:
    result, token = reviews(
        APP_ID,
        lang='ko',
        country='kr',
        sort=Sort.NEWEST,
        count=BATCH_SIZE,
        continuation_token=token
    )

    # 기준일 이후 리뷰만 필터링
    filtered = [r for r in result if r.get('at') and r['at'] > start_date]

    if not filtered:
        break

    all_reviews.extend(filtered)
    print(f"✅ {len(all_reviews)} / {TARGET_COUNT} 리뷰 수집됨")

    if token is None:
        break

    time.sleep(3)

# 결과 저장
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
    df_new['cleaned_text'] = np.nan
    df_new['positive'] = np.nan

    df_new['date'] = pd.to_datetime(df_new['date'], errors='coerce')
    df_old['date'] = pd.to_datetime(df_old['date'], errors='coerce')

    print(f"🕒 새로 수집한 리뷰 중 가장 최신 날짜: {df_new['date'].max()}")

    df_all = pd.concat([df_old, df_new], ignore_index=True)
    df_all.drop_duplicates(subset=['content', 'date'], keep='last', inplace=True)
    df_all = df_all.sort_values(by='date', ascending=False)
    df_all.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"✅ 새 리뷰 {len(df_new)}개 추가 저장 완료: {file_path}")