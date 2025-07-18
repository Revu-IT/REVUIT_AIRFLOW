import os
import pandas as pd
import re
import emoji
from konlpy.tag import Okt

okt = Okt()

# 불용어 리스트
stopwords = [
    '너무', '정말', '그냥', '그리고', '때문에', '또는', '이런', '저런', '해서',
    '니다', '지만', '하다', '에요', '예요', '이에요', '이', '가', '은', '는', '을', '를',
    '에', '에서', '으로', '로', '와', '과', '하고', '도', '만', '까지', '부터', '보다', '처럼',
    '의', '이며', '든지', '라도', '마저', '조차', '이나', '요', '죠', '네', '군요',
    '습니다', '합니다', '했다', '했어요', '했네요', '해요', '네요', '예요', '이에요',
    '였어요', '겠어요', '거든요', '랍니다', '했답니다', '인듯', '같음', '이다', '있다', '않다', '않고', '않으며',
    '들고', '들다', '들며', '같고', '같아서', '같으며', '같은데', '같아', '같다', '돼서', '되다', '되던데', '되어', '됐는데',
    '됐어', '그', '저', '제', '내', '너', '우리', '너희', '그들', '이것', '그것', '저것', '거기', '여기', '저기',
    '좀', '자꾸', '다시', '항상', '계속', '이제', '아직', '벌써', '그래서', '그러니까', '그런데', '하지만', '그러면',
    '혹시', '그치만', '막', '그랬더니', '그랬는데', '그랬어', '그랬음', '그럴까', '그럴게', '그럼', '그러다', '그러니까요',
    '진짜', '완전', '엄청', '되게', '많이', '적당히', '살짝', '조금', '약간', '대충', '아주',
    '때', '것', '수', '거', '중', '듯', '등', '안', '밖에', '밖에서', '안에서', '자신', '자기',
    '했던', '하는', '해서요', '하니까', '하더라고요', '하길래', '하곤', '하자마자', '하든', '하던', '하니까요'
]

def clean_text(text):
    if not isinstance(text, str):
        return ''

    text = text.strip()
    text = emoji.replace_emoji(text, replace='') # 이모지 제거 1
    text = re.sub(r'[~\u2600-\u27BF\u1F600-\u1F64F\u1F300-\u1F5FF\u1F680-\u1F6FF\u1F1E0-\u1F1FF]+', '', text)  # 이모지 제거 2
    text = re.sub(r'[ㅎㅜㅠㅋ]{1,}', ' ', text)  # ㅋㅋ, ㅎㅎ 제거
    text = re.sub(r'[^ㄱ-ㅎ가-힣0-9a-zA-Z]', '', text)  # 이상 문자 제거
    text = re.sub(r'[a-zA-Z]', '', text)  # 영어 제거
    text = re.sub(r'([ㄱ-ㅎㅏ-ㅣ]{2,})', ' ', text)  # 자모 반복 제거
    text = re.sub(r'\s+', ' ', text).strip()  # 공백 정리

    tokens = okt.morphs(text, stem=True)
    tokens = [word for word in tokens if word not in stopwords and len(word) > 1]

    return ' '.join(tokens)

def preprocess_reviews(input_path, output_path):
    df_all = pd.read_csv(input_path, encoding='utf-8-sig')
    print(f"[INFO] 전체 리뷰 개수: {len(df_all)}")

    # 기존 전처리 파일이 있다면 불러오기
    if os.path.exists(output_path):
        df_prev = pd.read_csv(output_path, encoding='utf-8-sig')
        print(f"[INFO] 기존 전처리 리뷰 개수: {len(df_prev)}")

        # 이전에 처리된 리뷰 내용만 필터링해서 제외
        prev_contents = set(df_prev['content'])
        df_new = df_all[~df_all['content'].isin(prev_contents)]
        print(f"[INFO] 새로 처리할 리뷰 개수: {len(df_new)}")
    else:
        df_prev = pd.DataFrame()
        df_new = df_all
        print("[INFO] 기존 전처리 파일 없음. 전체 리뷰 처리")

    # 전처리
    df_new['cleaned_text'] = df_new['content'].apply(clean_text)
    df_new = df_new[df_new['cleaned_text'].str.len().between(2, 999)]
    df_new = df_new[(df_new['score'] >= 1) & (df_new['score'] <= 5)]
    df_new = df_new.drop_duplicates()

    print(f"[INFO] 전처리 후 새 리뷰 수: {len(df_new)}")

    # 기존 데이터와 병합해서 저장
    df_final = pd.concat([df_prev, df_new], ignore_index=True)
    df_final.drop_duplicates(subset=['content', 'cleaned_text'], inplace=True)
    df_final.to_csv(output_path, index=False, encoding='utf-8-sig')

    print(f"✅ 최종 저장 완료: {output_path}, 총 {len(df_final)}개")

# 사용 예시
preprocess_reviews('/opt/airflow/data/G_review.csv', '/opt/airflow/data/G_review_okt.csv')