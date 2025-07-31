# 나눠져 있는 리뷰 데이터를 병합하는 스크립트
import pandas as pd

# 파일 경로 (⚠️ 각자 이커머스명으로 변경)
okt_path = 'data/G_review_okt.csv'
bert_path = 'data/G_review_bert.csv'
output_path = 'data/G_review_result.csv'

# CSV 로드
df_okt = pd.read_csv(okt_path, encoding='utf-8-sig')
df_bert = pd.read_csv(bert_path, encoding='utf-8-sig')

# content 기준 병합
df_merged = pd.merge(df_bert, df_okt[['content', 'cleaned_text']], on='content', how='outer')

# 열 순서 재정렬
desired_columns = ['score', 'date', 'content', 'like', 'type', 'positive', 'cleaned_text']
for col in desired_columns:
    if col not in df_merged.columns:
        df_merged[col] = None

df_merged = df_merged[desired_columns]

# 중복 제거
df_merged = df_merged.drop_duplicates(subset=['content'])

# 날짜 타입 변환 및 정렬
df_merged['date'] = pd.to_datetime(df_merged['date'], errors='coerce')
df_merged = df_merged.sort_values(by='date', ascending=False)

# 저장
df_merged.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"✅ 병합 및 정렬 완료: {output_path}, 총 {len(df_merged)}개 리뷰")
