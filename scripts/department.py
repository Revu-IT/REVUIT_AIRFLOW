import os
import pandas as pd
import numpy as np
import time
from openai import OpenAI

AIRFLOW_HOME = "/opt/airflow"
DATA_FOLDER = os.path.join(AIRFLOW_HOME, "data")
DEPT_INFO_PATH = os.path.join(DATA_FOLDER, "department_info.csv")
REVIEW_PATH = os.path.join(DATA_FOLDER, "G_review_result.csv") # ⚠️ 각자 이커머스명으로 변경

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))  # .env에 API 키 설정

# 부서 기준 정보 불러오기
dept_df = pd.read_csv(DEPT_INFO_PATH, encoding="utf-8-sig")
dept_descriptions = ""
for _, row in dept_df.iterrows():
    dept_descriptions += f"{row['통합 부서명']}: {row['주요 키워드']}\n"

def ask_gpt_for_department(review_text):
    system_prompt = (
        "너는 사용자의 리뷰를 분석해서 적절한 부서를 분류해주는 AI야. 가능한 부서는 아래와 같아:\n\n"
        + dept_descriptions +
        "\n\n리뷰가 어떤 부서와도 명확하게 관련이 없다면 '기타'라고만 대답해."
    )
    user_prompt = f"다음 리뷰는 어떤 부서와 가장 관련이 있을까?\n\n리뷰: \"{review_text}\"\n\n부서명만 정확하게 알려줘."

    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.0
    )
    return response.choices[0].message.content.strip()

def classify_department():
    df = pd.read_csv(REVIEW_PATH, encoding="utf-8-sig")

    if "department" not in df.columns:
        df["department"] = np.nan

    updated = 0

    for i in range(len(df)):
        if pd.isna(df.loc[i, "department"]) or str(df.loc[i, "department"]).strip() == "":
            try:
                dept = ask_gpt_for_department(df.loc[i, "content"])
            except Exception as e:
                print(f"[{i}] 에러 발생: {e}")
                dept = "기타"

            df.at[i, "department"] = dept
            updated += 1

            if updated % 50 == 0:
                df.to_csv(REVIEW_PATH, index=False, encoding="utf-8-sig")
                print(f"💾 중간 저장: {updated}개 분류 완료")
                time.sleep(1)

    df.drop_duplicates(subset=["content", "date"], keep="last", inplace=True)
    df = df.sort_values(by="date", ascending=False)
    df.to_csv(REVIEW_PATH, index=False, encoding="utf-8-sig")
    print(f"✅ 전체 부서 분류 완료: {updated}개 새로 분류됨 → {REVIEW_PATH}")

# 사용 예시
classify_department()