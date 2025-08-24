import os
import pandas as pd
import numpy as np
import json, re
import time
from dotenv import load_dotenv
from openai import OpenAI
from langchain_openai import ChatOpenAI
#from langsmith import traceable

load_dotenv("/opt/airflow/.env", override=True)
AIRFLOW_HOME = "/opt/airflow"
DATA_FOLDER = os.path.join(AIRFLOW_HOME, "data")
DEPT_INFO_PATH = os.path.join(DATA_FOLDER, "department_info.csv")
COMPANY_NAME = os.getenv("COMPANY_NAME", "coupang").lower()
REVIEW_PATH = os.path.join(DATA_FOLDER, f"{COMPANY_NAME}_review_result.csv")

# LangChain LLM 객체 생성
llm = ChatOpenAI(
    model="gpt-3.5-turbo",
    openai_api_key=os.getenv("OPENAI_API_KEY"),
    temperature=0.0
)

# 부서 기준 정보 불러오기
dept_df = pd.read_csv(DEPT_INFO_PATH, encoding="utf-8-sig")
dept_descriptions = ""
for _, row in dept_df.iterrows():
    dept_descriptions += f"{row['아이디']}: {row['통합 부서명']}: {row['주요 키워드']}\n"

#@traceable(name="review-dept-classification")
def ask_gpt_for_department(review_text):
    system_prompt = (
        "너는 사용자의 리뷰를 분석해서 적절한 부서로 분류해주는 AI야. 가능한 부서는 아래와 같아:\n\n"
        + dept_descriptions +
        "\n\n조건:\n"
        "1. 부서 여러 개면 ','로 정수만 알려줘 (예: 1,3)\n"
        #"2. confidence(1~10), think(이유 한 문장) 포함\n"
        "2. 반드시 아래 JSON 형식으로 답해:\n"
        "{\n"
        "  \"department\": \"2,3\",\n"
        #"  \"confidence\": 8,\n"
        #"  \"think\": \"리뷰에 배송과 품질에 대한 내용이 모두 언급됨.\"\n"
        "}\n"
    )
    user_prompt = f"리뷰: \"{review_text}\""
    
    # LangChain LLM 사용
    response = llm.invoke([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ])
    content = response.content.strip()
    
    try:
        json_str = re.search(r"\{.*\}", content, re.DOTALL).group()
        result = json.loads(json_str)
    except Exception as e:
        # result = {"department": "", "confidence": 0, "think": "오류 발생"}
        result = {"department": ""}
    
    # 부서 아이디만 추출
    dept_raw = result.get("department", "")
    dept_clean = ",".join([s.strip() for s in re.findall(r'\d+', dept_raw)])
    if not dept_clean:
        dept_clean = "0"
    return {
        "department": dept_clean,
        #"confidence": int(result.get("confidence", 0)),
        #"think": result.get("think", "")
    }

def classify_department():
    df = pd.read_csv(REVIEW_PATH, encoding="utf-8-sig")
    
    if "department" not in df.columns:
        df["department"] = np.nan

    updated = 0

    for i in range(len(df)):    
        if pd.isna(df.loc[i, "department"]) or str(df.loc[i, "department"]).strip() == "":
            result = ask_gpt_for_department(df.loc[i, "content"])
            df.at[i, "department"] = result["department"]
            updated += 1

            # 중간 저장 (50개 단위)
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