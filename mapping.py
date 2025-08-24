import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

COMPANY_NAME = os.getenv("COMPANY_NAME", "coupang").lower()
DATA_FOLDER = "data"
REVIEW_PATH = os.path.join(DATA_FOLDER, f"{COMPANY_NAME}_review_result.csv")
DEPT_INFO_PATH = os.path.join(DATA_FOLDER, "department_info.csv")

# 부서 정보 로드
dept_df = pd.read_csv(DEPT_INFO_PATH, encoding='utf-8-sig')
dept_map = {row['통합 부서명'].strip(): str(int(row['아이디'])) for _, row in dept_df.iterrows()}

def dept_to_id(x):
    x = str(x).strip()
    # '기타'는 0으로 매핑
    if not x or x == '기타' or x not in dept_map:
        return "0"
    return dept_map[x]

review_df = pd.read_csv(REVIEW_PATH, encoding='utf-8-sig')
review_df['department'] = review_df['department'].apply(dept_to_id)
review_df.to_csv(REVIEW_PATH, index=False, encoding='utf-8-sig')
print("✅ 변환 완료")
