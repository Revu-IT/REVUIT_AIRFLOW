from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys
import importlib.util
import csv
import psycopg2

# 에어플로우 환경 경로 설정
AIRFLOW_HOME = "/opt/airflow"
DAGS_FOLDER = os.path.join(AIRFLOW_HOME, "dags")

# 각 폴더 경로 설정
SCRIPTS_FOLDER = os.path.join(AIRFLOW_HOME, "scripts")
DATA_FOLDER = os.path.join(AIRFLOW_HOME, "data")

# 스크립트 파일 경로
CRAWL_SCRIPT = os.path.join(SCRIPTS_FOLDER, "crawl.py")
OKT_SCRIPT = os.path.join(SCRIPTS_FOLDER, "okt.py")
SENTIMENT_SCRIPT = os.path.join(SCRIPTS_FOLDER, "sentiment.py")
DEPARTMENT_SCRIPT = os.path.join(SCRIPTS_FOLDER, "department.py")

# PostgreSQL 연결 정보 (환경변수에서 불러오기)
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# 회사명 → ID 매핑
company_mapping = {
    "coupang": 1,
    "aliexpress": 2,
    "gmarket": 3,
    "11st": 4,
    "temu": 5
}
COMPANY_NAME = os.getenv("COMPANY_NAME", "coupang").lower()
COMPANY_ID = company_mapping.get(COMPANY_NAME, 1)

# 스크립트 모듈 로드 및 실행 함수
def execute_python_script(script_path):
    print(f"스크립트 경로: {script_path}")
    
    # 스크립트 파일 존재 확인
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"스크립트 파일을 찾을 수 없습니다: {script_path}")
    
    # 스크립트 이름 추출
    script_name = os.path.basename(script_path)
    module_name = os.path.splitext(script_name)[0]
    
    # 모듈 스펙 생성
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None:
        raise ImportError(f"스크립트를 모듈로 가져올 수 없습니다: {script_path}")
    
    # 모듈 생성
    module = importlib.util.module_from_spec(spec)
    
    # 스크립트 디렉토리를 시스템 경로에 추가
    script_dir = os.path.dirname(script_path)
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)
    
    # 현재 작업 디렉토리 변경
    original_dir = os.getcwd()
    os.chdir(script_dir)
    
    try:
        # 모듈 실행
        spec.loader.exec_module(module)
        print(f"스크립트 {script_name} 실행 완료")
    finally:
        # 원래 디렉토리로 복원
        os.chdir(original_dir)

# 각 스크립트 실행 함수
def run_crawling_script():
    print(f"크롤링 스크립트 실행 중... 경로: {CRAWL_SCRIPT}")
    execute_python_script(CRAWL_SCRIPT)

def run_general_preprocessing():
    print(f"일반 전처리 스크립트 실행 중... 경로: {OKT_SCRIPT}")
    execute_python_script(OKT_SCRIPT)

def run_sentiment_preprocessing():
    print(f"감정 분석 전처리 스크립트 실행 중... 경로: {SENTIMENT_SCRIPT}")
    execute_python_script(SENTIMENT_SCRIPT)
    
def run_department_classification():
    print(f"부서 분류 스크립트 실행 중... 경로: {DEPARTMENT_SCRIPT}")
    execute_python_script(DEPARTMENT_SCRIPT)

# PostgreSQL 업로드
def insert_results_to_postgres():
    result_file = os.path.join(DATA_FOLDER, f"{COMPANY_NAME}_review_result.csv")
    if not os.path.exists(result_file):
        raise FileNotFoundError(f"CSV 파일이 존재하지 않습니다: {result_file}")

    conn = psycopg2.connect(
        host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD, port=PG_PORT
    )
    cursor = conn.cursor()

    # 부서 매핑
    cursor.execute("SELECT id, name FROM department;")
    department_map = {name: dep_id for dep_id, name in cursor.fetchall()}
    print(f"✅ Department 매핑 완료: {department_map}")

    with open(result_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        reader.fieldnames = [name.strip() for name in reader.fieldnames]

        for row in reader:
            row = {k.strip(): v for k, v in row.items()}
            department_name = row.get("department")
            department_id = department_map.get(department_name)
            if department_id is None:
                print(f"❌ 매핑 실패: {department_name}")
                continue

            # 중복 체크
            cursor.execute(
                "SELECT 1 FROM reviews WHERE date=%s AND content=%s AND company_id=%s",
                (row.get("date"), row.get("content"), COMPANY_ID)
            )
            if cursor.fetchone():
                continue

            score_value = row.get("score")
            score = float(score_value) if score_value else None

            likes_value = row.get("like")
            try:
                likes = int(float(likes_value)) if likes_value else 0
            except ValueError:
                likes = 0

            positive_value = str(row.get("positive")).strip().lower()
            is_positive = positive_value in ["1", "1.0", "true", "t", "yes"]

            cursor.execute(
                """
                INSERT INTO reviews
                (date, score, content, likes, cleaned_text, positive, department_id, company_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row.get("date"),
                    score,
                    row.get("content"),
                    likes,
                    row.get("cleaned_text"),
                    is_positive,
                    department_id,
                    COMPANY_ID
                )
            )

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ 새로운 CSV 데이터만 PostgreSQL에 저장 완료")

# DAG 정의
with DAG(
    dag_id="revuit_pipeline_controller",
    schedule_interval="0 9 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["pipeline"]
) as dag:
    
    crawling = PythonOperator(
        task_id="run_crawling_script",
        python_callable=run_crawling_script
    )
    
    general_preprocessing = PythonOperator(
        task_id="run_general_preprocessing",
        python_callable=run_general_preprocessing
    )
    
    sentiment_preprocessing = PythonOperator(
        task_id="run_sentiment_preprocessing",
        python_callable=run_sentiment_preprocessing
    )
    
    department_classification = PythonOperator(
        task_id="run_department_classification",
        python_callable=run_department_classification
    )
    
    insert_to_postgres = PythonOperator(
        task_id="insert_results_to_postgres",
        python_callable=insert_results_to_postgres
    )
    
    crawling >> general_preprocessing >> sentiment_preprocessing >> department_classification >> insert_to_postgres