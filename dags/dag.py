from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys
import importlib.util
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

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

# S3 업로드
def upload_results_to_s3():
    print(f"데이터 폴더 경로: {DATA_FOLDER}")

    result_file = os.path.join(DATA_FOLDER, "11_review_result.csv") # 각자 이커머스에 맞게 수정
    if not os.path.exists(result_file):
        raise FileNotFoundError(f"11_review_result.csv 파일이 존재하지 않습니다: {result_file}")
    
    company_name = os.getenv("COMPANY_NAME", "default_company")
    bucket_name = os.getenv("S3_BUCKET_NAME")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION")

    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME 환경 변수가 설정되지 않았습니다")

    s3_key = f"airflow/{company_name}.csv"
    print(f"업로드 대상 S3 경로: s3://{bucket_name}/{s3_key}")

    # boto3 클라이언트 생성
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )

    try:
        s3.upload_file(result_file, bucket_name, s3_key)
        print(f"✅ S3 업로드 성공: s3://{bucket_name}/{s3_key}")
    except NoCredentialsError:
        print("❌ AWS 자격 증명이 누락되었습니다.")
        raise
    except Exception as e:
        print(f"❌ 업로드 실패: {str(e)}")
        raise

# DAG 정의
with DAG(
    dag_id="revuit_pipeline_controller",
    schedule_interval="0 9 * * *",  # 매일 아침 9시
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
    
    upload_to_s3 = PythonOperator(
        task_id="upload_results_to_s3",
        python_callable=upload_results_to_s3
    )
    
    # 태스크 의존성 설정
    crawling >> general_preprocessing >> sentiment_preprocessing >> department_classification >> upload_to_s3