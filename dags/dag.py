from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys
import importlib.util
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# 에어플로우 환경 경로 설정
AIRFLOW_HOME = "/opt/airflow"
DAGS_FOLDER = os.path.join(AIRFLOW_HOME, "dags")

# 각 폴더 경로 설정 - DAGs 폴더와 같은 레벨에 있는 경로
SCRIPTS_FOLDER = os.path.join(AIRFLOW_HOME, "scripts")
DATA_FOLDER = os.path.join(AIRFLOW_HOME, "data")
KEYS_FOLDER = os.path.join(AIRFLOW_HOME, "keys")

# 스크립트 파일 경로
CRAWL_SCRIPT = os.path.join(SCRIPTS_FOLDER, "crawl.py")
OKT_SCRIPT = os.path.join(SCRIPTS_FOLDER, "okt.py")
SENTIMENT_SCRIPT = os.path.join(SCRIPTS_FOLDER, "sentiment.py")

# 서비스 계정 파일 경로
SERVICE_ACCOUNT_FILE = os.path.join(KEYS_FOLDER, "airflow-463709-f8a4c39f2f87.json")

# 구글 드라이브 폴더 ID
FOLDER_IDS = {
    "크롤링": "157BLZMwIB2dxY7kllI4Tnf-x41TC2Q86",
    "데이터 전처리": "1ywh9Id4U0jEy1RXOWFL84EyS6xIUncNu",
    "감정 분석": "1vue5S1_z9gOPcbgkHDFPoQHnc9KOluy2"
}

# 스크립트 모듈 로드 및 실행 함수
def execute_python_script(script_path):
    """스크립트를 동적으로 임포트하고 실행하는 함수"""
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

# 구글 드라이브 업로드
def upload_results_to_drive():
    print(f"데이터 폴더 경로: {DATA_FOLDER}")
    print(f"서비스 계정 파일 경로: {SERVICE_ACCOUNT_FILE}")
    
    # 서비스 계정 파일 확인
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError(f"서비스 계정 파일을 찾을 수 없습니다: {SERVICE_ACCOUNT_FILE}")
    
    # 데이터 폴더 확인
    if not os.path.exists(DATA_FOLDER):
        raise FileNotFoundError(f"데이터 폴더를 찾을 수 없습니다: {DATA_FOLDER}")
    
    # 구글 드라이브 인증
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    
    service = build('drive', 'v3', credentials=credentials)
    
    # 데이터 폴더 내 파일 업로드
    uploaded_files = 0
    for filename in os.listdir(DATA_FOLDER):
        filepath = os.path.join(DATA_FOLDER, filename)
        
        if os.path.isfile(filepath):
            # 파일 분류
            if "okt" in filename:
                folder_id = FOLDER_IDS["데이터 전처리"]
            elif "bert" in filename:
                folder_id = FOLDER_IDS["감정 분석"]
            else:
                folder_id = FOLDER_IDS["크롤링"]
                
            file_metadata = {
                'name': filename,
                'parents': [folder_id]
            }
            
            media = MediaFileUpload(filepath, resumable=True)
            file = service.files().create(body=file_metadata, media_body=media, fields='id', supportsAllDrives=True).execute()
            uploaded_files += 1
            print(f"파일 업로드 완료: {filename}, ID: {file.get('id')}")
    
    print(f"총 {uploaded_files}개 파일 업로드 완료")

# DAG 정의
with DAG(
    dag_id="gyuri_pipeline_controller",
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
    
    upload_to_drive = PythonOperator(
        task_id="upload_results_to_drive",
        python_callable=upload_results_to_drive
    )
    
    # 태스크 의존성 설정
    crawling >> general_preprocessing >> sentiment_preprocessing >> upload_to_drive