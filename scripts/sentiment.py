import os
import torch
import numpy as np
import pandas as pd
from transformers import AutoTokenizer, AutoModelForSequenceClassification, TrainingArguments, Trainer


# ⚠️ 각자 이커머스명으로 변경
AIRFLOW_HOME = "/opt/airflow"
DATA_FOLDER = os.path.join(AIRFLOW_HOME, "data")
MODEL_FOLDER = os.path.join(AIRFLOW_HOME, "model")
INPUT_PATH = os.path.join(DATA_FOLDER, "G_review_result.csv")
OUTPUT_PATH = os.path.join(DATA_FOLDER, "G_review_result.csv")
MODEL_PATH = os.path.join(MODEL_FOLDER, "bert")
TOKENIZER_PATH = os.path.abspath(os.path.join(MODEL_FOLDER, "tokenizer"))

# 모델 및 토크나이저 로드
tokenizer = AutoTokenizer.from_pretrained(TOKENIZER_PATH, local_files_only=True)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_PATH, local_files_only=True)

# Trainer 설정
predict_args = TrainingArguments(
    output_dir=os.path.join(AIRFLOW_HOME, "predict_temp"),
    per_device_eval_batch_size=16,
    report_to="none"
)

trainer = Trainer(
    model=model,
    args=predict_args
)

df_all = pd.read_csv(INPUT_PATH, encoding='utf-8-sig')
df_all = df_all.dropna(subset=['content'])

# 기존 감정분석 결과가 있다면 불러오기
if 'positive' in df_all.columns:
    df_prev = df_all[df_all['positive'].notna()]
    print(f"✅ 이전 감정분석 리뷰 수: {len(df_prev)}")

    # 이미 분석된 리뷰는 제외
    df_new = df_all[df_all['positive'].isna()].copy()
    print(f"✅ 새로 분석할 리뷰 수: {len(df_new)}")
else:
    df_all['positive'] = np.nan
    df_prev = pd.DataFrame()
    df_new = df_all.copy()
    print("✅ 기존 감정분석 컬럼 없음. 전체 리뷰 분석")

# 새 리뷰가 없으면 종료
if df_new.empty:
    print("🚫 새로 분석할 리뷰 없음. 종료")
else:
    # 토크나이징
    texts = df_new['content'].astype(str).tolist()
    encodings = tokenizer(texts, truncation=True, padding=True, max_length=128)

    class UnlabeledDataset(torch.utils.data.Dataset):
        def __init__(self, encodings):
            self.encodings = encodings

        def __getitem__(self, idx):
            return {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}

        def __len__(self):
            return len(self.encodings["input_ids"])

    dataset = UnlabeledDataset(encodings)

    # 예측 수행
    output = trainer.predict(dataset)
    preds = np.argmax(output.predictions, axis=1)

    # 결과 추가
    df_new['positive'] = preds.astype(int)  # 0: 부정, 1: 긍정

    # 기존 결과와 합치기
    df_all = pd.concat([df_all, df_new], ignore_index=True)
    df_all.drop_duplicates(subset=["content", "date"], keep="last", inplace=True)
    df_all = df_all.sort_values(by="date", ascending=False)
    df_all.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
    
    print(f"✅ 감정 분석 완료. 총 저장 리뷰 수: {len(df_all)} → {OUTPUT_PATH}")
