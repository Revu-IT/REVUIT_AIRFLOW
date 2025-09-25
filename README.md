
<img width="1920" height="1080" alt="표지 이미지" src="https://github.com/user-attachments/assets/635cb1b6-67c5-4e3e-bd49-2f015725985c" />


<br><br>
## 📢 리뷰잇 REVU IT!
>*데이터에 가려진 고객의 생각, 리뷰잇이 명확하게 분석해드릴게요!* <br>


<br><br>

### 📙 프로젝트 소개
본 프로젝트는 **이커머스 플랫폼의 리뷰 데이터를 자동으로 수집**하고 자연어 처리
(NLP) 통해 분석 및 시각화하여 서비스의 품질 향상과 **사용자 경험을 지속적으로 개선하는 리뷰 분석 시스템을 구축**하는 것을 목표로 한다.

<br><br>

### 🙌 개발 배경
글로벌 이커머스 시장 경쟁 심화로 앱 리뷰와 사용자 피드백은 서비스 품질과 고객
유지율을 좌우하는 지표로 부상하고 있다. 그러나 많은 기업이 여전히 수작업 응대에 의존해 효율성과 비용 측면에서 한계를 겪고 있다. 이에 **감정 분석과 키워드 도출을 통해 고객 만족도를 수치화**하고 시각화된 **리뷰 데이터를 기반으로 주요 개선점을 도출**한다. 궁극적으로는 고객 피드백을 전략 수립에 직접 반영하는 데이터 기반 의사결정 시스템을 구축해 플랫폼 경쟁력을 강화하고자 한다.

<br><br>

### ✨ 주요 기능
#### 1. 메인 페이지
* 리뷰 평점 추이, 분기별 리포트, 분기별 키워드를 한 눈에 확인할 수 있다.<br>
<img src="https://github.com/user-attachments/assets/0dac3c91-8bb8-40a9-95f4-df2404664a59" alt="image" width="80%">

<br>

#### 2. 경쟁사 분석 (1) - 앱 순위
* 감정 분석을 통한 긍정/부정 리뷰 비율을 기반으로 소비자가 선호하는 기업의 순위를 알 수 있다.<br>
<img src="https://github.com/user-attachments/assets/4c0cac54-52cd-437b-adde-e7d8635a939e" alt="image" width="80%">

<br>

#### 3. 경쟁사 분석 (2) - 업계 키워드 클라우드
* 분기마다 갱신되는 이커머스 전체의 긍정/부정적인 키워드 50개를 확인할 수 있다.<br>
<img src="https://github.com/user-attachments/assets/91be0c21-3a1b-43e8-b43b-cd480aacd6c4" alt="image" width="80%">

<br>

#### 4. 자사 키워드 분석
* 최신 리뷰를 기반으로 어떤 부분이 강점/약점인지 확인할 수 있다.<br>
<img src="https://github.com/user-attachments/assets/9bfe8575-c89a-4e41-aef8-9fb447825fc5" alt="image" width="80%">

<br>

#### 5. 부서 분류
* 리뷰를 부서별로 분류하여 각 부서의 담당자가 쉽게 확인할 수 있도록 한다.<br>
<img src="https://github.com/user-attachments/assets/021e3f46-2e95-4863-8331-9649fa559220" alt="image" width="80%">

<br>

#### 6. 부서별 리포트
* 부서별 강점과 약점 네 가지를 요약하여 보여준다.<br>
* AI가 제시한 부서별 문제 해결 방안도 확인할 수 있다.<br>
<img src="https://github.com/user-attachments/assets/09964064-eb29-4dc9-b51a-19a44f91817a" alt="image" width="50%">

<br><br>

### 📊 타임라인

| 날짜     | 주요 내용 |
|----------|-----------|
| 2월 16일 | 아이디어 기획 및 기술 요구사항 정의서 작성 |
| 4월 11일 | 이커머스 주요 5사의 구글 플레이 스토어 리뷰 데이터 크롤링 |
| 4월 18일 | 자연어 처리(NLP)를 위해 데이터 OKT 형태소 분석 및 정 |
| 5월 2일  | 리뷰 데이터 파이프라인(크롤링 → 전처리 → 감정 분석)을 Airflow 워크플로우로 배포 |
| 5월 16일 | 리뷰 부서 분류 로직을 룰 베이스 방식에서 OpenAI 기반 자동 분류로 전환 |
| 5월 30일 | 리뷰 데이터 파이프라인(크롤링 → 전처리 → 감정 분석 → 부서 분류)을 Airflow 워크플로우로 확장 배포 |
| 6월 22일 | FastAPI 기반 백엔드 서버 구축 및 React 기반 프론트엔드 아키텍처 세팅 |
| 7월 4일  | 한이음 드림업 공모전 준비 및 출품 |
| 7월 25일 | 부서별 리포트 생성 기능을 OpenAI와 Claude 기반으로 각각 구현하고 결과를 비교·분석 |
| 8월 8일  | 프론트엔드와 백엔드 연동 |
| 8월 24일 | 피드백을 반영한 프로젝트 성과 점검 및 개선 |

<br><br>

### 🔥 기술 스택

####  ️Front-end
<img src="https://img.shields.io/badge/HTML5-E34F26?style=for-the-badge&logo=HTML5&logoColor=white"/> <img src="https://img.shields.io/badge/CSS3-1572B6?style=for-the-badge&logo=CSS3&logoColor=white"/> <img src="https://img.shields.io/badge/JavaScript-F7DF1E?style=for-the-badge&logo=JavaScript&logoColor=white"/>
<img src="https://img.shields.io/badge/React-61DAFB?style=for-the-badge&logo=React&logoColor=white"/> <img src="https://img.shields.io/badge/netlify-00C7B7?style=for-the-badge&logo=netlify&logoColor=white"/>

#### Back-end
<img src="https://img.shields.io/badge/python-3776AB?style=for-the-badge&logo=python&logoColor=white"/> <img src="https://img.shields.io/badge/fastapi-009688?style=for-the-badge&logo=fastapi&logoColor=white"/> 
<img src="https://img.shields.io/badge/Ubuntu-E95420?style=for-the-badge&logo=Ubuntu&logoColor=white"/> <img src="https://img.shields.io/badge/amazonec2-FF9900?style=for-the-badge&logo=amazonec2&logoColor=white"/>

#### Data Pipeline
<img src="https://img.shields.io/badge/python-3776AB?style=for-the-badge&logo=python&logoColor=white"/> <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white"/> 
<img src="https://img.shields.io/badge/Ubuntu-E95420?style=for-the-badge&logo=Ubuntu&logoColor=white"/> <img src="https://img.shields.io/badge/amazonec2-FF9900?style=for-the-badge&logo=amazonec2&logoColor=white"/>

#### database
<img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white"/> <img src="https://img.shields.io/badge/Ubuntu-E95420?style=for-the-badge&logo=Ubuntu&logoColor=white"/> <img src="https://img.shields.io/badge/amazonrds-527FFF?style=for-the-badge&logo=amazonrds&logoColor=white"/>

#### Collaboration
<img src="https://img.shields.io/badge/Github-black?style=for-the-badge&logo=Github&logoColor=white"/> <img src="https://img.shields.io/badge/Discord-5865F2?style=for-the-badge&logo=Discord&logoColor=white"/> <img src="https://img.shields.io/badge/Figma-F24E1E?style=for-the-badge&logo=Figma&logoColor=white"/> <img src="https://img.shields.io/badge/Notion-black?style=for-the-badge&logo=Notion&logoColor=white"/>

<br><br>

### 🧬 시스템 구조도
![image](https://github.com/user-attachments/assets/3a232ab3-9a57-47d6-9da4-f5863d955b69)
