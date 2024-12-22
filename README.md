# Catch Me My Capital

A data engineering project focused on collecting and managing financial data with a vision for backtesting and analysis.

### 개발환경 설정
- Python version: `3.11.8`
- 개발 의존성 설치
```bash
pip install -r requirements-dev.txt  # 개발 의존성 패키지 설치
```

- pre-commit 설정

```bash
chmod -R +x ./scripts/  # 스크립트 파일에 실행 권한 부여
pre-commit install      # pre-commit 설치
pre-commit install --hook-type commit-msg  # commit-msg 훅 설치
```
