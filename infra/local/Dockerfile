FROM python:3.12.12-slim
WORKDIR /app
COPY requirements-local.lock /app/requirements-local.lock
RUN pip install --no-cache-dir -r requirements-local.lock
COPY dpe /app/dpe
COPY rules /app/rules
COPY contracts /app/contracts
COPY synthetic /app/synthetic
COPY transformations /app/transformations
COPY benchmarks /app/benchmarks
COPY orchestration /app/orchestration
COPY infra/local /app/infra/local
CMD ["python", "-m", "dpe", "--help"]
