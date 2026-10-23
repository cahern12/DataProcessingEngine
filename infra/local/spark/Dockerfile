FROM apache/spark:3.5.5-python3@sha256:39321d67b23e2e0953f81b60778f74bf40c40a18dfb0e881e6a38593af60afa1
USER root
RUN pip install --no-cache-dir jinja2==3.1.6 MarkupSafe==2.1.5
COPY infra/local/spark/dependencies.json /tmp/dependencies.json
RUN python3 -c "import json,urllib.request,hashlib,pathlib; d=json.load(open('/tmp/dependencies.json')); [(lambda b,j: (pathlib.Path('/opt/spark/jars/'+j['url'].split('/')[-1]).write_bytes(b), b and hashlib.sha256(b).hexdigest()==j['sha256'] or (_ for _ in ()).throw(ValueError('jar checksum mismatch'))))(urllib.request.urlopen(j['url']).read(),j) for j in d['jars']]"
WORKDIR /app
COPY dpe /app/dpe
COPY rules /app/rules
COPY contracts /app/contracts
COPY synthetic /app/synthetic
COPY transformations /app/transformations
COPY benchmarks /app/benchmarks
COPY orchestration /app/orchestration
COPY infra/local /app/infra/local
COPY requirements-local.lock /app/requirements-local.lock
ENV PYTHONPATH=/app
ENV PYSPARK_PYTHON=python3
ENTRYPOINT ["/opt/spark/bin/spark-submit","--driver-memory","2g","--conf","spark.driver.host=spark","--conf","spark.sql.shuffle.partitions=2","/app/dpe/engines/spark_job.py"]
