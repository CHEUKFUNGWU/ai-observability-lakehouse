FROM apache/superset:4.1.1

USER root
RUN pip install --no-cache-dir PyMySQL==1.1.1
COPY docker/superset/sitecustomize.py /usr/local/lib/python3.10/site-packages/sitecustomize.py
USER superset
