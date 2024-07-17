FROM apache/superset:2.1.0
RUN pip install psycopg2-binary

COPY ./superset-init.sh /app/superset-init.sh
RUN chmod +x /app/superset-init.sh

COPY superset_config.py /app/
ENV SUPERSET_CONFIG_PATH /app/superset_config.py
RUN chown -R superset:superset /app

USER superset
ENTRYPOINT [ "/app/superset-init.sh" ]
