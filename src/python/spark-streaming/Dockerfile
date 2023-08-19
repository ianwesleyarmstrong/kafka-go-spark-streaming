FROM jupyter/all-spark-notebook:spark-3.4.1

ENV POETRY_VERSION=1.3.0
ENV PATH="/home/jovyan/.local/bin:$PATH"
RUN curl -sSL https://install.python-poetry.org | python3 - && \
    poetry config virtualenvs.create false

WORKDIR /app/

COPY pyproject.toml poetry.lock /app/

RUN poetry install --no-root

COPY spark_streaming/main.py /app/main.py

CMD [ "poetry", "run", "python", "main.py" ]