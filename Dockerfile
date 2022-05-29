FROM python:3.7-buster
RUN apt-get update -yqq
RUN pip install --user psycopg2-binary


ENV HOME_DIR=/opt/gtfs_import
ENV PYTHONPATH=$HOME_DIR
COPY . $HOME_DIR/
WORKDIR $HOME_DIR
RUN pip install --editable .
ENTRYPOINT ["gtfs_import"]