# set base image
FROM jupyter/pyspark-notebook

# set working directory in container
WORKDIR page_count

# copy dependencies file to container
COPY requirements.txt ./

RUN pip install -r requirements.txt

# copy files to working directory
COPY src ./src
COPY tests ./tests
COPY README.md setup.py ./

ENV PYTHONPATH="${PYTHONPATH}:."

# command to run on container start
CMD ["python", "src/main.py"]