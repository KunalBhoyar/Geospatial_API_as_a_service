# FROM python:3.10.6
# # Base docker image of python 3.x

# RUN pip install --upgrade pip
# # Upgrade pip package

# WORKDIR /
# # Change working dir to app

# ADD ./src/ /

# # COPY ../apis.py ../schemas.py ../requirements.txt ../metadata.db ../aws_logging.py /app/

# # COPY ../Authentication /app/Authentication

# # COPY ../Util/ /app/Util/

# ENV JWT_SECRET='damg_7225_big_data'
# ENV DbGeo='/src/data/GEOSPATIAL_DATA.db'
# ENV DbPath='/src/data'
# ENV DbUser='/src/data/USER_DATA.db'

# RUN pip install -r ./src/fastapi/requirements.txt

# # EXPOSE 8000
# ## Expose a port inside the container on which services run
# WORKDIR /src/fastapi/
# #CMD ["gunicorn" ,"-w", "4", "-k", "uvicorn.workers.UvicornWorker" , "--bind", "0.0.0.0:8000", "apis:app"]
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
# # gunicorn command to run the service with 4 worker nodes binding localhost/0.0.0.0 on port 8000 refering app inside the main.py



FROM python:3.10.6
# Base docker image of python 3.x

RUN pip install --upgrade pip
# Upgrade pip package

WORKDIR /app

COPY ../ /app
# Change working dir to app
RUN pip install -r requirements.txt
ENV JWT_SECRET='damg_7225_big_data'
EXPOSE 8000


CMD ["uvicorn", "main:app", "--port", "8000","--reload","--host", "0.0.0.0"]