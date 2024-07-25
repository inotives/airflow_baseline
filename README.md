# data_collectors

This is an Airflow project that bootstraps some commonly used libraries in data-related tasks, such as pandas, numpy, and BeautifulSoup (bs4). The purpose of this project is to demonstrate how to use Airflow to automate tasks using these libraries while keeping the code DRY by utilizing models and DAG factory functions. All the data are then stored to postgres DB using SQLAlchemy and psycopg2. 


### Setup python work environment 
1. Navigate to Project Root Directory. Open your terminal and navigate to the root directory of your project.
2. Create a virtual environment venv on the project root folder. `python3 -m venv venv`
3. Activate the virtual environment to switch the Python interpreter to the one within the virtual environment. Mac/Linux: `source venv/bin/activate`, Windows: `venv\Scripts\activate`
4. With the virtual environment activated, install the necessary libraries and packages listed in the requirements.txt file. `pip install -r requirements.txt`
5. [Optional] When you are done working in the virtual environment, you can deactivate it using: `deactivate`


### Setup Airflow 

- build the airflow image along with extended python packages dependecies using requirements.txt
```
docker build . --tag extending_airflow:latest
```
- If you want to change the postgres db username and password, you can change them in docker-compose.yaml file line 89 and 90. then need to change line 58 for the connection using SQLalchemy. 
- Run the docker airflow container on the backend.
```
docker compose up -d
```

### In case you need to rebuild the docker image with additional packages, follow the step below

- remove all docker containers, images and volume for this project. 
```
docker compose down --rmi local -v
```

- next, add the packages needed into requirements.txt
- then rebuild the docker image 
```
docker build . --tag extending_airflow:latest
```

### Setting POSTGRES Connection in Airflow UI
- If you are using dockerize version of airflow and if you wan to use the postgresdb that come with airflow, you can set up the postgres connection in the airflow ui
- One thing to take note of would the on the host, you cant use localhost since you are accessing the dockerize postgres in another docker container. Hence on the host name, you will need to use the alias of the postgres container e.q. `dag_basedry-postgres-1`


### running backfill for data

- First look for airflow-scheduler container in docker 
- Go into airflow-scheduler container bash
```
docker exec -it <container_id> bash
```
- run the airflow backfill command
```
airflow dags backfill -s 2024-01-01 -e 2024-01-31 <dag_id>
```