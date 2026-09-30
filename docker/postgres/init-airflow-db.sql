-- Airflow needs its own metadata database, separate from the `smartgrid`
-- application database. Created here so a single Postgres container can
-- serve both (keeps docker-compose.yml to one DB service for the
-- mini-project's scope).
CREATE DATABASE airflow OWNER smartgrid;
