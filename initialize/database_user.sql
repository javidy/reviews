-- DROP DATABASE IF EXISTS dwh;
-- DROP USER IF EXISTS dwh_user;
-- CREATE USER dwh_user PASSWORD 'dwh_user';

-- -- Create data warehouse db
-- CREATE DATABASE dwh;
-- \c dwh;

CREATE SCHEMA dwh AUTHORIZATION dwh_user;
CREATE SCHEMA staging AUTHORIZATION dwh_user;
ALTER ROLE dwh_user SET work_mem TO '2GB';