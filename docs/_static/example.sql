CREATE DATABASE dw;
\connect dw

CREATE TABLE book(bookid INTEGER PRIMARY KEY, book TEXT, genre TEXT);
CREATE TABLE time(timeid INTEGER PRIMARY KEY, day INTEGER, month INTEGER, year INTEGER);
CREATE TABLE location(locationid INTEGER PRIMARY KEY, city TEXT, region TEXT);
CREATE TABLE facttable(bookid INTEGER, locationid INTEGER, timeid INTEGER, sale INTEGER, PRIMARY KEY(bookid, locationid, timeid));

CREATE USER dwuser WITH PASSWORD 'dwpass';
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dwuser;
