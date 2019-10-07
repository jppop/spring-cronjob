DROP TABLE person IF EXISTS;

CREATE TABLE person  (
    person_id BIGINT IDENTITY NOT NULL PRIMARY KEY,
    first_name VARCHAR(20),
    last_name VARCHAR(20),
    email VARCHAR(40),
);
