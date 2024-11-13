DROP TABLE IF EXISTS long_process_data;

CREATE TABLE long_process_data(
    seq_num SERIAL PRIMARY KEY,
    execution_num INT NOT NULL,
    sub_process_desc VARCHAR(255) NOT NULL,
    record_create_dtm TIMESTAMP NOT NULL,
    record_create_username VARCHAR(255) NOT NULL
);