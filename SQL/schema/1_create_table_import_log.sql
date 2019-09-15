USE rover;

CREATE TABLE IF NOT EXISTS import_log (
    import_log_id integer AUTO_INCREMENT,
	source_file varchar(50) NOT NULL,
	target_host varchar(50) NOT NULL,
    target_db varchar(50) NOT NULL,
    target_table varchar(50) NOT NULL,
	total_row_count integer NOT NULL,
    chunk_row_count integer NOT NULL,
    comments varchar(500) NULL,
    import_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    primary key pk_import_log (import_log_id)
);
