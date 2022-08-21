CREATE TABLE db_openaq_michiel.measurements (
	id INT UNSIGNED auto_increment NOT NULL,
	sqs_message_id varchar(100) NOT NULL,
	date_utc DATETIME NOT NULL,
	date_local varchar(100) NOT NULL,
	`parameter` varchar(20) NOT NULL,
	value DOUBLE NOT NULL,
	unit varchar(20) NOT NULL,
	latitude DOUBLE NULL,
	longitude DOUBLE NULL,
	location varchar(100) NOT NULL,
	city varchar(100) NOT NULL,
	country varchar(10) NOT NULL,
	source_name varchar(100) NOT NULL,
	source_type varchar(100) NOT NULL,
	mobile BOOL NULL,
	averaging_period_value FLOAT NULL,
	averaging_period_unit varchar(20) NULL,
	CONSTRAINT measurements_PK PRIMARY KEY (id)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_general_ci;
CREATE UNIQUE INDEX measurements_sqs_message_id_IDX USING BTREE ON db_openaq_michiel.measurements (sqs_message_id);
CREATE INDEX measurements_date_utc_IDX USING BTREE ON db_openaq_michiel.measurements (date_utc);

CREATE TABLE db_openaq_michiel.attributions (
	id INT UNSIGNED auto_increment NOT NULL,
	measurement_id INT UNSIGNED NOT NULL,
	name varchar(100) NOT NULL,
	url varchar(255) NULL,
	CONSTRAINT attributions_PK PRIMARY KEY (id),
	CONSTRAINT attributions_FK FOREIGN KEY (measurement_id) REFERENCES db_openaq_michiel.measurements(id)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_general_ci;
CREATE INDEX attributions_measurement_id_IDX USING BTREE ON db_openaq_michiel.attributions (measurement_id);
CREATE INDEX attributions_name_IDX USING BTREE ON db_openaq_michiel.attributions (name);
