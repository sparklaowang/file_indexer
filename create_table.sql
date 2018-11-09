CREATE TABLE file_index_meta(
	id SERIAL PRIMARY KEY,
	sha INTEGER,
	name VARCHAR(64) NOT NULL,
	trademark VARCHAR(64) ,
	modytime TIMESTAMP,
	dir VARCHAR(256),
	filetype VARCHAR(16),
	tags INTEGER[]
);


