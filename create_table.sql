CREATE TABLE file_index_meta(
	id SERIAL PRIMARY KEY,
	sha INTEGER,
	name VARCHAR(128) NOT NULL,
	trademark VARCHAR(128) ,
	modytime TIMESTAMP,
	dir VARCHAR(512),
	filetype VARCHAR(64),
	filesize INTEGER,
	tags INTEGER[]
);


