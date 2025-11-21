-- STAGING SCHEMA
CREATE TABLE staging.staging_events (
    artist character varying(256),
    auth character varying(50),
    firstname character varying(256),
    gender character varying(1),
    iteminsession integer,
    lastname character varying(256),
    length numeric(10, 5),
    level character varying(10),
    location character varying(256),
    method character varying(10),
    page character varying(50),
    registration bigint,
    sessionid integer,
    song character varying(256),
    status integer,
    ts bigint,
    useragent character varying(512),
    userid integer
);

CREATE TABLE staging.staging_songs (
    song_id character varying(65535),
    num_songs integer,
    title character varying(65535),
    artist_name character varying(65535),
    artist_latitude numeric(10, 6),
    year integer,
    duration numeric(10, 5),
    artist_id character varying(65535),
    artist_longitude numeric(10, 6),
    artist_location character varying(65535)
);

-- AUDIT SCHEMA
CREATE TABLE audit.fct_songplays (
    songplay_id character varying(32) NOT NULL,
    start_time timestamp without time zone NOT NULL
	user_id integer NOT NULL,
	level character varying(10),
	song_id character varying(256),
	artist_id character varying(256),
	session_id integer,
	artist_name character varying(255),
	location character varying(512),
	user_agent character varying(512),
	PRIMARY KEY (songplay_id)
);

CREATE TABLE audit.dim_songs (
    song_id character varying(65535) NOT NULL,
	title character varying(65535),
	artist_id character varying(65535) NOT NULL,
	year integer,
	duration numeric(10, 5),
	PRIMARY KEY (song_id)
);

CREATE TABLE audit.dim_artists (
    artist_sk character varying(32) NOT NULL,
    artist_id character varying(65535) NOT NULL,
	name character varying(65535),
	location character varying(65535),
	latitude numeric(10, 6),
	longitude numeric(10, 6),
	PRIMARY KEY (artist_id)
);

CREATE TABLE audit.dim_users (
    user_id integer NOT NULL,
	first_name character varying(65535),
	last_name character varying(65535),
	gender character varying(65535),
	level character varying(65535),
	PRIMARY KEY (user_id)
);

CREATE TABLE audit.dim_times (
    start_time timestamp without time zone NOT NULL,
	hour integer,
	day integer,
	week integer,
	month integer,
	year integer,
	weekday integer,
	PRIMARY KEY (start_time)
);

-- PROD SCHEMA
CREATE TABLE prod.fct_songplays (
    songplay_id character varying(32) NOT NULL,
    start_time timestamp without time zone NOT NULL
	user_id integer NOT NULL,
	level character varying(10),
	song_id character varying(256),
	artist_id character varying(256),
	session_id integer,
	artist_name character varying(255),
	location character varying(512),
	user_agent character varying(512),
	PRIMARY KEY (songplay_id)
);

CREATE TABLE prod.dim_songs (
    song_id character varying(65535) NOT NULL,
	title character varying(65535),
	artist_id character varying(65535) NOT NULL,
	year integer,
	duration numeric(10, 5),
	PRIMARY KEY (song_id)
);

CREATE TABLE prod.dim_artists (
    artist_sk character varying(32) NOT NULL,
    artist_id character varying(65535) NOT NULL,
	name character varying(65535),
	location character varying(65535),
	latitude numeric(10, 6),
	longitude numeric(10, 6),
	PRIMARY KEY (artist_id)
);

CREATE TABLE prod.dim_users (
    user_id integer NOT NULL,
	first_name character varying(65535),
	last_name character varying(65535),
	gender character varying(65535),
	level character varying(65535),
	PRIMARY KEY (user_id)
);

CREATE TABLE prod.dim_times (
    start_time timestamp without time zone NOT NULL,
	hour integer,
	day integer,
	week integer,
	month integer,
	year integer,
	weekday integer,
	PRIMARY KEY (start_time)
);