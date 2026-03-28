-- 1. Entity Tables
CREATE TABLE production_companies (
    company_id SERIAL PRIMARY KEY, -- SERIAL for the ID
    company_name TEXT NOT NULL UNIQUE -- to ensure every company has a name and there are no duplicates
);

CREATE TABLE content_ratings (
    rating_id SERIAL PRIMARY KEY,
    rating_name TEXT NOT NULL UNIQUE
);

CREATE TABLE persons (
    person_id SERIAL PRIMARY KEY,
    full_name TEXT NOT NULL UNIQUE
);

CREATE TABLE genres (
    genre_id SERIAL PRIMARY KEY,
    genre_name TEXT NOT NULL UNIQUE
);

-- 2. Main Movies Table (Using NOT NULL only where data is guaranteed)
CREATE TABLE movies (
    movie_id SERIAL PRIMARY KEY,
    rt_link TEXT NOT NULL UNIQUE, -- 0 missing values
    movie_title TEXT NOT NULL,    -- 0 missing values
    movie_info TEXT,              -- 321 missing
    critics_consensus TEXT,       -- 8578 missing
    runtime INTEGER,              -- 314 missing
    original_release_date DATE,   -- 1166 missing
    streaming_release_date DATE,  -- 384 missing
    company_id INTEGER,           -- 499 missing
    rating_id INTEGER,            -- Links to content_ratings
    FOREIGN KEY (company_id) REFERENCES production_companies(company_id),
    FOREIGN KEY (rating_id) REFERENCES content_ratings(rating_id)
);

-- 3. Metrics Partition (1-to-1 with movies)
CREATE TABLE movie_metrics (
    movie_id INTEGER PRIMARY KEY,
    tomatometer_status TEXT,
    tomatometer_rating DECIMAL,
    tomatometer_count INTEGER,
    audience_status TEXT,
    audience_rating DECIMAL,
    audience_count INTEGER,
    top_critics_count INTEGER,
    fresh_critics_count INTEGER,
    rotten_critics_count INTEGER,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);

-- 4. Junction Tables for Many-to-Many Relationships
CREATE TABLE movie_actors (
    movie_id INTEGER,
    person_id INTEGER,
    PRIMARY KEY (movie_id, person_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (person_id) REFERENCES persons(person_id)
);

CREATE TABLE movie_directors (
    movie_id INTEGER,
    person_id INTEGER,
    PRIMARY KEY (movie_id, person_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (person_id) REFERENCES persons(person_id)
);

CREATE TABLE movie_writers (
    movie_id INTEGER,
    person_id INTEGER,
    PRIMARY KEY (movie_id, person_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (person_id) REFERENCES persons(person_id)
);

CREATE TABLE movie_genres (
    movie_id INTEGER,
    genre_id INTEGER,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
);