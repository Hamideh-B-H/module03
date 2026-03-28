import csv
import psycopg2
from typing import Any, Optional, List, Tuple


# --- SECTION 1: DATABASE CONNECTION ---

def connect_db() -> Any:
    """
    Establishes a connection to the PostgreSQL database.
    """
    return psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="Eh123456?",
        host="localhost",
        port="5432"
    )


# --- SECTION 2: DATA CLEANING functions ---

def clean_string(value: str) -> Optional[str]:
    """
    Handles empty CSV cells and returns None (SQL NULL) if the value is empty.
    Requirement in the instruction: You may NOT modify the input file; cleaning the happens here .
    """
    if not value or value.strip() == "":
        return None
    return value.strip()


def clean_numeric(value: str) -> Optional[float]:
    """
    Converts CSV strings to numbers, returning None if empty.
    Handles '97.0' format seen in the source data.
    """
    cleaned = clean_string(value)
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


# --- SECTION 3: NORMALIZATION functions (1NF & 2NF) ---

def get_or_create_id(cursor: Any, table: str, id_col: str, name_col: str, value: str) -> int:
    """
    Ensures data only exists once in lookup tables (2NF).
    Uses the RETURNING clause to get the SERIAL ID assigned by PostgreSQL.
    """
    # 1. Check if the name already exists
    cursor.execute(f"SELECT {id_col} FROM {table} WHERE {name_col} = %s", (value,))
    result: Optional[Tuple[int]] = cursor.fetchone()
    if result:
        return result

    # 2. If not, insert it and return the new auto-generated ID
    cursor.execute(f"INSERT INTO {table} ({name_col}) VALUES (%s) RETURNING {id_col}", (value,))
    new_id_row: Tuple[int] = cursor.fetchone()
    return new_id_row


def link_multivalued_entities(cursor: Any, movie_id: int, csv_string: str,
                              entity_table: str, junction_table: str,
                              id_col: str, name_col: str) -> None:
    """
    Splits comma-separated strings (like actors) and links them (1NF).
    This populates the many-to-many junction tables we have designed.
    """
    if not csv_string:
        return

    # Split "Actor 1, Actor 2" into a clean Python list
    items: List[str] = [i.strip() for i in csv_string.split(',') if i.strip()]

    for item in items:
        # Get or create the unique person/genre ID
        entity_id = get_or_create_id(cursor, entity_table, id_col, name_col, item)
        # Link the movie to the entity in the junction table
        # ON CONFLICT ensures we don't crash if the same person is listed twice for one movie
        cursor.execute(
            f"INSERT INTO {junction_table} (movie_id, {id_col}) VALUES (%s, %s) "
            f"ON CONFLICT DO NOTHING", (movie_id, entity_id)
        )


# --- SECTION 4: MAIN LOADING LOOP ---

def load_data(file_path: str) -> None:
    """
    Reads the CSV and populates the 10-table normalized database.
    Requirement: Results in a fully populated database for 17,712 films.
    """
    conn = connect_db()
    cur = conn.cursor()

    try:
        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                # 1. Handle Entity Tables (Production Company and Content Rating)
                rating_name = clean_string(row['content_rating'])
                rating_id = get_or_create_id(cur, "content_ratings", "rating_id",
                                             "rating_name", rating_name) if rating_name else None

                comp_name = clean_string(row['production_company'])
                company_id = get_or_create_id(cur, "production_companies", "company_id",
                                              "company_name", comp_name) if comp_name else None

                # 2. Insert into Main 'movies' table
                # This uses the Foreign Keys (rating_id, company_id) we just found
                cur.execute("""
                            INSERT INTO movies (rt_link, movie_title, movie_info, critics_consensus,
                                                runtime, original_release_date, streaming_release_date,
                                                company_id, rating_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING movie_id
                            """, (
                                row['rotten_tomatoes_link'],
                                row['movie_title'],
                                clean_string(row['movie_info']),
                                clean_string(row['critics_consensus']),
                                clean_numeric(row['runtime']),
                                clean_string(row['original_release_date']),
                                clean_string(row['streaming_release_date']),
                                company_id,
                                rating_id
                            ))
                movie_id_row: Tuple[int] = cur.fetchone()
                movie_id = movie_id_row

                # 3. Insert into 'movie_metrics' (3NF Vertical Partition)
                cur.execute("""
                            INSERT INTO movie_metrics (movie_id, tomatometer_status, tomatometer_rating,
                                                       tomatometer_count, audience_status, audience_rating,
                                                       audience_count, top_critics_count, fresh_critics_count,
                                                       rotten_critics_count)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (
                                movie_id,
                                clean_string(row['tomatometer_status']),
                                clean_numeric(row['tomatometer_rating']),
                                clean_numeric(row['tomatometer_count']),
                                clean_string(row['audience_status']),
                                clean_numeric(row['audience_rating']),
                                clean_numeric(row['audience_count']),
                                clean_numeric(row['tomatometer_top_critics_count']),
                                clean_numeric(row['tomatometer_fresh_critics_count']),
                                clean_numeric(row['tomatometer_rotten_critics_count'])
                            ))

                # 4. Populate Junction Tables (Splitting multivalued CSV strings)
                link_multivalued_entities(cur, movie_id, row['genres'],
                                          "genres", "movie_genres", "genre_id", "genre_name")
                link_multivalued_entities(cur, movie_id, row['actors'],
                                          "persons", "movie_actors", "person_id", "full_name")
                link_multivalued_entities(cur, movie_id, row['directors'],
                                          "persons", "movie_directors", "person_id", "full_name")
                link_multivalued_entities(cur, movie_id, row['authors'],
                                          "persons", "movie_writers", "person_id", "full_name")

        # Permanent save
        conn.commit()
        print(f"Successfully loaded {reader.line_num - 1} films into the database.")

    except Exception as e:
        conn.rollback()
        print(f"Error loading data: {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    load_data('rotten_tomatoes_movies.csv')