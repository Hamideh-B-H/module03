import psycopg2
from typing import Any, List, Tuple, Optional

# --- SECTION 1: CONNECTION SETUP ---

def connect_db() -> Any:
    return psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="Eh123456?",
        host="localhost",
        port="5432"
    )

# --- SECTION 2: THE 10 MENU METHODS ---

def print_total_films(cursor: Any) -> None:
    query = "SELECT COUNT(*) FROM movies;"
    cursor.execute(query)
    result = cursor.fetchone()
    print(f"\n1. Total films in database: {result[0]}") # fetchone() always returns a tuple so:result[0] = first element.

def print_role_counts(cursor: Any) -> None:
    query = """
        SELECT 
            (SELECT COUNT(DISTINCT person_id) FROM movie_actors) AS actors,
            (SELECT COUNT(DISTINCT person_id) FROM movie_writers) AS writers,
            (SELECT COUNT(DISTINCT person_id) FROM movie_directors) AS directors;
    """
    cursor.execute(query)
    act, writ, circ = cursor.fetchone()
    print(f"\n2. Unique Persons Statistics:")
    print(f"   - Actors: {act}")
    print(f"   - Writers: {writ}")
    print(f"   - Directors: {circ}")

def print_genres_and_counts(cursor: Any) -> None:
    query = """
        SELECT g.genre_name, COUNT(mg.movie_id) as movie_count
        FROM genres g
        JOIN movie_genres mg ON g.genre_id = mg.genre_id
        GROUP BY g.genre_name
        ORDER BY movie_count DESC;
    """
    cursor.execute(query)
    results = cursor.fetchall()     # gives us list of tuples
    print("\n3. Films per Genre (Most Frequent First):")
    for row in results:
        print(f"   - {row[0]}: {row[1]} films")

def print_most_active_director(cursor: Any) -> None:
    query = """
        SELECT p.full_name, COUNT(md.movie_id) as movie_count
        FROM persons p
        JOIN movie_directors md ON p.person_id = md.person_id
        GROUP BY p.full_name
        ORDER BY movie_count DESC
        LIMIT 1;
    """
    cursor.execute(query)
    res = cursor.fetchone()
    if res:
        print(f"\n4. Most Active Director: {res[0]} ({res[1]} films)")

def print_lengths_per_rating(cursor: Any) -> None:
    query = """
        SELECT r.rating_name, MIN(m.runtime), MAX(m.runtime)
        FROM movies m
        JOIN content_ratings r ON m.rating_id = r.rating_id
        WHERE m.runtime IS NOT NULL
        GROUP BY r.rating_name;
    """
    cursor.execute(query)
    results: List[Tuple[str, int, int]] = cursor.fetchall()
    print("\n5. Shortest and Longest Films per Rating:")
    for row in results:
        print(f"   - {row[0]}: Min {row[1]} min, Max {row[2]} min")

def print_films_per_company(cursor: Any) -> None:
    query = """
        SELECT c.company_name, COUNT(m.movie_id) as movie_count
        FROM production_companies c
        JOIN movies m ON c.company_id = m.company_id
        GROUP BY c.company_name
        ORDER BY movie_count DESC;
    """
    cursor.execute(query)
    print("\n6. Films Produced per Company:")
    for row in cursor.fetchall():
        print(f"   - {row[0]}: {row[1]} films")

def print_fast_to_streaming(cursor: Any) -> None:
    query = """
        SELECT movie_title, original_release_date, streaming_release_date
        FROM movies
        WHERE streaming_release_date <= original_release_date + INTERVAL '12 months';
    """
    cursor.execute(query)
    print("\n7. Films Released on Streaming Within 12 Months:")
    for row in cursor.fetchall():
        print(f"   - {row[0]} (Cinema: {row[1]}, Streaming: {row[2]})")

def print_review_toppers(cursor: Any) -> None:
    query = """
        SELECT year, movie_title, total_reviews
        FROM (
            SELECT 
                EXTRACT(YEAR FROM original_release_date) as year,
                movie_title,
                (top_critics_count + fresh_critics_count + rotten_critics_count) as total_reviews,
                ROW_NUMBER() OVER(
                    PARTITION BY EXTRACT(YEAR FROM original_release_date) 
                    ORDER BY (top_critics_count + fresh_critics_count + rotten_critics_count) DESC
                ) as rank
            FROM movies m
            JOIN movie_metrics mm ON m.movie_id = mm.movie_id
            WHERE original_release_date IS NOT NULL
        ) ranked
        WHERE rank = 1
        ORDER BY year DESC;
    """
    cursor.execute(query)
    print("\n8. Yearly Review Toppers (Highest Combined Critic Count):")
    for row in cursor.fetchall():
        print(f"   - Year {int(row[0])}: {row[1]} ({int(row[2])} reviews)")

def print_duplicate_titles(cursor: Any) -> None:
    query = """
        SELECT movie_title, COUNT(*) as occurrence
        FROM movies
        GROUP BY movie_title
        HAVING COUNT(*) > 1
        ORDER BY occurrence DESC;
    """
    cursor.execute(query)
    print("\n9. Film Titles Occurring More Than Once:")
    for row in cursor.fetchall():
        print(f"   - '{row[0]}' appears {row[1]} times")

def print_actor_and_director(cursor: Any) -> None:
    query = """
        SELECT full_name FROM persons
        WHERE person_id IN (SELECT person_id FROM movie_actors)
        AND person_id IN (SELECT person_id FROM movie_directors)
        ORDER BY full_name;
    """
    cursor.execute(query)
    print("\n10. Multi-talented Persons (Both Actor and Director):")
    for row in cursor.fetchall():
        print(f"    - {row[0]}")

# --- SECTION 3: MENU INTERFACE ---

def display_menu() -> None:
    conn = connect_db()
    cur = conn.cursor()

    while True:
        # Menu is printed at the TOP of every loop iteration
        print("\n" + "="*40)
        print("   Rotten Tomatoes Database Menu")
        print("="*40)
        print("  1.  Total number of films")
        print("  2.  Number of actors, writers, and directors")
        print("  3.  Genres and counts (sorted)")
        print("  4.  Most active director")
        print("  5.  Shortest/Longest films per rating")
        print("  6.  Films produced per company")
        print("  7.  Fast to streaming (within 12 months)")
        print("  8.  Review toppers per year")
        print("  9.  Duplicate film titles")
        print("  10. Persons who are both actor and director")
        print("  0.  Exit")
        print("="*40)

        # Input prompt appears right after the menu every time
        choice = input("  Select an option (0-10): ")

        if choice == "1":
            print_total_films(cur)
        elif choice == "2":
            print_role_counts(cur)
        elif choice == "3":
            print_genres_and_counts(cur)
        elif choice == "4":
            print_most_active_director(cur)
        elif choice == "5":
            print_lengths_per_rating(cur)
        elif choice == "6":
            print_films_per_company(cur)
        elif choice == "7":
            print_fast_to_streaming(cur)
        elif choice == "8":
            print_review_toppers(cur)
        elif choice == "9":
            print_duplicate_titles(cur)
        elif choice == "10":
            print_actor_and_director(cur)
        elif choice == "0":
            print("\nExiting... Goodbye!")
            break
        else:
            print("\n  Invalid choice. Please select a number between 0 and 10.")

        input("\n  Press Enter to return to the menu...")

    cur.close()
    conn.close()

if __name__ == "__main__":
    display_menu()