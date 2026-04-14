# Δημιουργία της σχεσιακής βάσης SQLite από τα τελικά καθαρά αρχεία CSV
# Το script δημιουργεί τους πίνακες, εισάγει τα δεδομένα
# και ορίζει βασικά ευρετήρια για τα ερωτήματα της εργασίας.

import csv
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

MOVIES_CSV = BASE_DIR / "movies.csv"
ACTORS_CSV = BASE_DIR / "actors.csv"
MOVIE_ACTORS_CSV = BASE_DIR / "movie_actors.csv"
DIRECTORS_CSV = BASE_DIR / "directors.csv"
MOVIE_DIRECTORS_CSV = BASE_DIR / "movie_directors.csv"

DB_FILE = BASE_DIR / "movies_sqlite.db"


# Μετατροπή τιμών σε ακέραιο, όπου αυτό είναι δυνατό.
def clean_int(value):
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


# Μετατροπή τιμών σε δεκαδικό αριθμό, όπου αυτό είναι δυνατό.
def clean_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def create_tables(conn):
    cur = conn.cursor()

    # Διαγραφή παλιών πινάκων, αν υπάρχουν, ώστε η βάση να δημιουργείται από την αρχή.
    cur.execute("DROP TABLE IF EXISTS movie_directors;")
    cur.execute("DROP TABLE IF EXISTS directors;")
    cur.execute("DROP TABLE IF EXISTS movie_actors;")
    cur.execute("DROP TABLE IF EXISTS actors;")
    cur.execute("DROP TABLE IF EXISTS movies;")

    # Δημιουργία βασικού πίνακα ταινιών.
    cur.execute("""
        CREATE TABLE movies (
            tconst TEXT PRIMARY KEY,
            primaryTitle TEXT,
            originalTitle TEXT,
            isAdult INTEGER,
            startYear INTEGER,
            runtimeMinutes INTEGER,
            genres TEXT,
            averageRating REAL,
            numVotes INTEGER
        );
    """)

    # Δημιουργία πίνακα ηθοποιών.
    cur.execute("""
        CREATE TABLE actors (
            nconst TEXT PRIMARY KEY,
            primaryName TEXT
        );
    """)

    # Δημιουργία πίνακα συσχέτισης ταινιών - ηθοποιών.
    cur.execute("""
        CREATE TABLE movie_actors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tconst TEXT,
            nconst TEXT,
            ordering_num INTEGER,
            category TEXT,
            FOREIGN KEY (tconst) REFERENCES movies(tconst),
            FOREIGN KEY (nconst) REFERENCES actors(nconst)
        );
    """)

    # Δημιουργία πίνακα σκηνοθετών.
    cur.execute("""
        CREATE TABLE directors (
            nconst TEXT PRIMARY KEY,
            primaryName TEXT
        );
    """)

    # Δημιουργία πίνακα συσχέτισης ταινιών - σκηνοθετών.
    cur.execute("""
        CREATE TABLE movie_directors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tconst TEXT,
            nconst TEXT,
            FOREIGN KEY (tconst) REFERENCES movies(tconst),
            FOREIGN KEY (nconst) REFERENCES directors(nconst)
        );
    """)

    conn.commit()


def import_movies(conn):
    print("1/5 Εισαγωγή movies.csv ...")
    cur = conn.cursor()

    with open(MOVIES_CSV, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            rows.append((
                row["tconst"],
                row["primaryTitle"],
                row["originalTitle"],
                clean_int(row["isAdult"]),
                clean_int(row["startYear"]),
                clean_int(row["runtimeMinutes"]),
                row["genres"],
                clean_float(row["averageRating"]),
                clean_int(row["numVotes"]),
            ))

    cur.executemany("""
        INSERT INTO movies (
            tconst, primaryTitle, originalTitle, isAdult,
            startYear, runtimeMinutes, genres, averageRating, numVotes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, rows)

    conn.commit()
    print(f"  Εισήχθησαν {len(rows)} εγγραφές στον πίνακα movies.")


def import_actors(conn):
    print("2/5 Εισαγωγή actors.csv ...")
    cur = conn.cursor()

    with open(ACTORS_CSV, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = [(row["nconst"], row["primaryName"]) for row in reader]

    cur.executemany("""
        INSERT INTO actors (nconst, primaryName)
        VALUES (?, ?);
    """, rows)

    conn.commit()
    print(f"  Εισήχθησαν {len(rows)} εγγραφές στον πίνακα actors.")


def import_movie_actors(conn):
    print("3/5 Εισαγωγή movie_actors.csv ...")
    cur = conn.cursor()

    with open(MOVIE_ACTORS_CSV, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            rows.append((
                row["tconst"],
                row["nconst"],
                clean_int(row["ordering"]),
                row["category"],
            ))

    cur.executemany("""
        INSERT INTO movie_actors (tconst, nconst, ordering_num, category)
        VALUES (?, ?, ?, ?);
    """, rows)

    conn.commit()
    print(f"  Εισήχθησαν {len(rows)} εγγραφές στον πίνακα movie_actors.")


def import_directors(conn):
    print("4/5 Εισαγωγή directors.csv ...")
    cur = conn.cursor()

    with open(DIRECTORS_CSV, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = [(row["nconst"], row["primaryName"]) for row in reader]

    cur.executemany("""
        INSERT INTO directors (nconst, primaryName)
        VALUES (?, ?);
    """, rows)

    conn.commit()
    print(f"  Εισήχθησαν {len(rows)} εγγραφές στον πίνακα directors.")


def import_movie_directors(conn):
    print("5/5 Εισαγωγή movie_directors.csv ...")
    cur = conn.cursor()

    with open(MOVIE_DIRECTORS_CSV, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = [(row["tconst"], row["nconst"]) for row in reader]

    cur.executemany("""
        INSERT INTO movie_directors (tconst, nconst)
        VALUES (?, ?);
    """, rows)

    conn.commit()
    print(f"  Εισήχθησαν {len(rows)} εγγραφές στον πίνακα movie_directors.")


def create_indexes(conn):
    print("Δημιουργία ευρετηρίων ...")
    cur = conn.cursor()

    # Ευρετήρια σε βασικά πεδία για ταχύτερη αναζήτηση και συσχέτιση.
    cur.execute("CREATE INDEX idx_movies_startYear ON movies(startYear);")
    cur.execute("CREATE INDEX idx_movies_rating ON movies(averageRating);")
    cur.execute("CREATE INDEX idx_movies_numVotes ON movies(numVotes);")
    cur.execute("CREATE INDEX idx_actors_name ON actors(primaryName);")
    cur.execute("CREATE INDEX idx_movie_actors_tconst ON movie_actors(tconst);")
    cur.execute("CREATE INDEX idx_movie_actors_nconst ON movie_actors(nconst);")
    cur.execute("CREATE INDEX idx_directors_name ON directors(primaryName);")
    cur.execute("CREATE INDEX idx_movie_directors_tconst ON movie_directors(tconst);")
    cur.execute("CREATE INDEX idx_movie_directors_nconst ON movie_directors(nconst);")

    conn.commit()
    print("  Τα ευρετήρια δημιουργήθηκαν.")


def main():
    # Έλεγχος ότι υπάρχουν όλα τα απαραίτητα αρχεία εισόδου.
    required_files = [
        MOVIES_CSV, ACTORS_CSV, MOVIE_ACTORS_CSV, DIRECTORS_CSV, MOVIE_DIRECTORS_CSV
    ]
    for file in required_files:
        if not file.exists():
            raise FileNotFoundError(f"Δεν βρέθηκε το αρχείο: {file}")

    print(f"Δημιουργία βάσης: {DB_FILE.name}")
    conn = sqlite3.connect(DB_FILE)

    try:
        # Δημιουργία πινάκων της βάσης.
        create_tables(conn)

        # Εισαγωγή των δεδομένων από τα καθαρά αρχεία CSV.
        import_movies(conn)
        import_actors(conn)
        import_movie_actors(conn)
        import_directors(conn)
        import_movie_directors(conn)

        # Δημιουργία ευρετηρίων για καλύτερη απόδοση των ερωτημάτων.
        create_indexes(conn)

        print("\nΈτοιμο. Η πλήρης βάση SQLite δημιουργήθηκε επιτυχώς.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()