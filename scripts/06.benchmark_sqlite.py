# Μετρήσεις απόδοσης των ερωτημάτων στη βάση SQLite
# Το script εκτελεί τα βασικά ερωτήματα της εργασίας,
# μετρά τον χρόνο εκτέλεσής τους και αποθηκεύει τα αποτελέσματα σε CSV.

import csv
import sqlite3
import time
from pathlib import Path
from statistics import mean

BASE_DIR = Path(__file__).resolve().parent
DB_FILE = BASE_DIR / "movies_sqlite.db"
OUT_CSV = BASE_DIR / "sqlite_results.csv"

REPEATS = 5


QUERIES = [
    {
        "id": "Q1",
        "description": "Ταινίες είδους Comedy",
        "sql": """
            SELECT tconst, primaryTitle, startYear, genres
            FROM movies
            WHERE genres LIKE '%Comedy%';
        """
    },
    {
        "id": "Q2",
        "description": "Ταινίες από το 2000 έως το 2010",
        "sql": """
            SELECT tconst, primaryTitle, startYear
            FROM movies
            WHERE startYear BETWEEN 2000 AND 2010;
        """
    },
    {
        "id": "Q3",
        "description": "Ταινίες με βαθμολογία > 8.0 και ψήφους > 100000",
        "sql": """
            SELECT tconst, primaryTitle, startYear, averageRating, numVotes
            FROM movies
            WHERE averageRating > 8.0
              AND numVotes > 100000;
        """
    },
    {
        "id": "Q4",
        "description": "Ταινίες του Robert Pattinson με είδος Romance",
        "sql": """
            SELECT DISTINCT m.tconst, m.primaryTitle, m.startYear, m.genres
            FROM movies m
            JOIN movie_actors ma ON m.tconst = ma.tconst
            JOIN actors a ON ma.nconst = a.nconst
            WHERE a.primaryName = 'Robert Pattinson'
              AND m.genres LIKE '%Romance%';
        """
    },
    {
        "id": "Q5",
        "description": "Ταινίες του James Cameron μετά το 1990",
        "sql": """
            SELECT DISTINCT m.tconst, m.primaryTitle, m.startYear
            FROM movies m
            JOIN movie_directors md ON m.tconst = md.tconst
            JOIN directors d ON md.nconst = d.nconst
            WHERE d.primaryName = 'James Cameron'
              AND m.startYear > 1990;
        """
    },
    {
        "id": "Q6",
        "description": "Πλήθος ταινιών ανά είδος",
        "sql": """
            WITH RECURSIVE split_genres(tconst, genre, rest) AS (
                SELECT
                    tconst,
                    CASE
                        WHEN instr(genres, ',') > 0 THEN substr(genres, 1, instr(genres, ',') - 1)
                        ELSE genres
                    END AS genre,
                    CASE
                        WHEN instr(genres, ',') > 0 THEN substr(genres, instr(genres, ',') + 1)
                        ELSE ''
                    END AS rest
                FROM movies
                WHERE genres IS NOT NULL AND genres <> ''

                UNION ALL

                SELECT
                    tconst,
                    CASE
                        WHEN instr(rest, ',') > 0 THEN substr(rest, 1, instr(rest, ',') - 1)
                        ELSE rest
                    END AS genre,
                    CASE
                        WHEN instr(rest, ',') > 0 THEN substr(rest, instr(rest, ',') + 1)
                        ELSE ''
                    END AS rest
                FROM split_genres
                WHERE rest <> ''
            )
            SELECT genre, COUNT(*) AS movie_count
            FROM split_genres
            WHERE genre <> ''
            GROUP BY genre
            ORDER BY movie_count DESC, genre ASC;
        """
    },
    {
        "id": "Q7",
        "description": "Μέσος όρος βαθμολογίας ανά είδος",
        "sql": """
            WITH RECURSIVE split_genres(tconst, averageRating, genre, rest) AS (
                SELECT
                    tconst,
                    averageRating,
                    CASE
                        WHEN instr(genres, ',') > 0 THEN substr(genres, 1, instr(genres, ',') - 1)
                        ELSE genres
                    END AS genre,
                    CASE
                        WHEN instr(genres, ',') > 0 THEN substr(genres, instr(genres, ',') + 1)
                        ELSE ''
                    END AS rest
                FROM movies
                WHERE genres IS NOT NULL
                  AND genres <> ''
                  AND averageRating IS NOT NULL

                UNION ALL

                SELECT
                    tconst,
                    averageRating,
                    CASE
                        WHEN instr(rest, ',') > 0 THEN substr(rest, 1, instr(rest, ',') - 1)
                        ELSE rest
                    END AS genre,
                    CASE
                        WHEN instr(rest, ',') > 0 THEN substr(rest, instr(rest, ',') + 1)
                        ELSE ''
                    END AS rest
                FROM split_genres
                WHERE rest <> ''
            )
            SELECT genre, ROUND(AVG(averageRating), 4) AS avg_rating
            FROM split_genres
            WHERE genre <> ''
            GROUP BY genre
            ORDER BY avg_rating DESC, genre ASC;
        """
    },
]


# Εκτέλεση ενός ερωτήματος και μέτρηση του χρόνου εκτέλεσής του.
def run_query(cursor, sql):
    start = time.perf_counter()
    rows = cursor.execute(sql).fetchall()
    end = time.perf_counter()
    elapsed = end - start
    return rows, elapsed


def main():
    # Έλεγχος ότι υπάρχει η βάση SQLite.
    if not DB_FILE.exists():
        raise FileNotFoundError(f"Δεν βρέθηκε η βάση: {DB_FILE}")

    # Σύνδεση με τη βάση SQLite.
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    results = []

    try:
        # Εκτέλεση όλων των ερωτημάτων του benchmark.
        for query in QUERIES:
            print("=" * 80)
            print(f"{query['id']} - {query['description']}")

            timings = []
            row_count = None

            # Επαναλαμβάνουμε κάθε ερώτημα για πιο αξιόπιστη μέτρηση.
            for i in range(REPEATS):
                rows, elapsed = run_query(cursor, query["sql"])
                timings.append(elapsed)

                if row_count is None:
                    row_count = len(rows)

                print(f"  Επανάληψη {i+1}: {elapsed:.6f} s | rows = {len(rows)}")

            avg_time = mean(timings)
            min_time = min(timings)
            max_time = max(timings)

            print(f"  ΜΕΣΟΣ ΧΡΟΝΟΣ: {avg_time:.6f} s")
            print(f"  ΕΛΑΧΙΣΤΟΣ ΧΡΟΝΟΣ: {min_time:.6f} s")
            print(f"  ΜΕΓΙΣΤΟΣ ΧΡΟΝΟΣ: {max_time:.6f} s")

            results.append({
                "query_id": query["id"],
                "description": query["description"],
                "row_count": row_count,
                "avg_time_sec": f"{avg_time:.6f}",
                "min_time_sec": f"{min_time:.6f}",
                "max_time_sec": f"{max_time:.6f}",
                "repeats": REPEATS,
            })

    finally:
        conn.close()

    # Αποθήκευση των αποτελεσμάτων σε αρχείο CSV.
    with open(OUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "query_id",
                "description",
                "row_count",
                "avg_time_sec",
                "min_time_sec",
                "max_time_sec",
                "repeats",
            ]
        )
        writer.writeheader()
        writer.writerows(results)

    print("\nΈτοιμο.")
    print(f"Αποθηκεύτηκαν τα αποτελέσματα στο: {OUT_CSV.name}")


if __name__ == "__main__":
    main()