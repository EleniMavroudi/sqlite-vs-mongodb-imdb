# Βασικός έλεγχος ορθής λειτουργίας της βάσης SQLite
# Το script εκτελεί δύο δοκιμαστικά ερωτήματα,
# ώστε να επιβεβαιωθεί ότι οι πίνακες και οι συνδέσεις λειτουργούν σωστά.

import sqlite3
from pathlib import Path

DB_FILE = Path(__file__).resolve().parent / "movies_sqlite.db"


def main():
    # Σύνδεση με τη βάση SQLite.
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    try:
        print("=" * 70)
        print("1. ΤΑΙΝΙΕΣ ΤΟΥ JAMES CAMERON (ΩΣ ΣΚΗΝΟΘΕΤΗ)")
        print("=" * 70)

        # Έλεγχος σωστής σύνδεσης ταινιών και σκηνοθετών.
        rows = cur.execute("""
            SELECT
                m.primaryTitle,
                m.startYear,
                m.averageRating,
                m.numVotes
            FROM movies m
            JOIN movie_directors md ON m.tconst = md.tconst
            JOIN directors d ON md.nconst = d.nconst
            WHERE d.primaryName = 'James Cameron'
            ORDER BY m.startYear, m.primaryTitle;
        """).fetchall()

        print(f"Βρέθηκαν {len(rows)} ταινίες.\n")
        for row in rows[:50]:
            print(row)

        print("\n" + "=" * 70)
        print("2. ΤΑΙΝΙΕΣ ΜΕ ROBERT PATTINSON ΠΟΥ ΑΝΗΚΟΥΝ ΣΤΟ ΕΙΔΟΣ ROMANCE")
        print("=" * 70)

        # Έλεγχος σωστής σύνδεσης ταινιών και ηθοποιών,
        # μαζί με απλό φίλτρο ως προς το είδος.
        rows = cur.execute("""
            SELECT
                m.primaryTitle,
                m.startYear,
                m.genres,
                m.averageRating,
                m.numVotes
            FROM movies m
            JOIN movie_actors ma ON m.tconst = ma.tconst
            JOIN actors a ON ma.nconst = a.nconst
            WHERE a.primaryName = 'Robert Pattinson'
              AND m.genres LIKE '%Romance%'
            ORDER BY m.startYear, m.primaryTitle;
        """).fetchall()

        print(f"Βρέθηκαν {len(rows)} ταινίες.\n")
        for row in rows[:50]:
            print(row)

    finally:
        conn.close()


if __name__ == "__main__":
    main()