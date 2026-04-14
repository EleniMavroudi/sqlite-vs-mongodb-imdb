# Μετρήσεις απόδοσης των ερωτημάτων στη βάση MongoDB
# Το script εκτελεί τα βασικά ερωτήματα της εργασίας,
# μετρά τον χρόνο εκτέλεσής τους και αποθηκεύει τα αποτελέσματα σε CSV.

import csv
import time
from pathlib import Path
from statistics import mean
from pymongo import MongoClient

BASE_DIR = Path(__file__).resolve().parent
OUT_CSV = BASE_DIR / "mongodb_results.csv"

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "imdb_mongodb_db"

REPEATS = 5


def q1(db):
    return list(db.movies.find(
        {"genres": "Comedy"},
        {"_id": 0, "tconst": 1, "primaryTitle": 1, "startYear": 1, "genres": 1}
    ))


def q2(db):
    return list(db.movies.find(
        {"startYear": {"$gte": 2000, "$lte": 2010}},
        {"_id": 0, "tconst": 1, "primaryTitle": 1, "startYear": 1}
    ))


def q3(db):
    return list(db.movies.find(
        {"averageRating": {"$gt": 8.0}, "numVotes": {"$gt": 100000}},
        {"_id": 0, "tconst": 1, "primaryTitle": 1, "startYear": 1, "averageRating": 1, "numVotes": 1}
    ))


def q4(db):
    return list(db.movies.find(
        {
            "actors.primaryName": "Robert Pattinson",
            "genres": "Romance"
        },
        {"_id": 0, "tconst": 1, "primaryTitle": 1, "startYear": 1, "genres": 1}
    ))


def q5(db):
    return list(db.movies.find(
        {
            "directors.primaryName": "James Cameron",
            "startYear": {"$gt": 1990}
        },
        {"_id": 0, "tconst": 1, "primaryTitle": 1, "startYear": 1}
    ))


def q6(db):
    pipeline = [
        {"$unwind": "$genres"},
        {"$group": {"_id": "$genres", "movie_count": {"$sum": 1}}},
        {"$sort": {"movie_count": -1, "_id": 1}}
    ]
    return list(db.movies.aggregate(pipeline))


def q7(db):
    pipeline = [
        {"$match": {"averageRating": {"$ne": None}}},
        {"$unwind": "$genres"},
        {
            "$group": {
                "_id": "$genres",
                "avg_rating": {"$avg": "$averageRating"}
            }
        },
        {"$sort": {"avg_rating": -1, "_id": 1}}
    ]
    return list(db.movies.aggregate(pipeline))


QUERIES = [
    ("Q1", "Ταινίες είδους Comedy", q1),
    ("Q2", "Ταινίες από το 2000 έως το 2010", q2),
    ("Q3", "Ταινίες με βαθμολογία > 8.0 και ψήφους > 100000", q3),
    ("Q4", "Ταινίες του Robert Pattinson με είδος Romance", q4),
    ("Q5", "Ταινίες του James Cameron μετά το 1990", q5),
    ("Q6", "Πλήθος ταινιών ανά είδος", q6),
    ("Q7", "Μέσος όρος βαθμολογίας ανά είδος", q7),
]


# Εκτέλεση ενός ερωτήματος και μέτρηση του χρόνου εκτέλεσής του.
def run_query(func, db):
    start = time.perf_counter()
    rows = func(db)
    end = time.perf_counter()
    elapsed = end - start
    return rows, elapsed


def main():
    # Σύνδεση με τη βάση MongoDB.
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    results = []

    try:
        # Εκτέλεση όλων των ερωτημάτων του benchmark.
        for query_id, description, func in QUERIES:
            print("=" * 80)
            print(f"{query_id} - {description}")

            timings = []
            row_count = None

            # Επαναλαμβάνουμε κάθε ερώτημα για πιο αξιόπιστη μέτρηση.
            for i in range(REPEATS):
                rows, elapsed = run_query(func, db)
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
                "query_id": query_id,
                "description": description,
                "row_count": row_count,
                "avg_time_sec": f"{avg_time:.6f}",
                "min_time_sec": f"{min_time:.6f}",
                "max_time_sec": f"{max_time:.6f}",
                "repeats": REPEATS,
            })

    finally:
        client.close()

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