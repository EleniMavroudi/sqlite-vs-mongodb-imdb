# Δημιουργία της βάσης MongoDB από τα τελικά καθαρά αρχεία CSV
# Το script φορτώνει τα δεδομένα, δημιουργεί τα έγγραφα των ταινιών
# και οργανώνει τις συλλογές movies, actors και directors.

import csv
from pathlib import Path
from pymongo import MongoClient

BASE_DIR = Path(__file__).resolve().parent

MOVIES_CSV = BASE_DIR / "movies.csv"
ACTORS_CSV = BASE_DIR / "actors.csv"
MOVIE_ACTORS_CSV = BASE_DIR / "movie_actors.csv"
DIRECTORS_CSV = BASE_DIR / "directors.csv"
MOVIE_DIRECTORS_CSV = BASE_DIR / "movie_directors.csv"

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "imdb_mongodb_db"


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


def load_actors():
    actors = {}
    with open(ACTORS_CSV, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            actors[row["nconst"]] = {
                "nconst": row["nconst"],
                "primaryName": row["primaryName"],
            }
    return actors


def load_directors():
    directors = {}
    with open(DIRECTORS_CSV, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            directors[row["nconst"]] = {
                "nconst": row["nconst"],
                "primaryName": row["primaryName"],
            }
    return directors


def load_movie_actors():
    movie_actors = {}
    with open(MOVIE_ACTORS_CSV, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tconst = row["tconst"]
            movie_actors.setdefault(tconst, []).append({
                "nconst": row["nconst"],
                "ordering": clean_int(row["ordering"]),
                "category": row["category"],
            })
    return movie_actors


def load_movie_directors():
    movie_directors = {}
    with open(MOVIE_DIRECTORS_CSV, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tconst = row["tconst"]
            movie_directors.setdefault(tconst, []).append({
                "nconst": row["nconst"],
            })
    return movie_directors


# Δημιουργία των τελικών εγγράφων της συλλογής movies
# με ενσωματωμένα στοιχεία για ηθοποιούς και σκηνοθέτες.
def build_movie_documents(actors, directors, movie_actors_map, movie_directors_map):
    movies = []

    with open(MOVIES_CSV, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tconst = row["tconst"]

            actor_docs = []
            for item in movie_actors_map.get(tconst, []):
                nconst = item["nconst"]
                if nconst in actors:
                    actor_docs.append({
                        "nconst": nconst,
                        "primaryName": actors[nconst]["primaryName"],
                        "ordering": item["ordering"],
                        "category": item["category"],
                    })

            director_docs = []
            for item in movie_directors_map.get(tconst, []):
                nconst = item["nconst"]
                if nconst in directors:
                    director_docs.append({
                        "nconst": nconst,
                        "primaryName": directors[nconst]["primaryName"],
                    })

            doc = {
                "tconst": tconst,
                "primaryTitle": row["primaryTitle"],
                "originalTitle": row["originalTitle"],
                "isAdult": clean_int(row["isAdult"]),
                "startYear": clean_int(row["startYear"]),
                "runtimeMinutes": clean_int(row["runtimeMinutes"]),
                "genres": [] if row["genres"] == "" else row["genres"].split(","),
                "averageRating": clean_float(row["averageRating"]),
                "numVotes": clean_int(row["numVotes"]),
                "actors": actor_docs,
                "directors": director_docs,
            }
            movies.append(doc)

    return movies


def main():
    # Έλεγχος ότι υπάρχουν όλα τα απαραίτητα αρχεία εισόδου.
    required_files = [
        MOVIES_CSV, ACTORS_CSV, MOVIE_ACTORS_CSV, DIRECTORS_CSV, MOVIE_DIRECTORS_CSV
    ]
    for file in required_files:
        if not file.exists():
            raise FileNotFoundError(f"Δεν βρέθηκε το αρχείο: {file}")

    # Φόρτωση των βασικών αρχείων προσώπων και σχέσεων.
    print("1/6 Φόρτωση actors.csv ...")
    actors = load_actors()
    print(f"  Φορτώθηκαν {len(actors)} ηθοποιοί.")

    print("2/6 Φόρτωση directors.csv ...")
    directors = load_directors()
    print(f"  Φορτώθηκαν {len(directors)} σκηνοθέτες.")

    print("3/6 Φόρτωση movie_actors.csv ...")
    movie_actors_map = load_movie_actors()
    print(f"  Φορτώθηκαν σχέσεις για {len(movie_actors_map)} ταινίες.")

    print("4/6 Φόρτωση movie_directors.csv ...")
    movie_directors_map = load_movie_directors()
    print(f"  Φορτώθηκαν σχέσεις για {len(movie_directors_map)} ταινίες.")

    # Δημιουργία των τελικών εγγράφων της συλλογής movies.
    print("5/6 Δημιουργία εγγράφων για τη συλλογή movies ...")
    movies = build_movie_documents(actors, directors, movie_actors_map, movie_directors_map)
    print(f"  Δημιουργήθηκαν {len(movies)} έγγραφα ταινιών.")

    # Σύνδεση στη MongoDB και εισαγωγή των δεδομένων.
    print("6/6 Σύνδεση στη MongoDB και εισαγωγή δεδομένων ...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # Διαγραφή παλιών συλλογών, αν υπάρχουν, ώστε η βάση να δημιουργείται από την αρχή.
    db.movies.drop()
    db.actors.drop()
    db.directors.drop()

    # Εισαγωγή δεδομένων στις βασικές συλλογές.
    db.movies.insert_many(movies)
    db.actors.insert_many(list(actors.values()))
    db.directors.insert_many(list(directors.values()))

    # Δημιουργία ευρετηρίων για τα βασικά πεδία αναζήτησης.
    db.movies.create_index("tconst", unique=True)
    db.movies.create_index("primaryTitle")
    db.movies.create_index("startYear")
    db.movies.create_index("averageRating")
    db.movies.create_index("numVotes")
    db.movies.create_index("genres")
    db.movies.create_index("actors.primaryName")
    db.movies.create_index("directors.primaryName")

    db.actors.create_index("nconst", unique=True)
    db.actors.create_index("primaryName")

    db.directors.create_index("nconst", unique=True)
    db.directors.create_index("primaryName")

    print("\nΈτοιμο. Η βάση MongoDB δημιουργήθηκε επιτυχώς.")
    print(f"Βάση: {DB_NAME}")
    print(f"movies: {db.movies.count_documents({})}")
    print(f"actors: {db.actors.count_documents({})}")
    print(f"directors: {db.directors.count_documents({})}")

    client.close()


if __name__ == "__main__":
    main()