# Προετοιμασία και καθαρισμός δεδομένων IMDb
# Κρατάμε μόνο κινηματογραφικές ταινίες (titleType = movie)
# και δημιουργούμε τα τελικά αρχεία CSV για SQLite και MongoDB.
import csv
from pathlib import Path
from collections import defaultdict

BASE_DIR = Path(__file__).resolve().parent

TITLE_BASICS = BASE_DIR / "title.basics.tsv"
TITLE_RATINGS = BASE_DIR / "title.ratings.tsv"
TITLE_PRINCIPALS = BASE_DIR / "title.principals.tsv"
TITLE_CREW = BASE_DIR / "title.crew.tsv"
NAME_BASICS = BASE_DIR / "name.basics.tsv"

OUT_MOVIES = BASE_DIR / "movies.csv"
OUT_ACTORS = BASE_DIR / "actors.csv"
OUT_MOVIE_ACTORS = BASE_DIR / "movie_actors.csv"
OUT_DIRECTORS = BASE_DIR / "directors.csv"
OUT_MOVIE_DIRECTORS = BASE_DIR / "movie_directors.csv"

# Αν ένα πεδίο έχει την ειδική τιμή \N, το μετατρέπουμε σε κενό string.
def clean_value(value: str) -> str:
    return "" if value == r"\N" else value


def read_movies():
    movies = {}
    print("1/5 Διαβάζω title.basics.tsv ...")

    with open(TITLE_BASICS, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            if clean_value(row["titleType"]) != "movie":
                continue

            tconst = clean_value(row["tconst"])
            movies[tconst] = {
                "tconst": tconst,
                "primaryTitle": clean_value(row["primaryTitle"]),
                "originalTitle": clean_value(row["originalTitle"]),
                "isAdult": clean_value(row["isAdult"]),
                "startYear": clean_value(row["startYear"]),
                "runtimeMinutes": clean_value(row["runtimeMinutes"]),
                "genres": clean_value(row["genres"]),
                "averageRating": "",
                "numVotes": "",
            }

    print(f"  Βρέθηκαν {len(movies)} ταινίες.")
    return movies


def add_ratings(movies):
    print("2/5 Διαβάζω title.ratings.tsv ...")

    matched = 0
    with open(TITLE_RATINGS, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            tconst = clean_value(row["tconst"])
            if tconst in movies:
                movies[tconst]["averageRating"] = clean_value(row["averageRating"])
                movies[tconst]["numVotes"] = clean_value(row["numVotes"])
                matched += 1

    print(f"  Προστέθηκαν ratings σε {matched} ταινίες.")


def read_movie_actors(movies):
    print("3/5 Διαβάζω title.principals.tsv ...")

    movie_actor_links = []
    actor_ids_needed = set()
    per_movie_count = defaultdict(int)

    with open(TITLE_PRINCIPALS, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            tconst = clean_value(row["tconst"])
            if tconst not in movies:
                continue

            category = clean_value(row["category"])
            if category not in ("actor", "actress"):
                continue

            if per_movie_count[tconst] >= 3:
                continue

            nconst = clean_value(row["nconst"])
            ordering = clean_value(row["ordering"])

            movie_actor_links.append({
                "tconst": tconst,
                "nconst": nconst,
                "ordering": ordering,
                "category": category,
            })

            actor_ids_needed.add(nconst)
            per_movie_count[tconst] += 1

    print(f"  Κρατήθηκαν {len(movie_actor_links)} σχέσεις ταινίας-ηθοποιού.")
    print(f"  Διαφορετικοί ηθοποιοί που χρειάζονται: {len(actor_ids_needed)}")
    return movie_actor_links, actor_ids_needed


def read_movie_directors(movies):
    print("4/5 Διαβάζω title.crew.tsv ...")

    movie_director_links = []
    director_ids_needed = set()

    with open(TITLE_CREW, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            tconst = clean_value(row["tconst"])
            if tconst not in movies:
                continue

            directors_field = clean_value(row["directors"])
            if not directors_field:
                continue

            director_ids = [x.strip() for x in directors_field.split(",") if x.strip()]
            for nconst in director_ids:
                movie_director_links.append({
                    "tconst": tconst,
                    "nconst": nconst,
                })
                director_ids_needed.add(nconst)

    print(f"  Κρατήθηκαν {len(movie_director_links)} σχέσεις ταινίας-σκηνοθέτη.")
    print(f"  Διαφορετικοί σκηνοθέτες που χρειάζονται: {len(director_ids_needed)}")
    return movie_director_links, director_ids_needed


def read_names(actor_ids_needed, director_ids_needed):
    print("5/5 Διαβάζω name.basics.tsv ...")

    actors = {}
    directors = {}

    all_needed = actor_ids_needed.union(director_ids_needed)

    with open(NAME_BASICS, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            nconst = clean_value(row["nconst"])
            if nconst not in all_needed:
                continue

            primary_name = clean_value(row["primaryName"])

            if nconst in actor_ids_needed:
                actors[nconst] = {
                    "nconst": nconst,
                    "primaryName": primary_name,
                }

            if nconst in director_ids_needed:
                directors[nconst] = {
                    "nconst": nconst,
                    "primaryName": primary_name,
                }

    print(f"  Βρέθηκαν ονόματα για {len(actors)} ηθοποιούς.")
    print(f"  Βρέθηκαν ονόματα για {len(directors)} σκηνοθέτες.")
    return actors, directors


def write_csv(path, fieldnames, rows):
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"  Γράφτηκε το αρχείο: {path.name}")


def main():
    # Έλεγχος ότι υπάρχουν όλα τα απαραίτητα αρχεία εισόδου
    required_files = [
        TITLE_BASICS, TITLE_RATINGS, TITLE_PRINCIPALS, TITLE_CREW, NAME_BASICS
    ]
    for file in required_files:
        if not file.exists():
            raise FileNotFoundError(f"Δεν βρέθηκε το αρχείο: {file}")

    # Ανάγνωση ταινιών και βαθμολογιών
    movies = read_movies()
    add_ratings(movies)

    # Ανάγνωση σχέσεων με ηθοποιούς και σκηνοθέτες
    movie_actor_links, actor_ids_needed = read_movie_actors(movies)
    movie_director_links, director_ids_needed = read_movie_directors(movies)

    # Ανάγνωση ονομάτων προσώπων
    actors, directors = read_names(actor_ids_needed, director_ids_needed)

    # Κρατάμε μόνο συνδέσεις για τις οποίες βρέθηκαν στοιχεία προσώπων
    movie_actor_links = [x for x in movie_actor_links if x["nconst"] in actors]
    movie_director_links = [x for x in movie_director_links if x["nconst"] in directors]

    # Αποθήκευση των τελικών καθαρών αρχείων CSV
    write_csv(
        OUT_MOVIES,
        [
            "tconst", "primaryTitle", "originalTitle", "isAdult",
            "startYear", "runtimeMinutes", "genres",
            "averageRating", "numVotes"
        ],
        movies.values()
    )

    write_csv(
        OUT_ACTORS,
        ["nconst", "primaryName"],
        actors.values()
    )

    write_csv(
        OUT_MOVIE_ACTORS,
        ["tconst", "nconst", "ordering", "category"],
        movie_actor_links
    )

    write_csv(
        OUT_DIRECTORS,
        ["nconst", "primaryName"],
        directors.values()
    )

    write_csv(
        OUT_MOVIE_DIRECTORS,
        ["tconst", "nconst"],
        movie_director_links
    )

    print("\nΈτοιμο. Δημιουργήθηκαν τα τελικά καθαρά αρχεία CSV με στοιχεία ταινιών, ηθοποιών και σκηνοθετών.")


if __name__ == "__main__":
    main()