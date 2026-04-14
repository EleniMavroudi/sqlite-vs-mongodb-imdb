# Βασικός έλεγχος ορθής λειτουργίας της βάσης MongoDB
# Το script ελέγχει ότι υπάρχει σύνδεση με τη MongoDB
# και ότι η βάση της εργασίας περιέχει τις βασικές συλλογές και εγγραφές.

from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "imdb_mongodb_db"


def main():
    # Σύνδεση με τον τοπικό MongoDB server.
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    try:
        print("=" * 70)
        print("ΕΛΕΓΧΟΣ ΣΥΝΔΕΣΗΣ ΜΕ ΤΗ MONGODB")
        print("=" * 70)

        print("Σύνδεση επιτυχής.")
        print("Όνομα βάσης:", db.name)

        print("\nΣυλλογές της βάσης:")
        print(db.list_collection_names())

        print("\nΠλήθος εγγράφων:")
        print("movies:", db.movies.count_documents({}))
        print("actors:", db.actors.count_documents({}))
        print("directors:", db.directors.count_documents({}))

    finally:
        client.close()


if __name__ == "__main__":
    main()