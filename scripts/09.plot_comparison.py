# Δημιουργία του βασικού διαγράμματος σύγκρισης SQLite και MongoDB
# Το script διαβάζει το συγκριτικό αρχείο αποτελεσμάτων
# και παράγει το συνολικό ραβδόγραμμα για τα ερωτήματα Q1-Q7.

import csv
from pathlib import Path
import matplotlib.pyplot as plt

BASE_DIR = Path(__file__).resolve().parent
IN_CSV = BASE_DIR / "comparison_results.csv"
OUT_PNG = BASE_DIR / "comparison_chart.png"


def main():
    # Έλεγχος ότι υπάρχει το συγκριτικό αρχείο αποτελεσμάτων.
    if not IN_CSV.exists():
        raise FileNotFoundError(f"Δεν βρέθηκε το αρχείο: {IN_CSV}")

    query_ids = []
    sqlite_times = []
    mongodb_times = []

    # Ανάγνωση των μέσων χρόνων από το comparison_results.csv.
    with open(IN_CSV, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            query_ids.append(row["query_id"])
            sqlite_times.append(float(row["sqlite_avg_time_sec"]))
            mongodb_times.append(float(row["mongodb_avg_time_sec"]))

    x = range(len(query_ids))
    width = 0.35

    # Δημιουργία του συνολικού ραβδογράμματος.
    plt.figure(figsize=(10, 6))
    plt.bar([i - width/2 for i in x], sqlite_times, width=width, label="SQLite")
    plt.bar([i + width/2 for i in x], mongodb_times, width=width, label="MongoDB")

    plt.xticks(list(x), query_ids)
    plt.xlabel("Queries")
    plt.ylabel("Μέσος χρόνος εκτέλεσης (s)")
    plt.title("Σύγκριση μέσου χρόνου εκτέλεσης SQLite και MongoDB")
    plt.legend()
    plt.tight_layout()

    # Αποθήκευση του διαγράμματος σε αρχείο PNG.
    plt.savefig(OUT_PNG, dpi=300)
    plt.show()

    print(f"Το διάγραμμα αποθηκεύτηκε στο: {OUT_PNG.name}")


if __name__ == "__main__":
    main()