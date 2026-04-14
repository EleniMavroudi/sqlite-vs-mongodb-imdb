# Δημιουργία του συμπληρωματικού διαγράμματος για τα Q3-Q5
# Το script διαβάζει το συγκριτικό αρχείο αποτελεσμάτων
# και παράγει ραβδόγραμμα εστιασμένο στα ερωτήματα με πολύ μικρούς χρόνους εκτέλεσης.

import csv
from pathlib import Path
import matplotlib.pyplot as plt

BASE_DIR = Path(__file__).resolve().parent
IN_CSV = BASE_DIR / "comparison_results.csv"
OUT_PNG = BASE_DIR / "comparison_chart_q3_q5.png"


def main():
    # Έλεγχος ότι υπάρχει το συγκριτικό αρχείο αποτελεσμάτων.
    if not IN_CSV.exists():
        raise FileNotFoundError(f"Δεν βρέθηκε το αρχείο: {IN_CSV}")

    query_ids = []
    sqlite_times = []
    mongodb_times = []

    # Ανάγνωση μόνο των Q3, Q4 και Q5 από το comparison_results.csv.
    with open(IN_CSV, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)

        for row in reader:
            qid = row[0].strip()
            if qid in ["Q3", "Q4", "Q5"]:
                query_ids.append(qid)
                sqlite_times.append(float(row[4]))
                mongodb_times.append(float(row[5]))

    x = list(range(len(query_ids)))
    width = 0.35

    # Δημιουργία του συμπληρωματικού ραβδογράμματος.
    plt.figure(figsize=(8, 5))
    plt.bar([i - width / 2 for i in x], sqlite_times, width=width, label="SQLite")
    plt.bar([i + width / 2 for i in x], mongodb_times, width=width, label="MongoDB")

    plt.xticks(x, query_ids)
    plt.xlabel("Queries")
    plt.ylabel("Μέσος χρόνος εκτέλεσης (s)")
    plt.title("Σύγκριση μέσου χρόνου εκτέλεσης SQLite και MongoDB (Q3-Q5)")
    plt.legend()
    plt.tight_layout()

    # Αποθήκευση του διαγράμματος σε αρχείο PNG.
    plt.savefig(OUT_PNG, dpi=300)
    print(f"Το διάγραμμα αποθηκεύτηκε στο: {OUT_PNG.name}")

    plt.show()


if __name__ == "__main__":
    main()