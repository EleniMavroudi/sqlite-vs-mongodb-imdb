# Σύγκριση των αποτελεσμάτων SQLite και MongoDB
# Το script διαβάζει τα δύο αρχεία benchmark,
# συγκρίνει πλήθος αποτελεσμάτων και μέσους χρόνους
# και αποθηκεύει το τελικό συγκριτικό αρχείο σε CSV.

import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

SQLITE_CSV = BASE_DIR / "sqlite_results.csv"
MONGODB_CSV = BASE_DIR / "mongodb_results.csv"
OUT_CSV = BASE_DIR / "comparison_results.csv"


def read_results(path):
    data = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            query_id = row["query_id"]
            data[query_id] = row
    return data


# Μετατροπή τιμής σε δεκαδικό αριθμό, όπου αυτό είναι δυνατό.
def to_float(value):
    try:
        return float(value)
    except Exception:
        return None


def main():
    # Έλεγχος ότι υπάρχουν τα δύο αρχεία benchmark.
    if not SQLITE_CSV.exists():
        raise FileNotFoundError(f"Δεν βρέθηκε το αρχείο: {SQLITE_CSV}")
    if not MONGODB_CSV.exists():
        raise FileNotFoundError(f"Δεν βρέθηκε το αρχείο: {MONGODB_CSV}")

    # Ανάγνωση αποτελεσμάτων από SQLite και MongoDB.
    sqlite_data = read_results(SQLITE_CSV)
    mongo_data = read_results(MONGODB_CSV)

    # Κρατάμε μόνο τα κοινά query_id που υπάρχουν και στα δύο αρχεία.
    query_ids = sorted(set(sqlite_data.keys()) & set(mongo_data.keys()))

    rows_out = []

    print("=" * 110)
    print("ΣΥΓΚΡΙΤΙΚΑ ΑΠΟΤΕΛΕΣΜΑΤΑ SQLITE vs MONGODB")
    print("=" * 110)

    for qid in query_ids:
        s = sqlite_data[qid]
        m = mongo_data[qid]

        desc = s["description"]
        sqlite_rows = s["row_count"]
        mongo_rows = m["row_count"]

        sqlite_avg = to_float(s["avg_time_sec"])
        mongo_avg = to_float(m["avg_time_sec"])

        diff = None
        ratio = None
        faster = ""

        if sqlite_avg is not None and mongo_avg is not None:
            diff = sqlite_avg - mongo_avg
            if mongo_avg != 0:
                ratio = sqlite_avg / mongo_avg

            if sqlite_avg < mongo_avg:
                faster = "SQLite"
            elif mongo_avg < sqlite_avg:
                faster = "MongoDB"
            else:
                faster = "Ισοπαλία"

        print(f"{qid} | {desc}")
        print(f"  rows -> SQLite: {sqlite_rows} | MongoDB: {mongo_rows}")
        print(f"  avg  -> SQLite: {sqlite_avg:.6f} s | MongoDB: {mongo_avg:.6f} s")
        print(f"  faster: {faster}")
        print("-" * 110)

        rows_out.append({
            "query_id": qid,
            "description": desc,
            "sqlite_row_count": sqlite_rows,
            "mongodb_row_count": mongo_rows,
            "sqlite_avg_time_sec": f"{sqlite_avg:.6f}" if sqlite_avg is not None else "",
            "mongodb_avg_time_sec": f"{mongo_avg:.6f}" if mongo_avg is not None else "",
            "difference_sqlite_minus_mongodb_sec": f"{diff:.6f}" if diff is not None else "",
            "time_ratio_sqlite_div_mongodb": f"{ratio:.6f}" if ratio is not None else "",
            "faster_system": faster,
        })

    # Αποθήκευση του τελικού συγκριτικού αρχείου CSV.
    with open(OUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "query_id",
                "description",
                "sqlite_row_count",
                "mongodb_row_count",
                "sqlite_avg_time_sec",
                "mongodb_avg_time_sec",
                "difference_sqlite_minus_mongodb_sec",
                "time_ratio_sqlite_div_mongodb",
                "faster_system",
            ]
        )
        writer.writeheader()
        writer.writerows(rows_out)

    print("\nΈτοιμο.")
    print(f"Αποθηκεύτηκε το συγκριτικό αρχείο: {OUT_CSV.name}")


if __name__ == "__main__":
    main()