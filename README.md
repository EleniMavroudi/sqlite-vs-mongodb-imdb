# Συγκριτική Αξιολόγηση Σχεσιακής και NoSQL Βάσης Δεδομένων με Χρήση Πραγματικών Δεδομένων Ταινιών

Η παρούσα εργασία εκπονήθηκε στο πλαίσιο του μαθήματος «Πλατφόρμες και Αρχιτεκτονικές Νέφους» και έχει ως αντικείμενο τη συγκριτική αξιολόγηση μίας σχεσιακής βάσης δεδομένων (SQLite) και μίας NoSQL βάσης δεδομένων (MongoDB), με χρήση πραγματικών δεδομένων ταινιών από το IMDb.

## Περιεχόμενα φακέλων

### scripts
Περιλαμβάνει όλα τα Python scripts της εργασίας:
- προεπεξεργασία των δεδομένων IMDb
- δημιουργία της βάσης SQLite
- δημιουργία της βάσης MongoDB
- δοκιμαστικά ερωτήματα ελέγχου
- benchmark ερωτημάτων
- σύγκριση αποτελεσμάτων
- δημιουργία διαγραμμάτων

### data
Περιλαμβάνει τα τελικά καθαρά αρχεία CSV που προέκυψαν από την προεπεξεργασία των δεδομένων:
- movies.csv
- actors.csv
- movie_actors.csv
- directors.csv
- movie_directors.csv

Σημείωση: Τα αρχεία `movies.csv` και `movie_actors.csv` δεν συμπεριλήφθηκαν στο repository λόγω περιορισμού μεγέθους στο ανέβασμα αρχείων μέσω GitHub, αλλά χρησιμοποιήθηκαν κανονικά στην τοπική εκτέλεση των scripts της εργασίας.

### results
Περιλαμβάνει τα αποτελέσματα των μετρήσεων και τα τελικά διαγράμματα:
- sqlite_results.csv
- mongodb_results.csv
- comparison_results.csv
- comparison_chart.png
- comparison_chart_q3_q5.png

### report
Περιλαμβάνει το τελικό PDF της εργασίας.

## Σειρά εκτέλεσης των scripts

1. `01.imdb_prepare_full.py`
2. `02.create_sqlite_db_full.py`
3. `03.create_mongodb_db.py`
4. `04.test_sqlite_queries.py`
5. `05.test_mongodb_connection.py`
6. `06.benchmark_sqlite.py`
7. `07.benchmark_mongodb.py`
8. `08.compare_results.py`
9. `09.plot_comparison.py`
10. `10.plot_comparison_q3_q5.py`

## Πηγή δεδομένων

Τα δεδομένα προέρχονται από τα επίσημα IMDb Non-Commercial Datasets και χρησιμοποιήθηκαν μετά από προεπεξεργασία και επιλογή των απαραίτητων πεδίων για τις ανάγκες της εργασίας.
