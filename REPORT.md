# Αναφορά Εξαμηνιαίας Εργασίας — DSML 2026

**Ονοματεπώνυμο:** Σταύρος Κοροβέσης  
**ΑΜ:** 03400297  
**Μάθημα:** Διαχείριση Δεδομένων Μεγάλης Κλίμακας  
**Repository:** `<συμπληρώστε GitHub URL>`

---

## 1. Σύγκριση μορφών αποθήκευσης (CSV vs Parquet)

### Ερώτηση
Είναι το CSV το καταλληλότερο format για Spark; Ποια εναλλακτικά υπάρχουν;

### Απάντηση (σύντομα)
- **CSV:** αναγνώσιμο, universal, αλλά schema inference/parsing cost, μεγαλύτερο μέγεθος, χειρότερη συμπίεση.
- **Parquet:** columnar, συμπιεσμένο, schema embedded, καλύτερο για analytics στο Spark.
- **Άλλα:** JSON, ORC, Avro — διαφέρουν σε schema evolution, compression, συμβατότητα.

### Μετατροπή
Εκτελέστηκε `convert_parquet.py` → Parquet στο  
`/user/dsml00297/data/LA_Crime_Data/parquet`

### Benchmark (Query 2)
| Format | Median χρόνος (s) | Παρατηρήσεις |
|--------|-------------------|--------------|
| CSV | _συμπληρώστε_ | από `QUERY_ELAPSED_SECONDS` / driver logs |
| Parquet | _συμπληρώστε_ | |

**Σχόλιο:** _συμπληρώστε μετά τα runs_

---

## 2. Query 1 — Μερίδιο εγκλημάτων σε δρόμο ανά τμήμα ημέρας

### Υλοποιήσεις
- RDD: `q1_rdd.py`
- DataFrame: `q1_df.py`
- DataFrame + UDF: `q1_df_udf.py`

### Config
`2 executors × 1 core × 2GB`

### Χρόνοι εκτέλεσης
| Υλοποίηση | Run 1 | Run 2 | Run 3 | Median |
|-----------|-------|-------|-------|--------|
| RDD | | | | |
| DataFrame | | | | |
| DataFrame+UDF | | | | |

### Αποτελέσματα (επικυρωμένα στο cluster)
| Περίοδος | street_pct | total_crimes | street_crimes |
|----------|------------|--------------|---------------|
| Νύχτα | 28.854 | 870235 | 251094 |
| Βράδυ | 27.552 | 719695 | 198292 |
| Πρωί | 18.881 | 693099 | 130866 |
| Απόγευμα | 18.294 | 855099 | 156432 |

Median χρόνος DataFrame (1 run): **20.3 s**

### Σχολιασμός
_Γιατί το DataFrame συνήθως ξεπερνάει RDD/UDF;_

---

## 3. Query 2 — Top-3 μήνες εγκλημάτων ανά έτος

### Υλοποιήσεις
- DataFrame: `q2_df.py`
- Spark SQL: `q2_sql.py`

### Config
`4 executors × 1 core × 2GB`

### Χρόνοι
| Υλοποίηση | Median (s) |
|-----------|------------|
| DataFrame | |
| SQL | |

### Σχολιασμός
_Catalyst optimizer / ίδιο logical plan;_

---

## 4. Query 3 — Κατακεφαλήν εισόδημα ανά Zip Code

### Ορισμός
`per_capita = median_household_income_2021 / population_2020`  
(άθροισμα `POP20` ανά `ZCTA20` από census blocks)

### Υλοποιήσεις
- DataFrame: `q3_df.py`
- RDD: `q3_rdd.py`

### Config
`3 executors × 1 core × 2GB`

### Χρόνοι
| Υλοποίηση | Median (s) |
|-----------|------------|
| DataFrame | |
| RDD | |

---

## 5. Query 4 — Πλησιέστερο αστυνομικό τμήμα ανά έγκλημα

### Μέθοδος
Cross join crimes × stations (broadcast stations), Haversine distance, επιλογή ελάχιστης απόστασης ανά έγκλημα, aggregation ανά `division`.

### Config A — κάθετη κλιμάκωση (2 executors)
| Cores/exec | Memory | Median (s) |
|------------|--------|------------|
| 1 | 2g | |
| 2 | 4g | |
| 4 | 8g | |

### Config B — οριζόντια κλιμάκωση (8 cores, 16g συνολικά)
| Config | Median (s) |
|--------|------------|
| 2 × (4c, 8g) | |
| 4 × (2c, 4g) | |
| 8 × (1c, 2g) | |

### Σχολιασμός κλιμάκωσης
_Ποιο config αποδίδει καλύτερα και γιατί;_

---

## 6. Join strategies (Q3 DataFrame, Q4)

### Q3 — join population με income
| Hint | Physical plan (κύριος κόμβος) | Median (s) |
|------|------------------------------|------------|
| auto | | |
| broadcast | | |
| merge | | |
| shuffle_hash | | |
| shuffle_replicate_nl | | |

### Q4 — cross join crimes × stations
| Hint | Physical plan | Median (s) |
|------|---------------|------------|
| broadcast (default) | BroadcastNestedLoopJoin | |
| merge | | |
| shuffle_hash | | |
| shuffle_replicate_nl | | |

### Συμπέρασμα
Ποια στρατηγική είναι καταλληλότερη και γιατί (μέγεθος πινάκων, skew, cross join).

---

## Παράρτημα — Εντολές αναπαραγωγής

```bash
source ~/bigdata-env.sh
bash ~/dsml2026-project/scripts/run_benchmarks.sh
```

Logs: `kubectl -n dsml00297-priv logs <driver-pod>`  
Αναζήτηση χρόνου: `QUERY_ELAPSED_SECONDS=`
