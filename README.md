# DSML 2026 — Εξαμηνιαία Εργασία Big Data

Φοιτητής: **Σταύρος Κοροβέσης**  
ΑΜ: **03400297**  
Username εργαστηρίου: **dsml00297**

## Περιεχόμενα

- `code/` — υλοποιήσεις Python (PySpark)
- `scripts/` — εκτέλεση στο cluster Kubernetes
- `REPORT.md` — κείμενο αναφοράς (μετατροπή σε PDF για παράδοση)
- `LLM_USAGE.md` — δήλωση χρήσης LLM (υποχρεωτικό)

## Προαπαιτούμενα

1. OpenVPN συνδεδεμένο (Windows)
2. WSL Ubuntu με `~/bigdata-env.sh` (οδηγός [04](https://github.com/ikons/bigdata-dsml/blob/main/docs/04_remote-spark-kubernetes/README.md))
3. `kubectl`, `spark-submit`, `hdfs` λειτουργικά

## Αντιγραφή project στο WSL

```bash
rm -rf ~/dsml2026-project
cp -r "/mnt/c/Users/steve/OneDrive/Desktop/mine/Data_Science_ML/courses/spring_semester/big data/dsml2026-project" ~/
chmod +x ~/dsml2026-project/scripts/*.sh
```

## Εκτέλεση ενός query

```bash
source ~/bigdata-env.sh
cd ~/dsml2026-project/code

spark-submit \
  --py-files common.py \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  q1_df.py \
  --output-base hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/project-output
```

## Εκτέλεση όλων των benchmarks

```bash
bash ~/dsml2026-project/scripts/run_benchmarks.sh
```

## Παρακολούθηση

```bash
kubectl -n dsml00297-priv get pods
kubectl -n dsml00297-priv logs <driver-pod>
hdfs dfs -ls /user/dsml00297/project-output
hdfs dfs -ls /user/dsml00297/logs
```

## Αναφορά LaTeX

Η αναφορά βρίσκεται στο `bigdata_2026_report.pdf` με **όλους τους χρόνους** ήδη συμπληρωμένους.


```

## Πίνακας χρόνων (συλλογή 2026-06-28)

| Ζητούμενο | Run | Χρόνος (s) |
|-----------|-----|------------|
| Μετατροπή Parquet | convert | 295.03 |
| Q2 CSV | format benchmark | 32.75 |
| Q2 Parquet | format benchmark | 4.25 |
| Q1 RDD | q1_rdd | 1.92 |
| Q1 DF | q1_df | 21.62 |
| Q1 DF+UDF | q1_df_udf | 224.56 |
| Q2 DF | q2_df | 327.66 |
| Q2 SQL | q2_sql | 101.97 |
| Q3 DF | q3_df | 285.30 |
| Q3 RDD | q3_rdd (rerun) | 3.39 |
| Q4 (2×1c×2g) | vertical | 295.14 |
| Q4 (2×2c×4g) | vertical | 356.36 |
| Q4 (2×4c×8g) | vertical | **89.38** |
| Q4 (2×4c×8g) | horizontal | 218.82 |
| Q4 (4×2c×4g) | horizontal | **176.56** |
| Q4 (8×1c×2g) | horizontal | 230.81 |


| Αρχείο | Περιγραφή |
|--------|-----------|
| `q1_rdd.py` | Q1 με RDD API |
| `q1_df.py` | Q1 με DataFrame (χωρίς UDF) |
| `q1_df_udf.py` | Q1 με DataFrame + UDF |
| `q2_df.py` | Q2 με DataFrame |
| `q2_sql.py` | Q2 με Spark SQL |
| `q3_df.py` | Q3 με DataFrame (+ join hints) |
| `q3_rdd.py` | Q3 με RDD |
| `q4_df.py` | Q4 nearest police division (+ join hints) |
| `convert_parquet.py` | Μετατροπή crime CSV → Parquet |
| `benchmark_format.py` | Σύγκριση CSV vs Parquet στο Q2 |

## Δεδομένα (HDFS `/data/`)

Τα datasets είναι read-only στο `/data/`. Το Parquet output γράφεται στο:

`hdfs://.../user/dsml00297/data/LA_Crime_Data/parquet`
