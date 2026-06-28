# Αποτύπωμα εκτέλεσης στο Kubernetes (dsml00297)

Οι εκτελέσεις καταγράφονται στο **δικό σου** namespace και HDFS path.

## Γρήγορος έλεγχος (με OpenVPN + WSL)

```bash
source ~/bigdata-env.sh

# Driver pods της εργασίας
kubectl -n dsml00297-priv get pods | grep -E 'q1-|q2-|q3-|q4-|convert|format-benchmark'

# Spark event logs (υποχρεωτικό για την εργασία)
hdfs dfs -ls /user/dsml00297/logs | tail -20

# Outputs της εργασίας
hdfs dfs -ls /user/dsml00297/project-output

# Parquet (ζητούμενο 1)
hdfs dfs -ls /user/dsml00297/data/LA_Crime_Data/parquet
```

## Τι υπάρχει ήδη (2026-06-28)

- **30+** driver pods (Completed) στο `dsml00297-priv`
- Event logs στο `/user/dsml00297/logs/`
- Outputs στο `/user/dsml00297/project-output/` (q1, q2, q3, q4, format benchmark)

## Προφορική εξέταση

Η εκφώνηση απαιτεί **ζωντανή επίδειξη**. Πριν την εξέταση, τρέξε εσύ ένα query:

```bash
source ~/bigdata-env.sh
cd ~/dsml2026-project/code
spark-submit --py-files common.py \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  q1_df.py \
  --output-base hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/project-output
kubectl -n dsml00297-priv get pods -w
```
