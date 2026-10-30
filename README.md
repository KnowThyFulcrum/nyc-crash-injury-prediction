# NYC Crash Injury Prediction — PySpark MLlib vs. scikit-learn

Predicting whether a New York City motor-vehicle crash caused an injury, and comparing two ML toolkits (PySpark MLlib and scikit-learn) on the same ~1,000,000-record dataset running on Google Cloud Dataproc.

**Course:** CNT 6148 – Advanced Big Data Analytics, Florida International University (Summer 2026)

## Tools
Python · PySpark / Spark MLlib · scikit-learn · pandas · NumPy · Matplotlib · Google Cloud Dataproc (1 master + 2 workers) · HDFS / Google Cloud Storage · Jupyter

## Methods
1. **Data cleaning** (`src/crashes_raw_normalization.py`, PySpark): normalized column names to snake_case, parsed mixed date formats, cleaned text fields, derived time features (hour, day of week, month, year), and wrote a 1M-row cleaned Parquet dataset.
2. **Leakage prevention:** removed all injury/fatality count columns so models can't "cheat."
3. **Feature engineering:** encoded categorical columns and scaled numeric columns.
4. **Split:** 80% train / 20% test.
5. **Models:** Logistic Regression and Random Forest in both tools. PySpark MLlib trained on the full 1M rows; scikit-learn on a ~60,000-row random sample (single-node memory limits).
6. **Metrics:** AUC, accuracy, F1, precision, recall, training time.

## Results
| Model | Tool | AUC | Accuracy | F1 | Precision | Recall | Train time |
|---|---|---|---|---|---|---|---|
| Logistic Regression | PySpark MLlib | **0.7617** | 0.7955 | 0.7575 | 0.7880 | 0.7955 | 10.1 s |
| Random Forest | PySpark MLlib | 0.7388 | 0.7498 | 0.6426 | 0.8124 | 0.7498 | 83.6 s |
| Logistic Regression | scikit-learn | 0.7586 | 0.7948 | 0.4231 | 0.7199 | 0.2996 | 3.8 s |
| Random Forest | scikit-learn | 0.7490 | 0.7501 | 0.0112 | 0.8947 | 0.0056 | 1.2 s |

Both tools reached similar AUC (~0.74–0.76); Logistic Regression was stronger overall. PySpark handled the full dataset, while scikit-learn was faster on a smaller sample but struggled more with the imbalanced injury class.

## Repository structure
```
notebooks/final_notebook.ipynb      Final analysis: features, training, evaluation, charts
src/crashes_raw_normalization.py    PySpark cleaning / normalization job
config/cluster_cli_commands.sh      gcloud commands to create/delete the Dataproc cluster
config/cluster_config.json          Dataproc cluster configuration (identifiers redacted)
docs/final_paper.pdf                Final paper
docs/final_presentation.pptx        Final presentation
docs/presentation_script.md         Presentation speaker script
docs/raw_data_format.pdf            Raw data format reference
docs/project_instructions.png       Assignment instructions
requirements.txt
```

## Dataset
NYC Open Data — [Motor Vehicle Collisions – Crashes](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95). The raw and cleaned data are not included because of size. Download the CSV and upload it to your own bucket as `raw_data/crashes_RAW.csv`.

## How to run
1. Create a Dataproc cluster (see `config/cluster_cli_commands.sh`; replace `YOUR_PROJECT_ID`, `YOUR_CLUSTER_NAME`, region, and bucket placeholders).
2. Upload the raw CSV to `gs://YOUR_BUCKET_NAME/raw_data/crashes_RAW.csv`.
3. Run the cleaning job:
   `gcloud dataproc jobs submit pyspark src/crashes_raw_normalization.py --cluster=YOUR_CLUSTER_NAME --region=YOUR_REGION`
4. Copy the cleaned Parquet to HDFS (`hdfs:///user/dataproc/crashes/crashes_cleaned_1M.parquet`) or edit the path in the notebook.
5. Open `notebooks/final_notebook.ipynb` in the cluster's Jupyter and run all cells.

Local (small sample): `pip install -r requirements.txt` (requires Java for PySpark).

## Author
Jay Cespedes
