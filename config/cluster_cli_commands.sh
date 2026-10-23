  CREATE CLUSTER
gcloud dataproc clusters create YOUR_CLUSTER_NAME \
  --project=YOUR_PROJECT_ID \
  --enable-component-gateway \
  --region=us-east1 \
  --subnet=default \
  --no-address \
  --master-machine-type=n4-standard-2 \
  --master-boot-disk-type=hyperdisk-balanced \
  --master-boot-disk-size=166 \
  --num-workers=2 \
  --worker-machine-type=n4-standard-2 \
  --worker-boot-disk-type=hyperdisk-balanced \
  --worker-boot-disk-size=166 \
  --image-version=2.3-debian12 \
  --optional-components=JUPYTER \
  --scopes 'https://www.googleapis.com/auth/cloud-platform' \
  --properties spark:spark.dataproc.engine=lightningEngine,spark:spark.dataproc.lightningEngine.runtime=default

  DELETE PROJECT
gcloud projects delete YOUR_PROJECT_ID

  DELETE CLUSTER
gcloud dataproc clusters delete YOUR_CLUSTER_NAME \
  --region=YOUR_REGION \
  --project=YOUR_PROJECT_ID

  DELETE BUCKET
gcloud storage buckets delete gs://YOUR_BUCKET_NAME \
  --project=YOUR_PROJECT_ID