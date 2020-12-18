# Machine Learning workflow with AWS Managed Apache Airflow and Google Big Query

This repository contains the assets for the Google Big Query and Apache Airflow integration sample described in this [blog post](https://#).

## Overview

This repository shows a sample pipeline to extract Google Analytics's data using Big Query and build, manage and orchestrate ML workflows using Amazon Managed Apache Airflow, S3 and AWS Personalise. We will build a recommender system to predict popularity items for a ecommerce website based on customer's daily page view/click/add to cart events of product page as well as the behavior of other similar customers.

## Setup Google Cloud Big Query in Amazon MWAA

At present, MWAA does not support Google Cloud Big Query SDK, you can use hack way to install the GCP Big Query Client.

```
$cd additional-packages
$bash setup.sh
$cd ..
```

## Add Personalise permissions to MWAA's role

```
$aws iam put-role-policy --role-name AmazonMWAA-MyAirflowEnvironment-em53Wv --policy-name AirflowPersonalizePolicy --policy-document file://airflowPersonalizePolicy.json

```

More details can be found at this [blog post](https://#).
