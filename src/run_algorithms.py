from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
import pandas as pd
import random
from poll_processing import execute_pipeline, directories, init_spark
import os
from pyspark.mllib.clustering import KMeansModel

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = "python" if os.name != "posix" else "python3"


def round_cf_df(df: pd.DataFrame) -> pd.DataFrame:
    col_rec_rounded = []
    for l in df["recommendations"]:
        col_rec_rounded.append([(item[0], round(item[1], 2)) for item in l])

    df.drop(columns="recommendations", inplace=True)
    df.insert(1, "recommendations", col_rec_rounded)

    return df


def add_nan_values(df: pd.DataFrame, percent: float = 0.1) -> pd.DataFrame:
    celle_el = []
    c = 1
    while c <= percent * df.shape[0] * df.shape[1]:
        i = random.randint(0, df.shape[0] - 1)
        j = random.randint(0, df.shape[1] - 1)

        cel = (i, j)
        if cel in celle_el:
            continue
        else:
            df.iloc[i, j] = None
            celle_el.append(cel)
            c += 1

    # print(df.shape)
    # print(celle_el)
    # print(len(celle_el))

    return df


def run_collaborative_filtering(
    spark: SparkSession,
    df,
):
    user_id = [i for i in range(df.shape[0])]

    rows = []
    for i in user_id:
        for j, _ in enumerate(df):
            rows.append((i, j, df.iloc[i][j]))

    cf_df = pd.DataFrame(rows, columns=["user_id", "feature_id", "value"])
    cf_df = spark.createDataFrame(cf_df)

    ratings = cf_df
    (training, test) = ratings.randomSplit([0.8, 0.2])

    # Build the recommendation model using ALS on the training data
    # Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    als = ALS(
        maxIter=5,
        regParam=0.01,
        userCol="user_id",
        itemCol="feature_id",
        ratingCol="value",
        coldStartStrategy="drop",
    )

    model = als.fit(training)

    # Evaluate the model by computing the RMSE on the test data
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="value",
        predictionCol="prediction",
    )

    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error = " + str(rmse))

    return model


def run_kmeans(spark: SparkSession, df: pd.DataFrame) -> list:
    vectors = []

    for _, r in df.iterrows():
        values = []
        for i in range(len(df.columns)):
            values.append(r[i])

        vectors.append(Vectors.dense(values))

    df_vectors = spark.createDataFrame(pd.DataFrame({"features": vectors}))

    kmeans = KMeans(
        k=3,
        seed=1,
        maxIter=10,
        predictionCol="prediction",
    )

    model = kmeans.fit(df_vectors)

    model.predict(df_vectors.head()["features"])  # type: ignore

    transformed = model.transform(df_vectors).select("features", "prediction")
    # rows = transformed.collect()

    clusters = [r[0] for r in model.summary.cluster.collect()]

    # add clusters column
    df.insert(df.shape[1], "cluster_idx", clusters)

    # sort by cluster index
    df.sort_values(by="cluster_idx", inplace=True)

    # save clustering result to disk
    # df.to_csv("./data/clusters.csv")

    return model.clusterCenters()


if __name__ == "__main__":
    spark = init_spark()
    df = execute_pipeline(spark, directories).toPandas()
    model = run_collaborative_filtering(spark, df)

    items_df = model.recommendForAllItems(52).toPandas()
    col_rec = items_df["recommendations"]

    # model.recommendForAllItems(52).show(truncate=False)
    # recs_df = model.recommendForAllItems(52).toPandas()
    # # print(model_pd)
    # print(model.recommendForAllUsers(25).toPandas())

    col_rec_rounded = []
    for l in col_rec:
        col_rec_rounded.append([(item[0], round(item[1], 2)) for item in l])

    items_df.drop(columns="recommendations", inplace=True)
    items_df.insert(1, "recommendations", col_rec_rounded)

    # print(items_df)

    # print(model.recommendForAllUsers(25).toPandas())

    # run_kmeans(spark, df[df.columns[1:]])

    # clustering = pd.read_csv("./data/clusters.csv")

    # clustering = pd.read_csv('./data/clusters.csv')
