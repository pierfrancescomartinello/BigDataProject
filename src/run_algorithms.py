from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
import pandas as pd
import random
from poll_processing import execute_pipeline, directories, init_spark
import os

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = "python" if os.name != "posix" else "python3"

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


def run_kmeans(spark: SparkSession, df: pd.DataFrame) -> None:
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

    model.predict(df_vectors.head()["features"])

    # centers = model.clusterCenters()
    # len(centers)

    transformed = model.transform(df_vectors).select("features", "prediction")
    # rows = transformed.collect()

    print(model.hasSummary)
    print(model.summary.k)
    print(model.summary.clusterSizes)

    model.summary.cluster.show(52)



if __name__ == "__main__":
    spark = init_spark()
    df = execute_pipeline(spark, directories).toPandas()

    # run_kmeans(spark, df)

    # df = add_nan_values(df[df.columns[1:]], percent=0.1)
    model = run_collaborative_filtering(spark, df)

    #model.recommendForAllItems(52).show(truncate=False)
    print(model.recommendForAllItems(52).toPandas())
    #print(model_pd)
    print(model.recommendForAllUsers(25).toPandas())
