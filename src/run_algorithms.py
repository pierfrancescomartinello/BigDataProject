from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
import pandas as pd

from poll_processing import execute_pipeline, directories, init_spark


def run_collaborative_filtering(
    spark: SparkSession,
    indexes_cols_df: pd.DataFrame,
) -> None:
    user_id = [i for i in range(indexes_cols_df.shape[0])]

    rows = []
    for i in user_id:
        for index, c in enumerate(indexes_cols_df):
            rows.append((i, index, indexes_cols_df.iloc[i][index]))

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

    # Generate top 10 movie recommendations for each user
    userRecs = model.recommendForAllUsers(10)


def run_kmeans(spark: SparkSession, df_indexes) -> None:
    df_features = df_indexes.toPandas()

    vectors = []
    for _, r in df_features.iterrows():
        values = []
        for i in range(len(df_features.columns)):
            values.append(r[i])

        vectors.append(Vectors.dense(values))

    df_vectors = spark.createDataFrame(pd.DataFrame({"features": vectors}))

    kmeans = KMeans(k=3)

    kmeans.setSeed(1)
    # kmeans.setWeightCol("weigh_col")
    kmeans.setMaxIter(10)
    kmeans.getMaxIter()
    kmeans.clear(kmeans.maxIter)
    kmeans.getSolver()

    model = kmeans.fit(df_vectors)

    model.getMaxBlockSizeInMB()
    model.getDistanceMeasure()
    model.setPredictionCol("prediction")

    model.predict(df_vectors.head().features)

    centers = model.clusterCenters()
    len(centers)

    transformed = model.transform(df_vectors).select("features", "prediction")
    rows = transformed.collect()

    print(model.hasSummary)
    summary = model.summary
    print(summary.k)
    print(summary.clusterSizes)

    summary.cluster.show(52)


if __name__ == "__main__":
    spark = init_spark()
    df = execute_pipeline(spark, directories)
