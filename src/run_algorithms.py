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
) -> None:
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
        coldStartStrategy="nan",
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

    model.getMaxBlockSizeInMB()
    model.getDistanceMeasure()

    model.predict(df_vectors.head().features)

    # centers = model.clusterCenters()
    # len(centers)

    transformed = model.transform(df_vectors).select("features", "prediction")
    # rows = transformed.collect()

    print(model.hasSummary)
    print(model.summary.k)
    print(model.summary.clusterSizes)

    model.summary.cluster.show(52)


def tune_ALS(train_data, validation_data, maxIter, regParams, ranks):
    """
    grid search function to select the best model based on RMSE of
    validation data
    Parameters
    ----------
    train_data: spark DF with columns ['userId', 'movieId', 'rating']

    validation_data: spark DF with columns ['userId', 'movieId', 'rating']

    maxIter: int, max number of learning iterations

    regParams: list of float, one dimension of hyper-param tuning grid

    ranks: list of float, one dimension of hyper-param tuning grid

    Return
    ------
    The best fitted ALS model with lowest RMSE score on validation data
    """
    # initial
    min_error = float("inf")
    best_rank = -1
    best_regularization = 0
    best_model = None
    for rank in ranks:
        for reg in regParams:
            # get ALS model
            als = (
                ALS(userCol="user_id", itemCol="feature_id", ratingCol="value")
                .setMaxIter(maxIter)
                .setRank(rank)
                .setRegParam(reg)
            )
            # train ALS model
            model = als.fit(train_data)
            # evaluate the model by computing the RMSE on the validation data
            predictions = model.transform(validation_data)
            evaluator = RegressionEvaluator(
                metricName="rmse", labelCol="value", predictionCol="prediction"
            )
            rmse = evaluator.evaluate(predictions)
            print(
                "{} latent factors and regularization = {}: "
                "validation RMSE is {}".format(rank, reg, rmse)
            )
            if rmse < min_error:
                min_error = rmse
                best_rank = rank
                best_regularization = reg
                best_model = model
    print(
        "\nThe best model has {} latent factors and "
        "regularization = {}".format(best_rank, best_regularization)
    )
    return best_model


if __name__ == "__main__":
    spark = init_spark()
    df = execute_pipeline(spark, directories).toPandas()

    df = add_nan_values(df[df.columns[1:]], percent=0.1)
    run_collaborative_filtering(spark, df)

    # user_id = [i for i in range(df.shape[0])]

    # rows = []
    # for i in user_id:
    #     for index, c in enumerate(df):
    #         rows.append((i, index, df.iloc[i][index]))

    # cf_df = pd.DataFrame(rows, columns=["user_id", "feature_id", "value"])

    # cf_df = spark.createDataFrame(cf_df)

    # ratings = cf_df
    # (training, test) = ratings.randomSplit([0.8, 0.2])

    # ranks = [1, 3, 5, 10, 15, 20, 20, 30]
    # regParams = [0.01, 0.1, 0.05, 0.7, 1.0, 1.5]
    # M = tune_ALS(training, test, 10, regParams, ranks)
    # print("********STO STAMPANDO********")
    # print("model=", M)
