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
    min_error = float('inf')
    best_rank = -1
    best_regularization = 0
    best_model = None
    for rank in ranks:
        for reg in regParams:
            # get ALS model
            als = ALS().setMaxIter(maxIter).setRank(rank).setRegParam(reg)
            # train ALS model
            model = als.fit(train_data)
            # evaluate the model by computing the RMSE on the validation data
            predictions = model.transform(validation_data)
            evaluator = RegressionEvaluator(metricName="rmse",
                                            labelCol="rating",
                                            predictionCol="prediction")
            rmse = evaluator.evaluate(predictions)
            print('{} latent factors and regularization = {}: '
                  'validation RMSE is {}'.format(rank, reg, rmse))
            if rmse < min_error:
                min_error = rmse
                best_rank = rank
                best_regularization = reg
                best_model = model
    print('\nThe best model has {} latent factors and '
          'regularization = {}'.format(best_rank, best_regularization))
    return best_model

if __name__ == "__main__":
    spark = init_spark()
    df = execute_pipeline(spark, directories)

    ratings = df
    (training, test) = ratings.randomSplit([0.8,0.2])

    ranks = [1.5, 3, 5, 10.5, 15, 20, 20.5, 30]
    regParams = [0.01, 0.1, 0.05, 0.7, 1.0, 1.5]
    M = tune_ALS(training, test, 10, regParams, ranks)
    print("********STO STAMPANDO********")
    print("model=",M)

