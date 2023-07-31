import os
import numpy as np
import pandas as pd
import seaborn as sns
from pandas.plotting import scatter_matrix
from matplotlib import pyplot

from poll_processing import execute_pipeline, init_spark
from run_algorithms import run_kmeans, directories


def show_heatmap(df_features) -> None:
    sns.heatmap(df_features.corr(), cmap="crest")


def show_correlation(df_features, threshold: float = 0.5) -> None:
    with pd.option_context("display.max_rows", 100, "display.max_columns", 100):
        print(df_features.corr()[df_features.corr() >= threshold])


def show_scatter_matrices(df_features) -> None:
    scatter_matrix(
        df_features[["i_e1", "i_e3", "i_e6", "i_e2", "i_e4", "i_e7"]], diagonal="kde"
    )
    scatter_matrix(
        df_features[["i_e1", "i_e3", "i_e6", "i_e2", "i_e4", "i_e7"]], diagonal="hist"
    )

    scatter_matrix(df_features[["i_e8", "i_e9", "i_e10", "i_e11"]], diagonal="kde")
    scatter_matrix(df_features[["i_e8", "i_e9", "i_e10", "i_e11"]], diagonal="hist")

    scatter_matrix(
        df_features[["i_S", "i_tot_S", "i_V", "i_tot_V", "i_C", "i_tot_C"]],
        diagonal="kde",
    )
    scatter_matrix(
        df_features[["i_S", "i_tot_S", "i_V", "i_tot_V", "i_C", "i_tot_C"]],
        diagonal="hist",
    )

    fig, ax = pyplot.subplots()
    pyplot.xlabel("casa")
    pyplot.ylabel("lavoro")
    ax.scatter(df_features["i_e8"], df_features["i_e9"])


def plot_clusters(df_clusters: pd.DataFrame) -> None:
    if not os.path.exists("./data/clusters_avg.csv"):
        avg = df_clusters.groupby(["cluster_idx"]).aggregate(
            {f: [np.mean] for f in df_clusters.columns[1:-1]}
        )

        avg.to_csv("./data/clusters_avg.csv")

    else:
        avg = pd.read_csv("./data/clusters_avg.csv")

    return avg

if __name__ == "__main__":
    spark = init_spark()
    # df = execute_pipeline(spark, directories, overwrite=False).toPandas()

    df = pd.read_csv("./data/clusters.csv")
    # centers = run_kmeans(spark, df[df.columns[1:]])

    plot_clusters(df)
