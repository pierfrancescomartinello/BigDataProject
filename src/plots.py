import pandas as pd
import seaborn as sns
from pandas.plotting import scatter_matrix
from matplotlib import pyplot

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

if __name__ == '__main__':
    pass