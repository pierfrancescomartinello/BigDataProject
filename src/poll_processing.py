import json
import os
from pathlib import Path

import pyspark.pandas as pd
from pyspark.sql import DataFrame, SparkSession

# directories used throughout the pipeline
directories = [
    str(Path(r"./data/poll_data.csv")),
    str(Path(r"./data/questions.json")),
    str(Path(r"./data/df_indexes.csv")),
]

def init_spark() -> SparkSession:
    spark: SparkSession = SparkSession.builder.master("local[*]").appName("poll_to_idxs").getOrCreate()  # type: ignore

    # ? doesn't work for some reason
    spark.sparkContext.setLogLevel("ERROR")

    return spark


def preprocess_poll(
    df: DataFrame,
    questions: dict,
) -> DataFrame:
    # create a question_text:question_idx map, useful to alias the columns
    questions_idxs = {questions[q]["question_text"]: q for q in questions.keys()}

    # alias the columns (long column names cause bugs and are hard to use)
    for k, v in questions_idxs.items():
        df = df.withColumnRenamed(k, v)

    return df


def compute_scores_df(
    df: DataFrame,
    questions: dict,
) -> DataFrame:
    # list of columns that will not be individually scored (due to not being used in the calculation)
    skip_cols = [
        "Ho letto e accettato l'informativa e confermo inoltre di avere più di 18 anni",
        "Informazioni cronologiche",
        "Quanti anni hai?",
        "Genere",
        "Da quante persone è composto il tuo nucleo familiare?",
        "Occupazione",
        "Quanto è grande la tua azienda?",
        "Da che regione provieni?",
        "Provincia di provenienza",
        "In che regione lavori/studi?",
        "Provincia del luogo di lavoro/studio",
        "Invalidità",
        "Tipo di residenza",
        "Numero di persone con cui convivi",
        "Entrate Familiari Mensili Nette",
        "Entrate Personali Mensili Nette ",
    ]

    # "Subtract" the list of unscorable cols from the list of scorable cols
    scorable_cols = [item for item in df.columns if item not in skip_cols]

    # Iterate over the scorable columns to create the scores dictionary
    scores = {}
    for col in scorable_cols:
        # Shortcuts for some useful values
        question_type = questions[col]["question_type"]
        question_score = questions[col]["question_score"]
        question_answers = questions[col]["answers"]

        # Skip unscored questions
        if len(question_answers) == 0 and question_type != "comma_separated":
            continue

        # Collect the Row values for the current column and access its value (index 0)
        answers = [str(d[0]) for d in df.select(col).collect()]

        # Handle score computation differently based on question_type
        row_scores = []
        if question_type in ["basic", "multivalue"]:
            for indexes_cols_df in answers:
                row_scores.append(
                    question_answers[indexes_cols_df]["answer_score"] * question_score
                )

            scores[col] = row_scores

        elif question_type == "comma_separated":
            for i in answers:
                row_scores.append(len(i.split(";")))

            scores[col] = row_scores

    # Create the scores dataframe from the scores dictionary
    df_scores = pd.DataFrame(scores).to_spark(index_col="_c0")

    return df_scores


def compute_indexes(df: DataFrame, df_scores: DataFrame, questions: dict):
    # The indexes dictionary, which will be used to compute the indexes dataframe
    indexes_cols = {}

    # Purchases indexes calculation
    df_s = df_scores.select([f"S_{d}" for d in range(1, 10)]).collect()

    # i_S_col
    i_S_col = []
    for s in df_s:
        i_S_col.append(sum([val for val in s]) / 1450)
    indexes_cols["i_S"] = i_S_col

    col_S_1 = df.select("S_1").collect()
    S_1 = []
    answer1 = questions["S_1"]["answers"]
    for i in range(len(col_S_1)):
        s1 = col_S_1[i][0]
        for j in answer1.keys():
            if s1 == j:
                val = answer1[j]["answer_score_1"]
                S_1.append(val)
                break

    col_S_2 = df.select("S_2").collect()
    S_2 = []
    answer2 = questions["S_2"]["answers"]
    for i in range(len(col_S_2)):
        s2 = str(col_S_2[i][0])
        for j in answer2.keys():
            if s2 == j:
                val = answer2[j]["answer_score_1"]
                S_2.append(val)
                break

    df_s1 = df_scores.select([f"S_{d}" for d in range(3, 10)]).collect()
    i_tot_S_col = []
    k = 0  # variabile temporanea per accedere agli elementi di S_1 ed S_2 ed effettuare il rapporto
    for s_1 in df_s1:
        somma = sum([val1 for val1 in s_1])
        r = S_2[k] / S_1[k]
        i_tot_S_col.append(somma * r)
        k += 1
    indexes_cols["i_tot_S"] = i_tot_S_col

    # Calcolo indici domande relative al Vestiario
    df_v = df_scores.select([f"V_{d}" for d in range(10, 13)]).collect()

    i_V_col = []
    for v in df_v:
        i_V_col.append(sum([val for val in v]) / 550)

    indexes_cols["i_V"] = i_V_col

    col_V_10 = df.select("V_10").collect()
    V_10 = []
    answer10 = questions["V_10"]["answers"]
    for i in range(len(col_V_10)):
        v10 = str(col_V_10[i][0])

        for j in answer10.keys():
            if v10 == j:
                val = answer10[j]["answer_score_1"]
                V_10.append(val)
                break

    df_v1 = df_scores.select([f"V_{d}" for d in range(11, 13)]).collect()
    i_tot_V_col = []
    l = 0  # variabile temporanea per accedere agli elementi di V_10
    for v1 in df_v1:
        somma1 = sum([val1 for val1 in v1]) / 400
        i_tot_V_col.append(somma1 * V_10[l])
        l += 1
    indexes_cols["i_tot_V"] = i_tot_V_col

    # Calcolo indici domande relative alla Casa
    df_c = df_scores.select([f"C_{d}" for d in range(13, 15)]).collect()

    i_C_col = []
    for c in df_c:
        i_C_col.append(sum([val for val in c]) / 350)

    indexes_cols["i_C"] = i_C_col

    i_tot_C_col = []
    for c1 in df_c:
        if c1[1] == 0:
            i_tot_C_col.append(c1[0] / 150)
        else:
            i_tot_C_col.append(c1[0] / c1[1])
    indexes_cols["i_tot_C"] = i_tot_C_col

    # Calcolo indice Educazione e Ricerca
    df_e = df_scores.select([f"ER_{d}" for d in range(1, 4)]).collect()
    i_ER_col = []
    for e in df_e:
        i_ER_col.append(sum([val for val in e]) / 500)
    indexes_cols["i_ER"] = i_ER_col

    # mobility indexes
    df_m1 = df_scores.select(["m_8", "m_9"]).collect()

    i_m1_col = []
    for s in df_m1:
        m_8, m_9 = s[0], s[1]
        i_m1_col.append(m_9 / m_8)

    indexes_cols["i_M1"] = i_m1_col

    df_m2 = df_scores.select(["m_17", "m_18"]).collect()

    i_m2_col = []
    for s in df_m2:
        m_17, m_18 = s[0], s[1]
        i_m2_col.append(m_18 / m_17)

    indexes_cols["i_M2"] = i_m2_col

    fam_size_col_label = "Da quante persone è composto il tuo nucleo familiare?"

    df_m3 = df_scores.select(["m_8"]).collect()
    df_fam = df.select([fam_size_col_label]).collect()

    i_m3_col = []
    for s in df_m3:
        m_8, f = s[0], int(df_fam[0][0])
        i_m3_col.append(m_8 / f)

    indexes_cols["i_M3"] = i_m3_col

    df_m4 = df_scores.select(["m_11", "m_12", "m_13"]).collect()

    i_m4_col = []
    for s in df_m4:
        m_11, m_12, m_13 = s[0], s[1], s[2]
        i_m4_col.append(m_12 / (m_11 + m_12 + m_13))

    indexes_cols["i_M4"] = i_m4_col

    df_m5 = df_scores.select(["m_20"]).collect()

    i_m5_col = []
    for s in df_m5:
        m_20 = s[0]
        i_m5_col.append(m_20)

    indexes_cols["i_M5"] = i_m5_col

    df_m6 = df_scores.select(["m_19"]).collect()

    i_m6_col = []
    for s in df_m6:
        m_19 = s[0]
        i_m6_col.append(m_19)

    indexes_cols["i_M6"] = i_m6_col

    df_m7 = df_scores.select(["m_4", "m_5"]).collect()

    i_m7_col = []
    for s in df_m7:
        m_4, m_5 = s[0], s[1]
        i_m7_col.append(m_5 / m_4)

    indexes_cols["i_M7"] = i_m7_col

    # energy indexes

    # Systems (home)
    df_e1 = df_scores.select(["eh2"]).collect()
    i_e1_col = []
    for s in df_e1:
        i_e1_col.append(s[0] / 5)
    indexes_cols["i_e1"] = i_e1_col

    # Systems (work)
    df_e2 = df_scores.select(["ew2"]).collect()
    i_e2_col = []
    for s in df_e2:
        i_e2_col.append(s[0] / 5)
    indexes_cols["i_e2"] = i_e2_col

    # Sustainable Source (home)
    df_e3 = df_scores.select(["eh3", "eh4"]).collect()
    i_e3_col = []
    for s in df_e3:
        eh3, eh4 = s[0], s[1]
        i_e3_col.append(eh3 * eh4)
    indexes_cols["i_e3"] = i_e3_col

    # Sustainable Source (work)
    df_e4 = df_scores.select(["ew3", "ew4"]).collect()
    i_e4_col = []
    for s in df_e4:
        ew3, ew4 = s[0], s[1]
        i_e4_col.append(ew3 * ew4)
    indexes_cols["i_e4"] = i_e4_col

    # Efficiency Ratio
    df_e5 = df_scores.select(["eh1", "eh5"]).collect()
    i_e5_col = []
    for s in df_e5:
        eh1, eh5 = s[0], s[1]
        i_e5_col.append(eh1 / eh5)
    indexes_cols["i_e5"] = i_e5_col

    # GreenBuilding (home)
    df_e6 = df_scores.select(["eh6"]).collect()
    i_e6_col = []
    for s in df_e6:
        i_e6_col.append(s[0] / 6)
    indexes_cols["i_e6"] = i_e6_col

    # GreenBuilding (work)
    df_e7 = df_scores.select(["ew5"]).collect()
    i_e7_col = []
    for s in df_e7:
        i_e7_col.append(s[0] / 6)
    indexes_cols["i_e7"] = i_e7_col

    # Water Index

    # Water (home)
    df_water1 = df_scores.select(["wh1", "wh2", "wh3", "wh4"]).collect()
    i_e8_col = []
    for s in df_water1:
        wh1, wh2, wh3, wh4 = s[0], s[1], s[2], s[3]
        i_e8_col.append((wh1 + wh2 + wh3 + wh4) / 4)
    indexes_cols["i_e8"] = i_e8_col

    # Water (work)
    df_water2 = df_scores.select(["ww1", "ww2", "ww3", "ww4"]).collect()
    i_e9_col = []
    for s in df_water2:
        ww1, ww2, ww3, ww4 = s[0], s[1], s[2], s[3]
        i_e9_col.append((ww1 + ww2 + ww3 + ww4) / 4)
    indexes_cols["i_e9"] = i_e9_col

    # Waste Indexes

    # Waste (home)
    df_waste1 = df_scores.select(["wasteh1", "wasteh2", "wasteh3", "wasteh4"]).collect()
    i_e10_col = []
    for s in df_waste1:
        wasteh1, wasteh2, wasteh3, wasteh4 = s[0], s[1], s[2], s[3]
        i_e10_col.append((wasteh1 + wasteh2 + wasteh3 + wasteh4) / 4)
    indexes_cols["i_e10"] = i_e10_col

    # Waste (work)
    df_waste2 = df_scores.select(["wastew1", "wastew2", "wastew3", "wastew4"]).collect()
    i_e11_col = []
    for s in df_waste1:
        wastew1, wastew2, wastew3, wastew4 = s[0], s[1], s[2], s[3]
        i_e11_col.append((wastew1 + wastew2 + wastew3 + wastew4) / 4)
    indexes_cols["i_e11"] = i_e11_col

    # Indexes dataframe

    # round to two digits
    for k in indexes_cols.keys():
        indexes_cols[k] = [round(e, 2) for e in indexes_cols[k]]

    return indexes_cols


def normalize_scores_df(spark: SparkSession, indexes_cols: list) -> DataFrame:
    indexes_cols_df = pd.DataFrame(indexes_cols)

    indexes_max = {
        # "i_S": None,
        # "i_tot_S": None,
        # "i_V": None,
        # "i_C": None,
        # "i_ER": None,
        # "i_M1": 1,
        # "i_M3": None,
        # "i_M7": None,
        "i_tot_V": 5,
        "i_tot_C": 1.5,
        "i_M2": 1,
        "i_M4": 0.95,
        "i_M5": 100,
        "i_M6": 100,
        "i_e1": 1,
        "i_e2": 1,
        "i_e3": 5,
        "i_e4": 5,
        "i_e5": 1 / 0.015,
        "i_e6": 1,
        "i_e7": 1,
        "i_e8": 200,
        "i_e9": 200,
        "i_e10": 200,
        "i_e11": 200,
    }

    for c in indexes_cols_df.columns:
        val = indexes_cols_df[c] / indexes_max.get(c, indexes_cols_df[c].max())
        indexes_cols_df[c] = round(val, 3)

    # create the indexes dataframe from the indexes dictionary
    df_indexes = spark.createDataFrame(pd.DataFrame(indexes_cols_df))

    return df_indexes


def execute_pipeline(
    spark: SparkSession,
    dirs: list,
    overwrite: bool = False,
):
    poll_dir, questions_dir, df_indexes_dir = dirs

    # load the csv poll as a dataframe
    df = (
        spark.read.format("csv")
        .option("encoding", "UTF-8")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(poll_dir)
    )

    # load the questions json
    with open(questions_dir, "r", encoding="utf-8") as questions_file:
        questions = json.load(questions_file)

    # skip pipeline by reading from disk if indexes_df already exists
    if not os.path.exists(df_indexes_dir) or overwrite:
        # preprocess poll csv and compute the scores df
        df_processed = preprocess_poll(df, questions)
        df_scores = compute_scores_df(df_processed, questions)

        # compute indexes and create a pd.DataFrame, to be converted later
        indexes = compute_indexes(df_processed, df_scores, questions)
        df_indexes = pd.DataFrame(indexes).to_pandas()

        # write pd.DataFrame to disk for caching
        df_indexes.to_csv(df_indexes_dir, index_label="_c0")

        df_indexes = spark.createDataFrame(df_indexes)
    else:
        # load indexes from disk and parse it using pandas
        df_indexes = pd.read_csv(df_indexes_dir, index_col="_c0")

        # convert pd-on-spark DF to spark DF
        df_indexes = df_indexes.to_spark(index_col="_c0")

    return df_indexes


if __name__ == "__main__":
    # create SparkSession instance
    spark = init_spark()

    # executes the pipeline and returns a Spark DataFrame
    df_indexes = execute_pipeline(spark, directories, overwrite=False)

    df_indexes.show()
