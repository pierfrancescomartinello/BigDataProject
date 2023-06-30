import json
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("test").getOrCreate()

    poll_dir = "data/poll_data.csv"

    df = (
        spark.read.format("csv")
        # .option("encoding", "UTF-8")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(poll_dir)
    )

    df = (
        df.drop("Informazioni cronologiche")
        .drop(
            "Ho letto e accettato l'informativa e confermo inoltre di avere più di 18 anni"
        )
        .drop("Quanti anni hai?")
        .drop("Genere")
        .drop("Da quante persone è composto il tuo nucleo familiare?")
        .drop("Occupazione")
        .drop("Quanto è grande la tua azienda?")
        .drop("Da che regione provieni?")
        .drop("Provincia di provenienza")
        .drop("In che regione lavori/studi?")
        .drop("Provincia del luogo di lavoro/studio")
        .drop("Invalidità")
        .drop("Tipo di residenza")
        .drop("Numero di persone con cui convivi")
        .drop("Entrate Familiari Mensili Nette")
        .drop("Entrate Personali Mensili Nette ")
    )

    # df.show(5)

    # with open("./data/questions_noidx.json", "r", encoding='utf-8') as questions_file:
    with open("./data/questions.json", "r", encoding="utf-8") as questions_file:
        questions = json.load(questions_file)

    questions_idxs = {questions[q]["question_text"]: q for q in questions.keys()}

    # print(questions_idxs)

    scores = []
    for col in df.columns:
        idx = questions_idxs[col]

        question_type = questions[idx]["question_type"]
        question_score = questions[idx]["question_score"]
        answers = questions[idx]["answers"]

        if len(answers) == 0:
            continue

        print(question_type)
        print(answers)
        print(question_score)

        # data = df.select(col).collect()
        # answers = [d[0] for d in data]

        # # col_scores = [answers[data[i][0]]["answer_score"] for i in range(len(data))]
        # # scores.append(col_scores)
        # print(answers)
