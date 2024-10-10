import requests
import pyspark


def get_all_data(link, total_size):
    data = []
    for i in range(0, total_size, 100):
        request = requests.get(f"{link}?limit=100&offset={i}")
        data += request.json()["results"]
    return data


def pre_process(link):
    request = requests.get(f"{link}?limit=1")
    data = request.json()
    total_size = data["total_count"]
    return get_all_data(link, total_size)


def process_bronze(data):
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(data)
    df.write.format("delta").mode("overwrite").save("/delta/bronze")


def __main__():
    link = ("https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/sites-disposant-du-service-paris-wi-fi"
            "/records")
    data = pre_process(link)
    process_bronze(data)


__main__()
