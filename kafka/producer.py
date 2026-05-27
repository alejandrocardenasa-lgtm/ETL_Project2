import time
import json
import pandas as pd
from sqlalchemy import create_engine
from kafka import KafkaProducer


DB_URL = "mysql+pymysql://root:root@localhost:3306/climate_dw"
TOPIC_NAME = "climate_temperature_city_metrics"


def get_city_metrics():
    engine = create_engine(DB_URL)

    query = """
    SELECT
        c.city,
        s.nombre_fuente AS source_name,
        ROUND(AVG(f.temperatura_promedio), 2) AS avg_temperature,
        ROUND(MAX(f.temperatura_maxima), 2) AS max_temperature,
        SUM(f.evento_calor_extremo) AS extreme_heat_events
    FROM fact_climate_daily f
    JOIN dim_city c
        ON f.city_id = c.city_id
    JOIN dim_source s
        ON f.source_id = s.source_id
    GROUP BY c.city, s.nombre_fuente
    ORDER BY extreme_heat_events DESC, max_temperature DESC;
    """

    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")


def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )

    print("Producer iniciado...")

    metrics_list = get_city_metrics()

    while True:
        for metrics in metrics_list:
            metrics["timestamp"] = pd.Timestamp.now().isoformat()

            producer.send(TOPIC_NAME, metrics)
            producer.flush()

            print("Metrica enviada:", metrics)

            time.sleep(5)


if __name__ == "__main__":
    main()