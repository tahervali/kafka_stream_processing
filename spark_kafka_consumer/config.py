import os


class Config(dict):
    def __getattr__(self, name):
        return self[name]


config = Config(
    {
        "KAFKA_SERVER": os.getenv("KAFKA_SERVER", "broker:9092"),
        "TOPIC_NAME": os.getenv("TOPIC_NAME", "test_topic"),
        "MAX_RETRIES": int(os.getenv("MAX_RETRIES", 5)),
        "RETRY_DELAY": int(os.getenv("RETRY_DELAY", 5)),  # seconds
        "PRODUCER_INTERVAL": float(os.getenv("PRODUCER_INTERVAL", 1)),
    }
)
