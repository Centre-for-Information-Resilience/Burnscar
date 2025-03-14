import duckdb


class DuckStorage:
    def __init__(self, path):
        self.path = path

    def __enter__(self):
        self.conn = duckdb.connect(self.path)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()
