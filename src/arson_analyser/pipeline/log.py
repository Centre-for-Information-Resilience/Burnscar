import datetime
import logging
from enum import StrEnum
from typing import Type

from ..storage.duckdb import DuckDBStorage

logger = logging.getLogger(__name__)


class Step(StrEnum):
    ingestion = "ingestion"
    filtering = "filtering"
    clustering = "clustering"
    analysis = "analysis"


class Status(StrEnum):
    pending = "pending"
    success = "success"
    failed = "failed"


def allowed_values(enum: Type[StrEnum]) -> str:
    return str(tuple(v.value for v in enum))


def create_log_table(storage: DuckDBStorage):
    sql_string = f"""
        CREATE TABLE IF NOT EXISTS processing_log (
            id BIGINT
            partition_date DATE NOT NULL,
            step VARCHAR NOT NULL NOT NULL CHECK (step IN {allowed_values(Step)}),
            status VARCHAR NOT NULL CHECK (status IN {allowed_values(Status)}) DEFAULT '{Status.pending}',
            attempts INTEGER DEFAULT 0,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (partition_date, step)
        );
        """

    storage.conn.execute(sql_string)


def insert_log(storage: DuckDBStorage, partition_date: datetime.date, step: Step):
    sql_string = """
        INSERT OR IGNORE INTO processing_log (partition_date, step, status)
        VALUES (?, ?, ?)
        """
    storage.conn.execute(
        sql_string,
        (
            partition_date,
            step,
            Status.pending,
        ),
    )


def update_log(
    storage: DuckDBStorage,
    partition_date: datetime.date,
    step: Step,
    status: Status,
    increment_attempts: bool = False,
):
    sql_string = """UPDATE processing_log
        SET status = ?,
            attempts = attempts + ?
        WHERE 
            partition_date = ? AND
            step = ?
        """
    storage.conn.execute(
        sql_string,
        (
            status,
            int(increment_attempts),
            partition_date,
            step,
        ),
    )
