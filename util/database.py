import os
from typing import List

import duckdb
import pandas as pd
from duckdb.duckdb import ParserException
from datetime import datetime

import psycopg2 as pg
from dotenv import load_dotenv

# init database connection on load

load_dotenv()
CONN = duckdb.connect("hsku_local.duckdb")

TABLES = {
    "users_table": [
        "user_id varchar unique",
        "name varchar",
        "nick varchar",
    ],
    "threads_table": [
        "user_id varchar unique",
        "personal_inbox_id varchar",
        "personal_inbox_name varchar",
    ],
    "roles_table": ["role_id varchar unique", "name varchar"],
    "messages_table": [
        "sender_id varchar",
        "recipient_id varchar",
        "time int",
        "message varchar",
    ],
    "loans_table": [
        "role_id varchar",
        "interest decimal(3,2)",
        "amount int",
        "term int",
        "submitted datetime",
        "active bool",
    ],
}

TABLES_ON = {
    "users_table": ["user_id"],
    "threads_table": ["user_id"],
    "roles_table": ["role_id"],
    "loans_table": ["role_id", "active", "submitted"],
    "messages_table": ["sender_id", "recipient_id", "time"],
}

TABLE_CONVERT = {
    "users_table": "diplo_member",
    "threads_table": "diplo_thread",
    "roles_table": "diplo_role",
    "loans_table": "diplo_loan",
    "messages_table": "diplo_message",
}


def connect_db():
    conn = pg.connect(
        database=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASS"),
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
    )
    return conn


"""
dbname: the database name
database: the database name (only as keyword argument)
user: user name used to authenticate
password: password used to authenticate
host: database host address (defaults to UNIX socket if not provided)
port: connection port number (defaults to 5432 if not provided)
"""


def execute_sql(sql: str, commit: bool = True, params: list = None):
    """
    Executes an SQL query on the connected database.

    Args:
        :sql The SQL query to execute.
        :commit Whether to commit the transaction after execution.
        :param params:

    Returns:
        None

    """
    try:
        if params is not None:
            CONN.execute(sql, params)
        else:
            CONN.execute(sql)
    except ParserException:
        print(sql)
        raise ParserException()
    if commit:
        CONN.commit()


def get_sql(sql) -> pd.DataFrame:
    """
    Executes an SQL query on the connected database.

    Args:
        sql: The SQL query to execute.

    Returns:
        A pandas DataFrame containing the results of the query.
    """
    try:
        df = CONN.sql(sql).df()
    except ParserException:
        print(sql)
        raise ParserException()
    return df


def create_table(table_name, tuple_of_fields: List[str]):
    execute_sql(
        f"create table if not exists {table_name} ({','.join(tuple_of_fields)})",
        commit=True,
    )


# function that determines if a user has accessed the bot before
def user_lookup(id: str) -> pd.DataFrame:
    sql = f"select user_id, name, nick from users where user_id = {str(id)}"
    df = get_sql(sql)
    return df


def role_lookup(id: str) -> pd.DataFrame:
    sql = f"select role_id, name from roles where role_id = {str(id)}"
    df = get_sql(sql)
    return df


def create_user(id, name, nick):
    sql = f"insert into users (user_id, name, nick) values (?, ?, ?)"
    execute_sql(
        sql,
        commit=True,
        params=[str(id), str(name), str(nick).replace(";", "").replace("'", "")],
    )


def create_role(id, name):
    sql = f"insert into roles (role_id, name) values ('{str(id)}', '{str(name)}')"
    execute_sql(sql, commit=True)


def get_user_inbox(id: str) -> pd.DataFrame:
    sql = f"select user_id, personal_inbox_id, personal_inbox_name from threads where user_id = {str(id)}"
    df = get_sql(sql)
    return df


def create_user_inbox(id, personal_inbox_id, personal_inbox_name):
    sql = f"insert into threads (user_id, personal_inbox_id, personal_inbox_name) values (?, ?, ?)"
    execute_sql(
        sql,
        commit=True,
        params=[str(id), str(personal_inbox_id), str(personal_inbox_name)],
    )


def update_user_inbox(id, personal_inbox_id, personal_inbox_name):
    sql = f"""
    update threads
    set personal_inbox_id = ?, personal_inbox_name = ?
    where user_id = '{str(id)}'
    """
    execute_sql(
        sql,
        commit=True,
        params=[str(personal_inbox_id), str(personal_inbox_name)],
    )


def create_message(send_id, recp_id, timestp, message):
    sql = f"""
    insert into messages (sender_id, recipient_id, time, message) 
    values (?, ?, ?, ?)
    """
    execute_sql(
        sql,
        commit=True,
        params=[
            str(send_id),
            str(recp_id),
            int(timestp),
            str(message.replace("'", "").replace(";", "")),
        ],
    )


def check_message_time(send_id, recp_id, chk_time, gap) -> int | None:
    sql = f"select coalesce(max(time), 1) as mx_tim from messages where sender_id = '{str(send_id)}' and recipient_id = '{str(recp_id)}' and time > {int(chk_time) - int(gap)}"
    df = get_sql(sql)
    mx_tim = df.iloc[0].to_dict()["mx_tim"]
    if mx_tim == 1:
        return None
    else:
        return mx_tim + gap


def sync_table(table: str, cols: list, on: list):
    conn = connect_db()

    data = CONN.sql(f"select * from {table}").fetchall()
    cur = conn.cursor()
    cur.execute("BEGIN")
    # create tmp table
    cur.execute(
        f"create table tmp_{table} as select * from {TABLE_CONVERT[table]} where 1=0"
    )
    # load data
    cur.execute(
        f"insert into tmp_{table} ({','.join(cols)}) values (?, ?, ?)",
        vars=data,
    )
    # upsert
    ons = [f"u.{c} = tu.{c}" for c in on]
    up_cols = [f"{c} = tu.{c}" for c in cols]
    sql = f"""
        MERGE INTO {TABLE_CONVERT[table + "_table"]} as u
        USING tmp_{table} tu
        ON {",".join(ons)}
        WHEN MATCHED THEN
            UPDATE SET 
                {up_cols}
        WHEN NOT MATCHED THEN
            INSERT ({','.join(cols)})
            VALUES ({','.join([f'tu.{col}' for col in cols])}) 
    """
    cur.execute(sql)
    # delete temp table
    cur.execute(f"drop table tmp_{table}")
    conn.commit()


def sync_all_tables():
    for table in ("users", "roles", "threads", "loans"):
        sync_table(
            table,
            TABLES[table + "_table"],
            TABLES_ON[table + "_table"],
        )


def sync_messages():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("select max(time) from diplo_message")
    mx = cur.fetchone()[0]
    if mx is None:
        mx = 0
    data = CONN.sql(f"select * from messages where time > {mx} ").fetchall()
    cur.execute("BEGIN")
    # create tmp table
    cur.execute(f"create table tmp_message as select * from diplo_message where 1=0")
    # load data
    cur.execute(
        f"insert into tmp_message (sender_id, recipient_id, time, message) values (?, ?, ?, ?)",
        vars=data,
    )
    # upsert
    sql = f"""
            MERGE INTO diplo_message as u
            USING tmp_message tu
            ON u.sender_id = tu.sender_id and u.recipient_id = tu.recipient_id and u.time = tu.time
            WHEN NOT MATCHED THEN
                INSERT (sender_id, recipient_id, time, message)
                VALUES (tu.sender_id, tu.recipient_id, tu.time, tu.message) 
        """
    cur.execute(sql)
    # delete temp table
    cur.execute(f"drop table tmp_message")
    conn.commit()


def initialize():
    # function for running all the create stable statements
    for table, name in (
        (TABLES["users_table"], "users"),
        (TABLES["threads_table"], "threads"),
        (TABLES["roles_table"], "roles"),
        (TABLES["messages_table"], "messages"),
        (TABLES["loans_table"], "loans"),
    ):
        create_table(name, table)


if __name__ == "__main__":
    initialize()
