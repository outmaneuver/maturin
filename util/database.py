from typing import List

import duckdb
import pandas as pd
from duckdb.duckdb import ParserException
from datetime import datetime

# init database connection on load

CONN = duckdb.connect("hsku_local.duckdb")


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
    sql = f"insert into users (user_id, name, nick) values ('{str(id)}', '{str(name)}', '{str(nick)}')"
    execute_sql(sql, commit=True)


def create_role(id, name):
    sql = f"insert into roles (role_id, name) values ('{str(id)}', '{str(name)}')"
    execute_sql(sql, commit=True)


def get_user_inbox(id: str) -> pd.DataFrame:
    sql = f"select user_id, personal_inbox_id, personal_inbox_name from threads where user_id = {str(id)}"
    df = get_sql(sql)
    return df


def create_user_inbox(id, personal_inbox_id, personal_inbox_name):
    sql = f"insert into threads (user_id, personal_inbox_id, personal_inbox_name) values ('{str(id)}', '{str(personal_inbox_id)}', '{str(personal_inbox_name)}')"
    execute_sql(sql, commit=True)


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


def initialize():
    # function for running all the create stable statements
    users_table = ["user_id varchar unique", "name varchar", "nick varchar"]
    threads_table = [
        "user_id varchar unique",
        "personal_inbox_id varchar",
        "personal_inbox_name varchar",
    ]
    roles_table = ["role_id varchar unique", "name varchar"]
    messages_table = [
        "sender_id varchar",
        "recipient_id varchar",
        "time int",
        "message varchar",
    ]

    for table, name in (
        (users_table, "users"),
        (threads_table, "threads"),
        (roles_table, "roles"),
        (messages_table, "messages"),
    ):
        create_table(name, table)


if __name__ == "__main__":
    initialize()
