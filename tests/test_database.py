import pandas as pd
import pytest

from util.database import (
    create_user,
    create_user_inbox,
    get_user_inbox,
    user_lookup,
)


@pytest.mark.parametrize("id", ["user1", "user2"])
def test_user_lookup(id):
    df = user_lookup(id)
    assert isinstance(df, pd.DataFrame)


@pytest.mark.parametrize(
    "id, name, nick",
    [("user3", "John Doe", "johndoe"), ("user4", "Jane Doe", "janedoe")],
)
def test_create_user(id, name, nick):
    create_user(id, name, nick)
    rth = user_lookup(str(id))
    assert rth.shape[0] == 0


@pytest.mark.parametrize("id", ["user1", "user2"])
def test_get_user_inbox(id):
    df = get_user_inbox(id)
    assert isinstance(df, pd.DataFrame)


@pytest.mark.parametrize(
    "id, personal_inbox_id, personal_inbox_name",
    [("user3", "inbox3", "Inbox 3"), ("user4", "inbox4", "Inbox 4")],
)
def test_create_user_inbox(id, personal_inbox_id, personal_inbox_name):
    create_user_inbox(id, personal_inbox_id, personal_inbox_name)
    rth = get_user_inbox(str(id))
    assert rth.shape[0] == 0
