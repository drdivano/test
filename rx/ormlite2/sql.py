# -*- coding: utf-8 -*-
from datetime import date
from dbop import sql_quote, sql_repr


if __name__ == "__main__":
    assert sql_repr(1) == "'1'"
    assert sql_repr("ne'ban") == "'ne''ban'"
    print sql_repr((1, 2, 3, 4, 'a', 'b', date.today()))

