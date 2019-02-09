# -*- coding: utf-8 -*-
"""
Created on Sat Jan 12 14:43:45 2019

@author: demerss1
"""

import pandas as pd
import datetime as dt
import sqlite3
from typing import List


def strip_time(df: pd.DataFrame) -> pd.DataFrame:
    """
    strips the time way from any columns with dates in them
    """
    subset = df.select_dtypes(include=["object"])
    for col in subset.columns:
        new_col = []
        temp = subset[col]
        for item in temp.iteritems():
            try:
                date = dt.datetime.strptime(item[1], "%m/%d/%Y %H:%M%S")
                new_col.append(date.strftime("%Y-%m-%d"))
            except ValueError:
                new_col.append(item[1])
        df[col] = new_col
    return df.copy()


def upload(df: pd.DataFrame, table_name: str,
           table_exists: str = 'replace') -> None:
    """
    Uploads dataframe to database with table_name
    Will replace by default
    """
    with sqlite3.connect("IntelyCare.db") as con:
        df.to_sql(table_name, con, if_exists=table_exists, index=False)


def get_tnames() -> List[str]:
    """
    Gets all table names as a List[str]
    """
    query = "SELECT name FROM sqlite_master WHERE type='table'"
    with sqlite3.connect('IntelyCare.db') as con:
        df = pd.read_sql(query, con)
    return list(df["name"])


def drop_t(table_name: str) -> None:
    """
    Quick function to drop one table
    If you want all tables input all
    becuase of this do not name a table all
    """
    if table_name == "all":
        names = get_tnames()
        with sqlite3.connect('IntelyCare.db') as con:
            for name in names:
                query = f"DROP TABLE IF EXISTS '{name}'"
                con.cursor().execute(query)
    else:
        query = f"DROP TABLE IF EXISTS {table_name}"
        with sqlite3.connect("IntelyCare.db") as con:
            con.cursor().execute(query)


def query(query: str) -> pd.DataFrame:
    """
    Used so you dont have to worry about opening database.
    Just insert sql query and get a dataframe of that query.
    """
    with sqlite3.connect("IntelyCare.db") as con:
        df = pd.read_sql(query, con)
    return df


def duration(df: pd.DataFrame, dur: str = "days") -> pd.DataFrame:
    """
    Populates duration column as days as default
    acceptable durations are days, hours, mins, secs
    """
    def make_new_row(row, multi) -> float:
        new_row = 0
        new_row += int(row[0:row.index('days') - 1])
        new_row += int(row[row.index(",") + 2:row.index("hours") - 1]) / 24
        new_row += int(row[row.index(",", row.index("hours")) +
                           2: row.index("minutes") - 1]) / (24 * 60)
        return multi * new_row
    if dur == "days":
        multi = 1
    elif dur == "hours":
        multi = 24
    elif dur == "mins":
        multi = 24 * 60
    elif dur == "secs":
        multi = 24 * 60 * 60
    df.Duration = df.Duration.apply(make_new_row, args=(multi,))
    return df


def adv_query(table_name: str, columns: List[str] = None,
              conds: List[str] = None) -> pd.DataFrame:
    """
    advance query, if you have data to query for
    Makes simple queries easy with optional args
    required table_name optional columns and conditions/conds
    columns = list of column names
    conds = List[str] for each WHERE condition
    example conds = ["Datecreated > 2018-7-29"]
    """
    if columns is None:
        query = f"""
                 SELECT *
                 FROM '{table_name}'
                 """
    else:
        query = f"""
                 SELECT {",".join(columns)}
                 FROM '{table_name}'
                 """
    if conds is not None:
        query += "WHERE "
        for cond in conds:
            print(cond)
            query += f"{cond} AND "
        query = query[0:query.rindex('AND')]
        print(query)
    with sqlite3.connect("IntelyCare.db") as con:
        df = pd.read_sql(query, con)
    return df


def join(table_name: str = "join", rate: str = "rate",
         shift: str = "shift") -> None:
    query = f"""
            SELECT s.RequestID, s.CNA, s.LPN, s.RN, s.UserGroup,
                   r.CareDateTime, r.RequestHours, r.TimeGroup, r.ShiftTimeID,
                   r.IntelyProType, r.LastMinuteShift, r.Holiday,
                   r.HolidayBilling, r.FinalStatus, r.FinalStatusTime,
                   r.ClientID, r.Market, r.State, r.City, r.ZipCode,
                   r.ClientActivationTime, r.DateCreated, r.Duration
            FROM {shift} as s
            JOIN {rate} as r
            ON s.RequestID == r.RequestID
            """
    with sqlite3.connect("IntelyCare.db") as con:
        df = pd.read_sql(query, con)
        upload(df, table_name)


def skimData(df: pd.DataFrame) -> pd.DataFrame:
    """
    returns the rate table with Duration < 15 days
    """
    dur = list(df.Duration)
    drop_list = []
    for i in range(len(dur)):
        val = int(dur[i][0: dur[i].index(" days")])
        if val > 14:
            drop_list.append(i)
    df = df.drop(drop_list)
    return df.copy()


def df_status(df: pd.DataFrame, col: str = "FinalStatus") -> None:
    """
    gives tha mount of data in whatever column you want
    default is FinalStatus
    """
    print(f"column = {col}")
    print(f"DataFrame length = {len(df)}")
    for status in df[col].unique():
        print(f"The amount of {status} = {len(df[df[col] == status])}")


if __name__ == "__main__":
    # refresh database
    drop_t('all')
    rate = pd.read_csv("Data Set with time duration.csv")
    shift = pd.read_csv("shift_rate_history_sample.csv")
    shift = strip_time(shift)
    rate = strip_time(rate)
    upload(shift, "shift")
    upload(rate, "rate")
    join()
    skimRate = skimData(rate)
    upload(skimRate, "rate_s")
    join(table_name="join_s", rate="rate_s")
