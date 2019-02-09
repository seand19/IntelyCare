# -*- coding: utf-8 -*-
"""
Created on Wed Jan 23 16:28:17 2019

@author: demerss1
"""
import database_setup as db
import plot_setup as ps
from typing import Tuple


def plot_market_by_nurse(params: Tuple[str] = ("Completed", "Accepted",
                         "Deleted"), skim: bool = False) -> None:
    """
    Creates a graphs for each nurse type
    1 will be for ech indivual market and the other will be by state
    """
    if skim:
        df = db.adv_query("rate_s")
    else:
        df = db.adv_query("rate")
    df = db.duration(df, "days")
    df = ps.censor(df, params)
    print("Params =", params)
    for ntype in df.IntelyProType.unique():
        print("Nurse type =", ntype)
        subset = df[df.IntelyProType == ntype]
        T = subset.Duration
        C = subset.FinalStatus
        ps.graph_batch_market(T, C, subset, ntype)


def plot_overview(params: Tuple[str] = ("Completed", "Accepted", "Deleted"),
                  skim: bool = False):
    if skim:
        df = db.adv_query("rate_s")
    else:
        df = db.adv_query("rate")
    df = db.duration(df, "days")
    df = ps.censor(df, params)
    print("Params =", params)
    T = df.Duration
    C = df.FinalStatus
    ps.graph(T, C, df, "IntelyProType")
    ps.graph(T, C, df, "Market")
    ps.graph(T, C, df, "State")


def plot_by_shift_rate(params: Tuple[str] = ("Completed", "Accepted",
                       "Deleted"), skim: bool = False):
    """
    Plots graph for each nurse by shift rate
    """
    if skim:
        df = db.adv_query("join_s")
    else:
        df = db.adv_query("join")
    df = db.duration(df, "days")
    df = ps.censor(df, params)
    print("Params =", params)
    for ntype in df.IntelyProType.unique():
        if ntype == "Nurse":
            continue
        print("Nurse type =", ntype)
        subset = df[df.IntelyProType == ntype]
        pay = sorted([x for x in subset[ntype]])
        while True:
            try:
                pay.remove(0.0)
            except ValueError:
                break
        segment = (pay[-1] - pay[0]) / 3
        for i in range(3):
            sub = subset[(subset[ntype] >= (pay[0] + segment * (i)))
                         & (subset[ntype] <= (pay[0] + segment * (i + 1)))]
            T = sub.Duration
            C = sub.FinalStatus
            ps.graph_batch_market(T, C, sub, ntype, ["Market"],
                                  ("pay is between "
                                   f"{round(pay[0] + segment * (i), 2)}"
                                   " and "
                                   f"{round(pay[0] + segment * (i + 1), 2)}"))


def plot_by_rate(params: Tuple[str] = ("Completed", "Accepted", "Deleted"),
                 skim: bool = False, gtype: str = "IntelyProType") -> None:
    """
    plot by pay rate regardless of nurses
    nurse type will be a group
    """
    if skim:
        df = db.adv_query("join_s")
    else:
        df = db.adv_query("join")
    df = db.duration(df, "days")
    df = ps.censor(df, params)
    print("Params =", params)
    for rate in range(14, 50, 4):
        subset = df[((df.CNA > rate) & (df.CNA < (rate + 4))) |
                    ((df.LPN > rate) & (df.LPN < (rate + 4))) |
                    ((df.RN > rate) & (df.RN < (rate + 4)))]
        if len(subset) == 0:
            print("empty")
            continue
        T = subset.Duration
        C = subset.FinalStatus
        ps.graph(T, C, subset, gtype=gtype,
                 params=f"pay rate = {rate} to {rate + 4}")


if __name__ == "__main__":
    params = ("Completed", "Accepted",
              "Deleted", "Requested")
    # possible status parameters to see
#    ("Completed", "Accepted",
#     "Deleted", "Requested",
#     "PNetwork", "Process",
#     "Transit", "Incompleted")

    plot_market_by_nurse(params=params, skim=True)
    plot_overview(params=params, skim=True)
    plot_by_shift_rate(params=params, skim=True)
    plot_by_rate(params=params, skim=True)
