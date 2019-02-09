# -*- coding: utf-8 -*-
"""
Created on Fri Jan 18 16:06:52 2019

@author: demerss1
"""

import lifelines as lf
import database_setup as db
import pandas as pd
from typing import Tuple, List


def censor(df: pd.DataFrame,
           params: Tuple[str] = ("Completed", "Deleted", "Accpeted")
           ) -> pd.DataFrame:
    """
    This trims all uncessary attributes
    Keeps Completed, Accpeted, Deleted
    """
    def censor_col(row):
        if row == "Completed" or row == "Accepted" or row == "Deleted":
            row = 1
        else:
            row = 0
        return row
    if params != "all":
        df = df[df.FinalStatus.isin(params)].copy()
    df.FinalStatus = df.FinalStatus.apply(censor_col)
    return df


def graph(T: pd.Series, C: pd.Series, df: pd.DataFrame,
          gtype: str, ntype: str = '', params: Tuple[str] = ()):
    """
    plots 1 graph with each nurse type against each other
    Requried T, C each are Pandas series
    T = Duration or time and C = censored or not
    gtype is the graph type also the title
    """
    kmf = lf.KaplanMeierFitter()
    groups = df[gtype]
    var_dict = {}
    for var in df[gtype].unique():
        var_name = var.lower()
        var_dict[var_name] = (groups == var)
        kmf.fit(T[var_dict[var_name]], C[var_dict[var_name]], label=var)
        if len(var_dict) == 1:
            ax = kmf.survival_function_.plot()
        else:
            ax = kmf.survival_function_.plot(ax=ax)
    if ntype == '' and params != ():
        ax.set_title(f"Survival curve by {gtype} {params}")
    elif ntype != '' and params == ():
        ax.set_title(f"Survival curve by {ntype} {gtype}")
    elif ntype == '':
        ax.set_title(f"Survival curve by {gtype}")
    else:
        ax.set_title(f"Survival curve by {ntype} {gtype} {params}")
    ax.set_xlabel("Days")
    ax.set_ylabel("Probability")


def graph_batch_market(T: pd.Series, C: pd.Series, df: pd.DataFrame,
                       ntype: str, gtype: List[str] = ["Market", "State"],
                       params: Tuple[str] = ()) -> None:
    """
    Makes main look nice
    """
    for g in gtype:
        graph(T, C, df, gtype=g, ntype=ntype, params=params)


if __name__ == "__main__":
    """
    Start to invetigate data and attributes of plot functions
    Make some functions to make plots easy
    Survival functions and confidence interval have been var_dicted
    you can start adding titles using plt
    """

    rate = db.adv_query("rate")
    rate = db.duration(rate, "days")
    rate = censor(rate)
    print("Params =", "Completed, Accepted, Deleted")
    T = rate.Duration
    C = rate.FinalStatus
    graph(T, C, rate,  "IntelyProType")
    graph(T, C, rate, "Market")
    graph(T, C, rate, "State")
