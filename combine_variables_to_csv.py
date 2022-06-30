from ast import parse
import pandas as pd
import os

path = os.path.join(os.getcwd(), "data")

dep_data = pd.DataFrame({"date": []}).set_index("date")
dep_path = os.path.join(path, "dependent-vars")
ind_path = os.path.join(path, "independent-vars")


def dateparser(date):
    try:
        return pd.to_datetime(date, format="%b-%y")
    except ValueError:
        return pd.to_datetime(date, format="%y-%b")


def ind_dateparser(date):
    try:
        return pd.to_datetime(date, infer_datetime_format=True)
    except ValueError:
        return dateparser(date)


def pop_date_parser(date):
    return pd.to_datetime(date, format="%Y")


for f in os.listdir(dep_path):
    if f.endswith(".csv"):
        df = pd.read_csv(
            os.path.join(dep_path, f), date_parser=dateparser, parse_dates=["date"]
        )
        dep_data = dep_data.join(df.set_index("date"), how="outer")
dep_data.to_csv("data/dependent_variables.csv")

ind_data = pd.DataFrame({"date": []})

for f in os.listdir(ind_path):
    if (
        f.endswith(".csv")
        and not (f.startswith("population-growth"))
        and not (f.startswith("rate"))
    ):
        # print(f)
        try:
            df = pd.read_csv(
                os.path.join(ind_path, f),
                date_parser=ind_dateparser,
                parse_dates=["date"],
            )
            ind_data = ind_data.merge(df, on=["date"], how="outer")
            # print(ind_data.head())
        except ValueError:
            pass

    elif f == "population-growth.csv":
        df = pd.read_csv(
            os.path.join(ind_path, f),
            skiprows=1,
            names=["date", "population", "growth", "growth_rate"],
            dtype={"date": str},
            date_parser=pop_date_parser,
            parse_dates=["date"],
        )
        ind_data = ind_data.merge(df, on=["date"], how="outer")
    elif f.startswith("rate"):
        df = pd.read_csv(os.path.join(ind_path, f))
        df = df.melt(id_vars="Year", value_vars=df.columns[1:13])
        df = df.rename(
            columns={"Year": "year", "variable": "month", "value": "interest_rate"}
        )
        dates = (
            df.iloc[:, :2].join(pd.Series("01", index=df.index, name="day")).astype(str)
        )
        dates["date"] = dates.year + dates.month + dates.day
        df["date"] = pd.to_datetime(dates["date"], format="%Y%b%d")
        df["date"] = pd.to_datetime(dates["date"], format="%Y%b%d")
        ind_data = ind_data.merge(df[["date", "interest_rate"]], on="date")
    else:
        print(f"{f} was not parsed.")


ind_data = ind_data.sort_values(by="date").reset_index(drop=True)
ind_data.dropna(subset=["date"], how="all", inplace=True)
ind_data.fillna(method="ffill", limit=11, inplace=True)
ind_data.to_csv("data/independent_variables.csv")
