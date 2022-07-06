"""
Parse dependent and independent variables into two separate .csv files
This file can be run from command line without arguments

Saves "dependent_variables.csv" and "independent_variables.csv"
to "data" subdirectory of current working directory.
"""

import pandas as pd
import os


# Create directory path variables
path = os.path.join(os.getcwd(), "data")
dep_path = os.path.join(path, "dependent-vars")
ind_path = os.path.join(path, "independent-vars")


# Dateparser function for abbrev. month, two-digit year
def dateparser(date):
    try:
        return pd.to_datetime(date, format="%b-%y")
    except ValueError:
        return pd.to_datetime(date, format="%y-%b")


# Dateparser function for independent data (except )
def ind_dateparser(date):
    try:
        return pd.to_datetime(date, infer_datetime_format=True)
    except ValueError:
        return dateparser(date)


def pop_date_parser(date):
    return pd.to_datetime(date, format="%Y")


# Parse dependent variables
dep_data = pd.DataFrame({"date": []}).set_index("date")

for f in os.listdir(dep_path):
    if (
        f == "average-price-sqft.csv"
        or f == "closed-sales.csv"
        or f == "homes-for-sale.csv"
        or f == "median-price-sqft.csv"
    ):
        print(f)
        df = pd.read_csv(
            os.path.join(dep_path, f), date_parser=dateparser, parse_dates=["date"]
        )
        dep_data = dep_data.join(df.set_index("date"), how="outer")
    elif f == "months-supply-updated.csv":
        print(f)
        df = pd.read_csv(
            os.path.join(dep_path, f),
            names=["month", "2017", "2018", "2019", "2020", "2021", "2022"],
            skiprows=1,
        )
        # Rearrange dataframe: all months to one column
        df = df.melt(
            id_vars="month",
            value_vars=["2017", "2018", "2019", "2020", "2021", "2022"],
            var_name="year",
            value_name="months_supply",
        )
        # Create datetime index and add to DataFrame
        dates = df.month + df.year
        df["date"] = pd.to_datetime(dates, format="%b%Y")
        df = df.drop(["year", "month"], axis=1).set_index("date").dropna(how="any")
        # Merge monthly supply to main dependent data DataFrame
        dep_data = dep_data.join(df, on="date", how="outer")
    else:
        print(f"{f} was not parsed.")
# Save dependent data to csv file
dep_data.to_csv("data/dependent_variables.csv")


# Parse independent variables
ind_data = pd.DataFrame({"date": []})

for f in os.listdir(ind_path):
    # Parse fed-funds-rate.csv, median-income.csv, unemployment-rate.csv
    if (
        f.endswith(".csv")
        and not (f.startswith("population-growth"))
        and not (f.startswith("rate"))
    ):
        print(f)
        try:
            df = pd.read_csv(
                os.path.join(ind_path, f),
                date_parser=ind_dateparser,
                parse_dates=["date"],
            )
            ind_data = ind_data.merge(df, on=["date"], how="outer")
        except ValueError:
            pass
    # Parse population-growth.csv
    elif f == "population-growth.csv":
        print(f)
        df = pd.read_csv(
            os.path.join(ind_path, f),
            skiprows=1,
            names=["date", "population", "growth", "growth_rate"],
            dtype={"date": str},
            date_parser=pop_date_parser,
            parse_dates=["date"],
        )
        ind_data = ind_data.merge(df, on=["date"], how="outer")

    # Parse rate-of-interest.csv
    elif f.startswith("rate"):
        print(f)
        df = pd.read_csv(os.path.join(ind_path, f))
        # Rearrange DataFrame: all months to one column
        df = df.melt(
            id_vars="Year",
            value_vars=df.columns[1:13],
            var_name="month",
            value_name="rate_of_inflation",
        )
        # Create datetime column
        dates = df.Year.astype("str") + df.month
        df["date"] = pd.to_datetime(dates, format="%Y%b")
        df = df.drop(["Year", "month"], axis=1).dropna(how="any")
        # Merge to main independent data DataFrame
        ind_data = ind_data.merge(
            df[["date", "rate_of_inflation"]], on="date", how="outer"
        )
    else:
        print(f"{f} was not parsed.")

# Sort data, drop NaN vals
ind_data = ind_data.sort_values(by="date").reset_index(drop=True)
ind_data.dropna(subset=["date"], how="all", inplace=True)
# Fill missing monthly data for annual-only data
ind_data.fillna(method="ffill", limit=11, inplace=True)
# Save independent data to .csv file
ind_data.to_csv("data/independent_variables.csv")
