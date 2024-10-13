"""Bot for automated value betting on BetFair."""

import tempfile
import logging
import os

import pandas as pd

import betfair
from betfair import betting
import betfairlightweight
from betfairlightweight import filters

# Competitions
COMPS = ["10932509"]  # English Premier League

# Constants
THIS_DIR = os.path.dirname(__file__)
MIN_BET = 5.0
MIN_LIABILITY = MIN_BET * 5
MIN_EV = 0.0
FRACTION = 0.25

# Auth
USERNAME = os.getenv("BETFAIR_USERNAME")
PASSWORD = os.getenv("BETFAIR_PASSWORD")
APP_KEY = os.getenv("BETFAIR_APP_KEY")
CERTIFICATE = os.getenv("BETFAIR_CERTIFICATE")
KEY = os.getenv("BETFAIR_KEY")

# Config
logging.getLogger().setLevel(logging.INFO)
pd.options.mode.chained_assignment = None


def login(
    username: str,
    password: str,
    app_key: str,
    certificate: str,
    key: str,
) -> betfairlightweight.APIClient:
    """Login on betfair API"""

    # Create a temp certificates folder to avoid data leakage.
    with tempfile.TemporaryDirectory() as tmp:
        cert_path = os.path.join(tmp, "client-2040.crt")
        with open(cert_path, mode="w", encoding="utf-8") as file:
            file.write(certificate)

        key_path = os.path.join(tmp, "client-2040.key")
        with open(key_path, mode="w", encoding="utf-8") as file:
            file.write(key)

        client = betfairlightweight.APIClient(
            username=username,
            password=password,
            app_key=app_key,
            certs=tmp,
        )
        client.login()
        return client


def load_proba():
    """Load probabilities."""
    df = pd.read_json("soccer_epl.json")
    df_lookup = pd.read_json(
        "theoddsapi2betfair.json",
        # dtype={
        #     "the_odds_api_sport_key": "string",
        #     "the_odds_api_team": "string",
        #     "betfair_competition_id": "string",
        #     "betfair_selection_id": "int",
        # },
    )

    keys = []
    for key in ["bookmakers", "markets", "outcomes"]:
        keys.append(key)
        col = ".".join(keys)
        df = pd.json_normalize(df.explode(col).to_dict(orient="records"))
        if f"{col}.last_update" in df.columns:
            df[f"{col}.last_update"] = pd.to_datetime(df[f"{col}.last_update"])
            df = df.convert_dtypes()

    return df.merge(
        df_lookup,
        left_on=["sport_key", "bookmakers.markets.outcomes.name"],
        right_on=["the_odds_api_sport_key", "the_odds_api_team"],
        how="left",
        validate="many_to_one",
    )


def unmatched():
    client = login(
        username=USERNAME,
        password=PASSWORD,
        app_key=APP_KEY,
        certificate=CERTIFICATE,
        key=KEY,
    )

    # Get selection_id from all runners
    # Do it one competition at time to avoid hitting max results limits.

    df_selection = (
        pd.DataFrame.from_records(
            [
                {
                    "competition_id": int(market.competition.id),
                    "competition_name": market.competition.name,
                    "selection_id": runner.selection_id,
                    "runner_name": runner.runner_name,
                }
                for comp in COMPS
                for market in client.betting.list_market_catalogue(
                    market_projection=["COMPETITION", "RUNNER_DESCRIPTION"],
                    filter=filters.market_filter(
                        market_type_codes=["MATCH_ODDS"],
                        competition_ids=[comp],
                    ),
                    max_results=1000,
                )
                for runner in market.runners
            ]
        )
        .convert_dtypes()
        .groupby(["competition_name", "runner_name"], as_index=False)
        .last()
    )

    df_proba = load_proba()
    return (
        df_selection.merge(
            df_proba,
            how="left",
            left_on=["competition_id", "selection_id"],
            right_on=["betfair_competition_id", "betfair_selection_id"],
            indicator=True,
            validate="one_to_many",
        )
        .query("_merge == 'left_only'")
        .filter(["competition_id", "competition_name", "selection_id", "runner_name"])
        .to_dict(orient="records")
    )


# def main(*args, **kwargs):  # pylint: disable=unused-argument
#     """Main execution."""
#     # Login on Betfair API.
#     trading = betfair.login(
#         username=os.getenv("BETFAIR_USERNAME"),
#         password=os.getenv("BETFAIR_PASSWORD"),
#         app_key=os.getenv("BETFAIR_APP_KEY"),
#         certificate=os.getenv("BETFAIR_CERTIFICATE"),
#         key=os.getenv("BETFAIR_KEY"),
#     )

#     # Check and store markets that had already received a bet.
#     already_bet = [o["market_id"] for o in trading.open_bets()]
#     logging.info("There are %s open bets", len(already_bet))

#     df_proba = load_proba()

#     bets = []
#     # for book in trading.books(df_proba["competition_id"].unique().astype(int).tolist()):
#     for book in trading.markets(competition_ids=[10932509]):

#         # Load data into a DataFrame.
#         df_book = (
#             pd.DataFrame().from_records(book).convert_dtypes().query("option == 'back'")
#         )
#         pass

#         # Extract date for Pacific Time to match FiveThirtyEight timezone.
#         df_book["market_time"] = pd.to_datetime(df_book["market_time"], utc=True)
#         df_book["date"] = (
#             df_book["market_time"]
#             .dt.tz_convert("America/Los_Angeles")
#             .dt.date.astype(pd.StringDtype())
#         )

#         # Make sure data is in a serializable format
#         df_book["market_time"] = df_book["market_time"].apply(lambda x: x.isoformat())

#         # Merge Betfair and FiveThirtyEight data.
#         merge_on = ["event_name", "date", "competition_id", "selection_id"]
#         df_bets = df_book.merge(df_proba, on=merge_on, suffixes=("", "_proba"))

#         # If merge failed, move on.
#         if df_bets.shape[0] == 0:
#             logging.info(
#                 "%s - %s: No merge",
#                 df_book["competition_name"].iloc[0],
#                 df_book["event_name"].iloc[0],
#             )
#             continue

#         # Invert probability for laying calculation.
#         df_bets["proba"] = df_bets.apply(
#             lambda x: x["proba"] if x["option"] == "back" else 1 - x["proba"],
#             axis=1,
#         )

#         # Calculate percentage from the bankroll to bet.
#         df_bets["kelly"] = df_bets.apply(
#             lambda x: betting.kelly_criterion(
#                 proba=x["proba"],
#                 odds=x["price"],
#                 fraction=FRACTION,
#             ),
#             axis=1,
#         )

#         # Get my bankroll and estimate how much to bet (liability).
#         df_bets["liability"] = df_bets["kelly"].clip(0) * trading.bankroll()

#         # Converts liability to stake.
#         df_bets["stake"] = df_bets.apply(
#             lambda x: x["liability"]
#             if x["option"] == "back"
#             else x["liability"] / (x["price"] - 1),
#             axis=1,
#         ).round(2)

#         # Estimate the expected value for each possible bet on this market.
#         df_bets["ev"] = df_bets.apply(
#             lambda x: betting.expected_value(
#                 stake=x["stake"],
#                 proba=x["proba"],
#                 odds=x["price"],
#                 rate=x["market_rate"],
#                 option=x["option"],
#             ),
#             axis=1,
#         )

#         # Skip if it is an already bet market.
#         if df_bets["market_id"].iloc[0] in already_bet:
#             logging.info(
#                 "%s - %s: Already bet",
#                 df_bets["competition_name"].iloc[0],
#                 df_bets["event_name"].iloc[0],
#             )
#             continue

#         # Filter only bets that are worth it.
#         df_bets = df_bets.query(f"ev > {MIN_EV}")

#         # Filter only bets that are above minimum values.
#         df_bets = df_bets.query(
#             f"option == 'back' and stake >= {MIN_BET}"
#             f"or option == 'lay' and liability >= {MIN_LIABILITY}"
#         )

#         # Continue only if there are still bets left after the filter.
#         if df_bets.shape[0] == 0:
#             logging.info(
#                 "%s - %s: No bet",
#                 df_book["competition_name"].iloc[0],
#                 df_book["event_name"].iloc[0],
#             )
#             continue

#         # Select the best bet.
#         bet = df_bets.loc[df_bets["ev"].idxmax()]

#         logging.info(
#             "%s - %s: %s $%.2f on %s at %.2f",
#             bet["competition_name"],
#             bet["event_name"],
#             bet["option"].capitalize(),
#             bet["stake"],
#             bet["runner_name"],
#             bet["price"],
#         )

#         # Place bet
#         report = trading.place_bet(
#             market_id=bet["market_id"],
#             selection_id=bet["selection_id"],
#             stake=bet["stake"],
#             price=bet["price"],
#             option=bet["option"],
#         )

#         # Include data from the bet
#         bet["placed_at"] = report["placed_date"]
#         bet["bet_id"] = report["bet_id"]
#         bet["status"] = report["status"].lower()

#         # Store bet as a dict to be returned at the end.
#         boto3.client("s3").put_object(
#             Bucket=BUCKET_NAME,
#             Key=f"bets/{bet['bet_id']}.json",
#             Body=bet.to_json(),
#         )
#         bets.append(bet.to_dict())

#     return bets


if __name__ == "__main__":

    import json

    print(json.dumps(unmatched(), indent=2))
