"""Betfair API wrapper."""

from __future__ import annotations

import tempfile
import os
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, List, Sequence

import betfairlightweight


class Betfair:
    """A class for interacting with the Betfair API."""

    def __init__(self, username: str, password: str, app_key: str, certs: str):
        self.trading = betfairlightweight.APIClient(
            username=username,
            password=password,
            app_key=app_key,
            certs=certs,
        )
        self.trading.login()

    def open_bets(self) -> List[str]:
        """Get current open bets."""
        return [
            {
                "average_price_matched": o.average_price_matched,
                "bet_id": o.bet_id,
                "bsp_liability": o.bsp_liability,
                "customer_order_ref": o.customer_order_ref,
                "customer_strategy_ref": o.customer_strategy_ref,
                "handicap": o.handicap,
                "market_id": o.market_id,
                "matched_date": o.matched_date.isoformat(),
                "order_type": o.order_type,
                "persistence_type": o.persistence_type,
                "placed_date": o.placed_date.isoformat(),
                "price": o.price_size.price,
                "size": o.price_size.size,
                "regulator_auth_code": o.regulator_auth_code,
                "regulator_code": o.regulator_code,
                "selection_id": o.selection_id,
                "side": o.side,
                "size_cancelled": o.size_cancelled,
                "size_lapsed": o.size_lapsed,
                "size_matched": o.size_matched,
                "size_remaining": o.size_remaining,
                "size_voided": o.size_voided,
                "status": o.status,
                "current_item_description": o.current_item_description,
            }
            for o in self.trading.betting.list_current_orders().orders
        ]

    def settled_bets(self) -> List[str]:
        """Get settled bets."""

        return [
            {
                "bet_count": o.bet_count,
                "bet_id": o.bet_id,
                "bet_outcome": o.bet_outcome,
                "customer_order_ref": o.customer_order_ref,
                "customer_strategy_ref": o.customer_strategy_ref,
                "event_id": o.event_id,
                "event_type_id": o.event_type_id,
                "handicap": o.handicap,
                "last_matched_date": o.last_matched_date.isoformat(),
                "market_id": o.market_id,
                "order_type": o.order_type,
                "persistence_type": o.persistence_type,
                "placed_date": o.placed_date.isoformat(),
                "price_matched": o.price_matched,
                "price_reduced": o.price_reduced,
                "price_requested": o.price_requested,
                "profit": o.profit,
                "commission": o.commission,
                "selection_id": o.selection_id,
                "settled_date": o.settled_date.isoformat(),
                "side": o.side,
                "size_settled": o.size_settled,
                "size_cancelled": o.size_cancelled,
                "item_description": o.item_description,
            }
            for o in self.trading.betting.list_cleared_orders().orders
        ]

    def place_bet(
        self,
        market_id: str,
        selection_id: int,
        stake: float,
        price: float,
        option: str = "back",
    ):  # pylint: disable=too-many-arguments
        """Place a bet."""
        limit_order = betfairlightweight.filters.limit_order(
            size=round(stake, 2),
            price=price,
            persistence_type="LAPSE",
        )
        instruction = betfairlightweight.filters.place_instruction(
            order_type="LIMIT",
            selection_id=int(selection_id),
            side=option.upper(),
            limit_order=limit_order,
        )
        res = self.trading.betting.place_orders(
            market_id=market_id,
            instructions=[instruction],
        )

        if res.status == "FAILURE":
            raise ValueError(f"{res.status}: {res.error_code}")

        report = res.place_instruction_reports[0]
        return {
            "bet_id": report.bet_id,
            "placed_date": report.placed_date.isoformat(),
            "average_price_matched": report.placed_date.isoformat(),
            "size_matched": report.size_matched,
            "status": report.status,
        }

    def bankroll(self) -> float:
        """Get current available."""
        return self.trading.account.get_account_funds().available_to_bet_balance

    def markets(
        self,
        competition_ids: Sequence[int],
        max_results: int = 1000,
        max_days_left: int = 7,
    ) -> Generator[Dict[str, Any]]:
        """Get open markets from specified competitions."""
        for competition_id in competition_ids:
            # Filter match odds only for the competitions I am interested in.
            market_filter = betfairlightweight.filters.market_filter(
                market_type_codes=["MATCH_ODDS"],
                competition_ids=[competition_id],
            )

            # Get market data.
            market_catalogues = self.trading.betting.list_market_catalogue(
                market_projection=[
                    "COMPETITION",
                    "MARKET_DESCRIPTION",
                    "RUNNER_DESCRIPTION",
                    "EVENT",
                ],
                filter=market_filter,
                max_results=max_results,
            )

            # Flatten market data.
            for mkt in market_catalogues:
                # Check date limit.
                lim = datetime.now() + timedelta(days=max_days_left)
                if mkt.description.market_time > lim:
                    continue

                yield {
                    "event_name": mkt.event.name,
                    "market_id": mkt.market_id,
                    "market_name": mkt.market_name,
                    "market_start_time": mkt.market_start_time,
                    "total_matched": mkt.total_matched,
                    "competition_id": int(mkt.competition.id),
                    "competition_name": mkt.competition.name,
                    "market_base_rate": mkt.description.market_base_rate / 100,
                    "market_time": mkt.description.market_time,
                    "suspend_time": mkt.description.suspend_time,
                }

    def books(
        self,
        competition_ids: Sequence[int],
        max_markets: int = 1000,
    ) -> Generator[Dict[str, Any]]:
        """Get books for an specific market."""
        for market in self.markets(competition_ids, max_markets):
            # Get the books for each market id.
            market_book = self.trading.betting.list_market_book(
                market_ids=[market["market_id"]],
                price_projection={"priceData": ["EX_BEST_OFFERS"]},
            )[0]

            # Check if market is open and avoid inplay.
            if market_book.status != "OPEN" or market_book.inplay:
                continue

            # Flatten back and lay separately.
            back = [
                {
                    "event_name": market["event_name"],
                    "market_id": market["market_id"],
                    "market_time": market["market_time"],
                    "competition_id": market["competition_id"],
                    "competition_name": market["competition_name"],
                    "selection_id": r.selection_id,
                    "option": "back",
                    "price": r.ex.available_to_back[0].price,
                    "size": r.ex.available_to_back[0].size,
                    "market_rate": market["market_base_rate"],
                }
                for r in market_book.runners
                if r.ex.available_to_lay
            ]
            lay = [
                {
                    "event_name": market["event_name"],
                    "market_id": market["market_id"],
                    "market_time": market["market_time"],
                    "competition_id": market["competition_id"],
                    "competition_name": market["competition_name"],
                    "selection_id": r.selection_id,
                    "option": "lay",
                    "price": r.ex.available_to_lay[0].price,
                    "size": r.ex.available_to_lay[0].size,
                    "market_rate": market["market_base_rate"],
                }
                for r in market_book.runners
                if r.ex.available_to_lay
            ]
            if len(back + lay) > 0:
                yield back + lay


def login(username: str, password: str, app_key: str, certificate: str, key: str) -> betfair.Betfair:
    """Login on betfair API"""

    # Create a temp certificates folder to avoid data leakage.
    with tempfile.TemporaryDirectory() as tmp:
        cert_path = os.path.join(tmp, "client-2040.crt")
        with open(cert_path, mode="w", encoding="utf-8") as file:
            file.write(certificate)

        key_path = os.path.join(tmp, "client-2040.key")
        with open(key_path, mode="w", encoding="utf-8") as file:
            file.write(key)

        return Betfair(
            username=username,
            password=password,
            app_key=app_key,
            certs=tmp,
        )
