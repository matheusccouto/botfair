"""Betting strategy functions."""


def kelly_criterion(proba: float, odds: float, fraction: float) -> float:
    """Calculate fraction of bankroll to be bet using Kelly Criterion."""
    if proba < 0 or proba > 1:
        raise ValueError("Event probability must be in between 0 and 1.")
    if odds <= 1:
        raise ValueError("Odds must be greater than 1.")
    if fraction < 0 or fraction > 1:
        raise ValueError("Kelly's fraction must be in between 0 and 1.")
    return fraction * (odds * proba - (1 - proba)) / odds


def _ev(proba: float, profit: float, loss: float) -> float:
    """Basic expected value formula."""
    if proba < 0 or proba > 1:
        raise ValueError("Event probability must be in between 0 and 1.")
    return proba * profit - (1 - proba) * loss


def _ev_back(stake: float, proba: float, odds: float, rate: float):
    """Expected value for backing."""
    return _ev(
        proba=proba,  # Event success probability.
        profit=stake * (odds - 1) * (1 - rate),  # Profit without stake and rake.
        loss=stake,  # The maximum you can losw while backing is your stake.
    )


def _ev_lay(stake: float, proba: float, odds: float, rate: float):
    """Expected value for laying."""
    return _ev(
        proba=proba,  # Event failure probability.
        profit=stake * (1 - rate),  # Stake from the backer less the rake.
        loss=stake * (odds - 1),  # Liability.
    )


def expected_value(
    stake: float,
    proba: float,
    odds: float,
    rate: float,
    option: str,
) -> float:
    """Expected value for back or lay."""
    if option not in ["back", "lay"]:
        raise ValueError(
            f"Invalid option '{option}'. Accepted values are 'back' and 'lay'"
        )

    if odds <= 1:
        raise ValueError("Odds must be greater than 1.")

    if rate < 0 or rate > 1:
        raise ValueError("Rate must be in between 0 and 1.")

    if "back" in option.lower():
        return _ev_back(stake=stake, proba=proba, odds=odds, rate=rate)
    return _ev_lay(stake=stake, proba=proba, odds=odds, rate=rate)