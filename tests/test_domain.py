from decimal import Decimal as D

import pytest

from btc5m.config import Settings
from btc5m.domain import Book, Market, MomentumStrategy, fee, window_slugs


def event(start=1800000000):
    return {
        "slug": f"btc-updown-5m-{start}",
        "markets": [
            {
                "slug": f"btc-updown-5m-{start}",
                "conditionId": "condition-1",
                "version": "v1",
                "active": True,
                "closed": False,
                "acceptingOrders": True,
                "cryptoMarketConfig": {
                    "id": "btc-5m-twap-60",
                    "asset": "btc",
                    "duration": "5m",
                    "twapEnabled": True,
                    "twapLookbackSeconds": 60,
                },
                "endDate": "2027-01-15T08:05:00Z",
                "outcomes": '["Up","Down"]',
                "clobTokenIds": '["up","down"]',
                "orderMinSize": 5,
                "orderPriceMinTickSize": 0.01,
                "feesEnabled": True,
                "feeType": "crypto_fees_v2",
                "feeSchedule": {"exponent": 1, "rate": 0.07, "takerOnly": True},
            }
        ],
    }


def make_book(token="up", bid="0.49", ask="0.51", size="10", now=1800000001):
    book = Book(token, D("0.01"))
    book.snapshot(
        {
            "asset_id": token,
            "timestamp": str(int(now * 1000)),
            "bids": [{"price": bid, "size": size}],
            "asks": [{"price": ask, "size": size}],
        },
        now,
    )
    return book


def test_window_and_strict_fee_schema():
    assert window_slugs(1800000299) == ("btc-updown-5m-1800000000", "btc-updown-5m-1800000300")
    market = Market.from_event(event())
    assert market.tokens == {"Up": "up", "Down": "down"}
    assert Market.from_dict(market.to_dict()) == market
    bad = event()
    bad["markets"][0]["feeSchedule"]["rate"] = 0.1
    with pytest.raises(ValueError, match="fee"):
        Market.from_event(bad)


def test_fee_five_decimals():
    assert fee(D("100"), D("0.5")) == D("1.75000")
    assert fee(D("100"), D("0.1")) == D("0.63000")
    assert fee(D("5"), D("0.123")) == D("0.03775")


def test_book_delta_tick_and_stale():
    book = make_book()
    assert book.healthy(1800000002, 5)
    book.delta(
        {"asset_id": "up", "price": "0.51", "size": "0", "side": "SELL"},
        "1800000002000",
        1800000002,
    )
    assert book.best_ask is None
    assert not book.healthy(1800000002, 5)
    book.change_tick("0.001")
    assert book.tick == D("0.001")
    assert not make_book().healthy(1800000010, 5)
    book.disconnect()
    assert not book.connected


def test_book_invalid_or_old_messages_fail_closed():
    book = make_book()
    with pytest.raises(ValueError):
        book.delta(
            {"asset_id": "up", "price": "0.50", "size": "2", "side": "MAYBE"},
            "1800000002000",
            1800000002,
        )
    assert not book.connected
    book = make_book()
    book.snapshot(
        {"asset_id": "up", "timestamp": "1800000000000", "bids": [], "asks": []}, 1800000003
    )
    assert book.connected
    assert book.best_ask == D("0.51")
    assert book.source_time == 1800000001


def test_momentum_requires_observed_change_and_fresh_books():
    market = Market.from_event(event())
    settings = Settings(signal_threshold=D("0.02"), min_momentum=D("0.02"))
    strategy = MomentumStrategy(settings)
    books = {"up": make_book(), "down": make_book("down")}
    assert strategy.signal(market, books, 1800000001) is None
    books["up"] = make_book(bid="0.55", ask="0.57", now=1800000003)
    signal = strategy.signal(market, books, 1800000003)
    assert signal is not None and signal.outcome == "Up"
    assert signal.quantity == settings.shares
    assert strategy.signal(market, books, 1800000010) is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("active", False),
        ("closed", True),
        ("acceptingOrders", False),
        ("active", "true"),
        ("closed", None),
        ("acceptingOrders", None),
    ],
)
def test_market_tradeability_is_strict(field, value):
    raw = event()
    raw["markets"][0][field] = value
    with pytest.raises(ValueError, match="trad"):
        Market.from_event(raw)


@pytest.mark.parametrize(
    "field,value",
    [
        ("id", "other"),
        ("asset", "eth"),
        ("duration", "15m"),
        ("twapEnabled", False),
        ("twapLookbackSeconds", 30),
    ],
)
def test_market_rules_require_observed_chainlink_twap_schema(field, value):
    raw = event()
    raw["markets"][0]["cryptoMarketConfig"][field] = value
    with pytest.raises(ValueError, match="TWAP"):
        Market.from_event(raw)


def test_market_missing_rules_fail_closed():
    raw = event()
    del raw["markets"][0]["cryptoMarketConfig"]
    with pytest.raises(ValueError, match="TWAP"):
        Market.from_event(raw)


def test_trade_updates_latest_price_without_refreshing_book_freshness():
    book = make_book()
    book.trade(
        {"asset_id": "up", "price": "0.53", "size": "2", "timestamp": "1800000003000"}, 1800000003
    )
    assert book.latest_trade_price == D("0.53")
    assert book.latest_trade_time == 1800000003
    assert book.source_time == 1800000001
    assert book.updated_at == 1800000001
    assert not book.healthy(1800000007, 5)
    book.trade(
        {"asset_id": "up", "price": "0.50", "size": "1", "timestamp": "1800000002000"}, 1800000004
    )
    assert book.latest_trade_price == D("0.53")


@pytest.mark.parametrize(
    "patch",
    [
        {"asset_id": "other"},
        {"price": "NaN"},
        {"price": "1.1"},
        {"size": "-1"},
        {"size": "0"},
        {"timestamp": "1800000100000"},
    ],
)
def test_invalid_trade_fails_closed_without_mutating_depth(patch):
    book = make_book()
    message = {"asset_id": "up", "price": "0.53", "size": "2", "timestamp": "1800000003000"}
    message.update(patch)
    with pytest.raises(ValueError):
        book.trade(message, 1800000003)
    assert book.latest_trade_price is None
    assert book.updated_at == 1800000001
    assert book.asks == {D("0.51"): D("10")}


def test_health_rechecks_all_depth_prices_after_tick_change_or_corruption():
    book = make_book()
    book.asks[D("0.515")] = D("10")
    assert not book.healthy(1800000002, 5)
    book = make_book()
    book.tick = D("0.1")
    assert not book.healthy(1800000002, 5)
