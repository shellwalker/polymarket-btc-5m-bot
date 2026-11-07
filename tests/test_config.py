from decimal import Decimal

import pytest

from btc5m.config import Settings


def test_defaults_and_env_override(tmp_path):
    config = tmp_path / "config.toml"
    config.write_text('max_trade="12.5"\nlive=false\n')
    settings = Settings.load(config, {"BTC5M_MAX_TRADE": "7"})
    assert settings.max_trade == Decimal("7")
    assert not settings.live


@pytest.mark.parametrize("value", ["true", "1", "yes"])
def test_live_always_refused(value):
    with pytest.raises(ValueError, match="live"):
        Settings.load(environ={"BTC5M_LIVE": value})


@pytest.mark.parametrize(
    "key,value", [("shares", "NaN"), ("stale_seconds", "inf"), ("max_trade", "-1"), ("bogus", "5")]
)
def test_invalid_settings_refused(tmp_path, key, value):
    path = tmp_path / "config.toml"
    path.write_text(f'{key}="{value}"')
    with pytest.raises(ValueError):
        Settings.load(path, {})
