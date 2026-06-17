# News Trading Bot

An **event-driven trading bot** in Python that trades around scheduled news / market events, with a cross-platform MetaTrader 5 integration layer. Containerized with Docker and covered by an automated test suite.

## What it does

- Reacts to news / event triggers and routes orders through an execution layer.
- **Cross-platform MT5 integration** — works on native Windows (MetaTrader5 API) and on macOS via a bridge service, so development isn't tied to one OS.
- Modular structure separating market data, agent/decision logic, and configuration.

## Stack

Python · MetaTrader 5 integration · Docker · automated tests (pytest).

## Project structure

```
src/       # core bot logic
agent/      # decision / strategy layer
market/     # market data layer
config/     # configuration
docker/     # container setup
tests/      # automated test suite
docs/       # documentation
```

## Quick start

```bash
pip install -r requirements.txt
cp .env.example .env        # configure credentials and settings
python -m src.main          # see QUICKSTART.md for full setup
```

For the MT5 bridge setup (macOS) and full configuration, see `QUICKSTART.md` and the `docs/` folder.

## Notes

Research / educational project. Configure and test in a demo environment first. Do not commit real credentials — use `.env` (gitignored) for tokens and account details.
