from strategies import robinhood_lighter_market_maker as entrypoint


def test_entrypoint_adds_default_env_file_without_rewriting_user_args(monkeypatch):
    monkeypatch.setattr(entrypoint._maker, "resolve_default_env_file", lambda: "robinhood.env")
    assert entrypoint.build_argv(["--spread-bps", "8"]) == [
        "--env-file",
        "robinhood.env",
        "--spread-bps",
        "8",
    ]


def test_entrypoint_preserves_explicit_env_file():
    args = ["--env-file", "robinhood.env", "--lighter-leverage", "3"]
    assert entrypoint.build_argv(args) == args


def test_entrypoint_forwards_to_existing_maker_main(monkeypatch):
    calls = []
    monkeypatch.setattr(entrypoint._maker, "resolve_default_env_file", lambda: ".env.robinhood")
    monkeypatch.setattr(entrypoint._maker, "main", lambda args: calls.append(args))

    entrypoint.main(["--allowed-side", "buy"])

    assert calls == [["--env-file", ".env.robinhood", "--allowed-side", "buy"]]
