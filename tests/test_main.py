from benchmark.main import parse_args, async_main


def test_parse_args_overrides():
    ns = parse_args(["--message-count", "500", "--clients", "fake", "--amqp-url", "amqp://h/"])
    assert ns.message_count == 500
    assert ns.clients == "fake"
    assert ns.amqp_url == "amqp://h/"


def test_parse_args_confirms_and_durable():
    ns = parse_args([])
    assert ns.confirms is None and ns.durable is None  # unset -> config decides
    ns = parse_args(["--no-confirms", "--durable"])
    assert ns.confirms is False
    assert ns.durable is True


async def test_async_main_end_to_end_with_fake(tmp_path):
    run_dir = await async_main([
        "--clients", "fake", "--message-count", "50", "--iterations", "2",
        "--output-dir", str(tmp_path),
    ])
    import os
    assert os.path.exists(os.path.join(run_dir, "results.json"))
    assert os.path.exists(os.path.join(run_dir, "results.csv"))
    assert os.path.exists(os.path.join(run_dir, "report.html"))
