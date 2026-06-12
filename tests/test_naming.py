from ebb import naming


def test_parse_inc_key():
    f = naming.parse_key("a/b", "a/b/dt=2026-06-11/inc-100-200.parquet")
    assert f is not None
    assert (f.dt, f.kind, f.from_id, f.to_id) == ("2026-06-11", "inc", 100, 200)


def test_parse_data_key():
    f = naming.parse_key("a/b", "a/b/dt=2026-06-11/data-1-99.parquet")
    assert f is not None and f.kind == "data"


def test_ignore_irrelevant_keys():
    prefix = "a/b"
    bad = [
        "a/b/dt=2026-06-11/tmp-data-1-2.parquet",  # 合并中间产物
        "a/b/dt=2026-06-11/other.txt",
        "a/b/dt=bad/inc-1-2.parquet",
        "a/b/inc-1-2.parquet",  # 不在分区目录下
        "a/b/dt=2026-06-11/inc-5-2.parquet",  # from > to
        "c/d/dt=2026-06-11/inc-1-2.parquet",  # 前缀不符
        "a/bb/dt=2026-06-11/inc-1-2.parquet",  # 前缀边界
    ]
    assert naming.parse_keys(prefix, bad) == []


def test_watermark():
    keys = [
        "p/dt=2026-06-10/data-1-1000.parquet",
        "p/dt=2026-06-11/inc-1001-1500.parquet",
        "p/dt=2026-06-11/inc-1501-1600.parquet",
    ]
    files = naming.parse_keys("p", keys)
    assert naming.watermark_of(files) == 1600
    assert naming.watermark_of([]) == 0


def test_key_builders_roundtrip():
    key = naming.inc_key("x/y", "2026-01-02", 5, 9)
    f = naming.parse_key("x/y", key)
    assert f is not None and f.from_id == 5 and f.to_id == 9 and f.kind == "inc"
    key2 = naming.data_key("x/y", "2026-01-02", 5, 9)
    assert naming.parse_key("x/y", key2).kind == "data"
