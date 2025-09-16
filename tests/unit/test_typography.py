"""Test typography."""

from haolib.utils.typography import to_constant_case


def test_to_constant_case() -> None:
    """Test to constant case."""
    assert to_constant_case("test") == "TEST"
    assert to_constant_case("Test") == "TEST"
    assert to_constant_case("testTest") == "TEST_TEST"
    assert to_constant_case("TestTest") == "TEST_TEST"
    assert to_constant_case("TestTestTest") == "TEST_TEST_TEST"
    assert to_constant_case("TestTestTestTest") == "TEST_TEST_TEST_TEST"
    assert to_constant_case("TestTestTestTestTest") == "TEST_TEST_TEST_TEST_TEST"
    assert to_constant_case("TestTestTestTestTestTest") == "TEST_TEST_TEST_TEST_TEST_TEST"
    assert to_constant_case("TestTestTestTestTestTestTest") == "TEST_TEST_TEST_TEST_TEST_TEST_TEST"
    assert to_constant_case("TestTestTestTestTestTestTestTest") == "TEST_TEST_TEST_TEST_TEST_TEST_TEST_TEST"
    assert to_constant_case("TestTestTestTestTestTestTestTestTest") == "TEST_TEST_TEST_TEST_TEST_TEST_TEST_TEST_TEST"
    assert (
        to_constant_case("TestTestTestTestTestTestTestTestTestTest")
        == "TEST_TEST_TEST_TEST_TEST_TEST_TEST_TEST_TEST_TEST"
    )
    assert (
        to_constant_case("TestTestTestTestTestTestTestTestTestTestTest")
        == "TEST_TEST_TEST_TEST_TEST_TEST_TEST_TEST_TEST_TEST_TEST"
    )
    assert (
        to_constant_case("TestTestTestTestTestTestTestTestTestTestTestTest")
        == "TEST_TEST_TEST_TEST_TEST_TEST_TEST_TEST_TEST_TEST_TEST_TEST"
    )
