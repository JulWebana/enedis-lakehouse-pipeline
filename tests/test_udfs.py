import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from udfs import categorize_consumption, compute_carbon_score  # noqa: E402


def test_categorize_none():
    assert categorize_consumption(None) == "INCONNU"


def test_categorize_tres_faible():
    assert categorize_consumption(500) == "TRES FAIBLE"


def test_categorize_faible():
    assert categorize_consumption(3000) == "FAIBLE"


def test_categorize_moderee():
    assert categorize_consumption(10000) == "MODEREE"


def test_categorize_elevee():
    assert categorize_consumption(50000) == "ELEVEE"


def test_categorize_tres_elevee():
    assert categorize_consumption(200000) == "TRES ELEVEE"


def test_carbon_score_none_consumption():
    assert compute_carbon_score(None, 10) is None


def test_carbon_score_none_sites():
    assert compute_carbon_score(1000, None) is None


def test_carbon_score_zero_sites():
    assert compute_carbon_score(1000, 0) is None


def test_carbon_score_valide():
    result = compute_carbon_score(1000000, 10)
    assert result is not None
    assert result > 0
