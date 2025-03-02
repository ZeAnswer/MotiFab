import pytest
from flowline import UnitAmountConverterPipe

# ------------------------------
# Tests for UnitAmountConverterPipe
# ------------------------------

def test_convert_amount():
    """Test converting an absolute amount."""
    pipe = UnitAmountConverterPipe()
    sample_items = ["item1", "item2", "item3", "item4", "item5"]
    
    result = pipe.execute({"items": sample_items, "amount": 3})
    
    assert "amount" in result
    assert result["amount"] == 3

def test_convert_percentage_amount():
    """Test converting a percentage-based amount."""
    pipe = UnitAmountConverterPipe()
    sample_items = ["item1", "item2", "item3", "item4", "item5", "item6", "item7", "item8", "item9", "item10"]
    
    result = pipe.execute({"items": sample_items, "amount": "50%"})
    
    assert "amount" in result
    assert result["amount"] == 5  # 50% of 10 items = 5

def test_convert_percentage_rounding():
    """Test that percentage conversion rounds to nearest integer."""
    pipe = UnitAmountConverterPipe()
    sample_items = ["item1", "item2", "item3"]
    
    # 33.33% of 3 items = 0.9999, which should round to 1
    result = pipe.execute({"items": sample_items, "amount": "33.33%"})
    assert result["amount"] == 1
    
    # 66.67% of 3 items = 2.0001, which should round to 2
    result = pipe.execute({"items": sample_items, "amount": "66.67%"})
    assert result["amount"] == 2

def test_convert_empty_items_list():
    """Test converting with an empty items list."""
    pipe = UnitAmountConverterPipe()
    sample_items = []
    
    # Absolute amount with empty list
    result = pipe.execute({"items": sample_items, "amount": 5})
    assert result["amount"] == 0  # Should return 0 when no items exist
    
    # Percentage amount with empty list
    result = pipe.execute({"items": sample_items, "amount": "50%"})
    assert result["amount"] == 0  # Should return 0 when no items exist

def test_convert_amount_more_than_available():
    """Test that the result is limited to the number of available items."""
    pipe = UnitAmountConverterPipe()
    sample_items = ["item1", "item2", "item3"]
    
    result = pipe.execute({"items": sample_items, "amount": 10})
    assert result["amount"] == 3  # Should be limited to the number of items
    
    result = pipe.execute({"items": sample_items, "amount": "200%"})
    assert result["amount"] == 3  # Should be limited to the number of items

def test_convert_invalid_percentage_format():
    """Test with invalid percentage format."""
    pipe = UnitAmountConverterPipe()
    sample_items = ["item1", "item2", "item3"]
    
    with pytest.raises(ValueError):
        pipe.execute({"items": sample_items, "amount": "invalid%"})
    
    with pytest.raises(ValueError):
        pipe.execute({"items": sample_items, "amount": "%50"})