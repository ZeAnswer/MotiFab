import random
import pytest
from src.shuffle import naive_shuffle, di_pair_shuffle, shuffle_sequence

def test_naive_shuffle():
    seq = "ACGTACGTAC"
    shuffled = naive_shuffle(seq)
    # Check that the length is preserved.
    assert len(shuffled) == len(seq)
    # Check that the multiset of characters is the same.
    assert sorted(shuffled) == sorted(seq)
    
    # Check that repeated shuffling produces a different order at least sometimes.
    different = False
    for _ in range(10):
        if naive_shuffle(seq) != seq:
            different = True
            break
    assert different, "Naive shuffle did not produce a different order in 10 attempts."

def test_di_pair_shuffle_even():
    # Even-length sequence.
    seq = "ACGTACGT"  # Length 8.
    # Expected dinucleotide blocks.
    original_pairs = [seq[i:i+2] for i in range(0, len(seq), 2)]
    
    shuffled = di_pair_shuffle(seq)
    # The shuffled sequence should have the same length.
    assert len(shuffled) == len(seq)
    
    # For even-length, the output is a concatenation of blocks each of length 2.
    blocks = [shuffled[i:i+2] for i in range(0, len(shuffled), 2)]
    # Every block should be of length 2.
    for block in blocks:
        assert len(block) == 2
    # The sorted blocks should equal the sorted original pairs.
    assert sorted(blocks) == sorted(original_pairs)

def test_di_pair_shuffle_odd():
    # Odd-length sequence.
    seq = "ACGTACGTA"  # Length 9.
    # Expected: split into dinucleotide pairs plus a remainder.
    pairs = [seq[i:i+2] for i in range(0, len(seq)-1, 2)]
    remainder = seq[-1]
    # The expected blocks are the dinucleotide pairs and one extra block (the remainder).
    original_blocks = pairs + [remainder]
    
    shuffled = di_pair_shuffle(seq)
    # Check that the output length is preserved.
    assert len(shuffled) == len(seq)
    
    # Because the blocks were concatenated in random order without delimiters,
    # we try every possible way of removing one character from the shuffled output.
    # For each removal, if the remaining string divides into chunks of 2 characters
    # that (when sorted) equal the expected dinucleotide pairs and the removed character
    # equals the expected remainder, we consider the test passed.
    found_valid_partition = False
    for i in range(len(shuffled)):
        candidate_remainder = shuffled[i]
        candidate_pairs_str = shuffled[:i] + shuffled[i+1:]
        if len(candidate_pairs_str) % 2 != 0:
            continue  # This candidate cannot form complete pairs.
        candidate_pairs = [candidate_pairs_str[j:j+2] for j in range(0, len(candidate_pairs_str), 2)]
        if sorted(candidate_pairs) == sorted(pairs) and candidate_remainder == remainder:
            found_valid_partition = True
            break
    assert found_valid_partition, (
        "Di-pair shuffle (odd-length) did not produce blocks matching the expected pairs and remainder."
    )

def test_shuffle_sequence_invalid():
    seq = "ACGTACGT"
    with pytest.raises(ValueError):
        shuffle_sequence(seq, method="unknown")