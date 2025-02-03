import unittest
from resolvers.resolver_manager import ResolverManager
from resolvers.resolver import Resolver, GlobalResolver, Constraint
import copy

# Dummy dataset
data = [
    {"chrom": "chr1", "start": 100, "end": 200, "gene": "gene1"},
    {"chrom": "chr2", "start": 300, "end": 400, "gene": "gene2"},
    {"chrom": "chr1", "start": 500, "end": 600, "gene": "gene3"},
    {"chrom": "chr2", "start": 700, "end": 800, "gene": "gene4"},
]

# Define a per-record constraint to filter only "chr1"
class ChromosomeConstraint(Constraint):
    def action(self, record):
        return record["chrom"] == "chr1"

# Define a per-record resolver to add 100 to each "start" position
class StartOffsetResolver(Resolver):
    def action(self, record):
        record["start"] += 100
        return record

# Define a global modifier to duplicate the dataset and append it to itself
class DuplicateDataResolver(GlobalResolver):
    def action(self, data):
        return data + [copy.deepcopy(record) for record in data]  # Duplicate the dataset

# Define a resolver with a custom iterator (reversing order)
class CustomGeneModifier(Resolver):
    def __init__(self):
        super().__init__(iterator=self.iterate)
        
    def action(self, record):
        return record

    # Custom iterator to reverse the order of records
    def iterate(self, data):
        return reversed(data)

# Define a resolver with a custom mask (modify all but the last record)
class CustomMaskResolver(Resolver):
    def __init__(self):
        super().__init__(mask=[True, True, True, False])
        
    def action(self, record):
        record["gene"] = f"{record['gene']}iterated"
        return record
    
    # Custom mask (boolean iterable) to exclude the last record
    # def mask(self, record):
    #     return record["gene"] != "gene4"

# Test class
class TestResolverManager(unittest.TestCase):
    def test_global_and_per_record_resolvers(self):
        manager = ResolverManager()
        manager.add_resolver(ChromosomeConstraint())   # Filter only "chr1"
        manager.add_resolver(StartOffsetResolver())    # Modify "start" position
        manager.add_resolver(DuplicateDataResolver())  # Duplicate globally
        manager.add_resolver(CustomGeneModifier())     # Modify order using custom iterator
        manager.add_resolver(CustomMaskResolver())     # Modify "gene" using custom mask
        
        result = manager.apply(data)
        expected = [
            {"chrom": "chr1", "start": 600, "end": 600, "gene": "gene3iterated"},
            {"chrom": "chr1", "start": 200, "end": 200, "gene": "gene1iterated"},
            {"chrom": "chr1", "start": 600, "end": 600, "gene": "gene3iterated"},
            {"chrom": "chr1", "start": 200, "end": 200, "gene": "gene1"},
        ]
        self.assertEqual(result, expected)

if __name__ == "__main__":
    unittest.main()