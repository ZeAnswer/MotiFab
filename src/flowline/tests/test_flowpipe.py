import pytest
import sys
import os
import re
from flowline import FlowPipe, FlowOutputFilter, FlowManager

# --- Helper functions to create FlowPipe instances with specific actions ---

def passthrough_action(data):
    """Simple action that returns the inputs as outputs."""
    return data

def combine_action(data):
    """Combines input values into a single dictionary."""
    return {key: f"combined_{value}" for key, value in data.items()}

def identity_action(data):
    """Returns a copy of the input dictionary."""
    return dict(data)


# --- Test Cases ---

def test_valid_pipeline():
    # A simple linear pipeline:
    # start (outputs: "A") -> node2 (inputs: "A", outputs: "B")
    start = FlowPipe(inputs=[], outputs=["A"], action=passthrough_action)
    node2 = FlowPipe(inputs=["A"], outputs=["B"], action=passthrough_action)
    start.add_downstream(node2)
    
    manager = FlowManager(start)
    assert manager.validate_flow() == True


def test_valid_split():
    # A valid split: one start node outputs "A"; two downstream nodes each require "A".
    start = FlowPipe(inputs=[], outputs=["A"], action=passthrough_action)
    node2 = FlowPipe(inputs=["A"], outputs=["B"], action=passthrough_action)
    node3 = FlowPipe(inputs=["A"], outputs=["C"], action=passthrough_action)

    start.add_downstream(node2)
    start.add_downstream(node3)

    manager = FlowManager(start)
    assert manager.validate_flow() == True


def test_duplicate_input_error():
    # Two upstream nodes provide the same input to a downstream node.
    start = FlowPipe(inputs=[], outputs=["X"], action=passthrough_action)
    branch1 = FlowPipe(inputs=["X"], outputs=["Y"], action=passthrough_action)
    branch2 = FlowPipe(inputs=["X"], outputs=["Y"], action=passthrough_action)

    start.add_downstream(branch1)
    start.add_downstream(branch2)

    merge_node = FlowPipe(inputs=["Y"], outputs=["Z"], action=passthrough_action)
    branch1.add_downstream(merge_node)
    branch2.add_downstream(merge_node)

    manager = FlowManager(start)
    with pytest.raises(RuntimeError, match=re.escape("Duplicate input 'Y'")):
        manager.validate_flow()


def test_deadlock_missing_input():
    # A node requires an input that is never provided.
    start = FlowPipe(inputs=[], outputs=["A"], action=passthrough_action)
    node2 = FlowPipe(inputs=["B"], outputs=["C"], action=passthrough_action)  # Requires "B", but start only outputs "A"

    start.add_downstream(node2)

    manager = FlowManager(start)
    with pytest.raises(RuntimeError, match="requires input 'B' which is not provided"):
        manager.validate_flow()


def test_cycle_detection():
    # Create a cycle: node1 -> node2 -> node3 -> node1
    node1 = FlowPipe(inputs=["A"], outputs=["B"], action=passthrough_action)
    node2 = FlowPipe(inputs=["B"], outputs=["C"], action=passthrough_action)
    node3 = FlowPipe(inputs=["C"], outputs=["A"], action=passthrough_action)

    node1.add_downstream(node2)
    node2.add_downstream(node3)
    node3.add_downstream(node1)

    manager = FlowManager(node1)
    with pytest.raises(RuntimeError, match="Cycle detected"):
        manager.validate_flow()


def test_duplicate_outputs_in_node():
    # A node declares duplicate outputs.
    start = FlowPipe(inputs=[], outputs=["A", "A"], action=passthrough_action)  # Duplicate "A"

    manager = FlowManager(start)
    with pytest.raises(RuntimeError, match="has duplicate outputs"):
        manager.validate_flow()


def test_duplicate_sink_outputs():
    # Two sink nodes produce the same output.
    start = FlowPipe(inputs=[], outputs=["X"], action=passthrough_action)
    node2 = FlowPipe(inputs=["X"], outputs=["Y"], action=passthrough_action)
    node3 = FlowPipe(inputs=["X"], outputs=["Y"], action=passthrough_action)  # Both node2 and node3 produce "Y"

    start.add_downstream(node2)
    start.add_downstream(node3)

    manager = FlowManager(start)
    with pytest.raises(RuntimeError, match="Output 'Y' is produced by multiple sink nodes"):
        manager.validate_flow()


def test_output_filter_usage():
    # Use a FlowOutputFilter to filter specific outputs before passing them forward.
    start = FlowPipe(inputs=[], outputs=["A", "Extra"], action=passthrough_action)
    filter_node = FlowOutputFilter(["A"])  # Filters out "Extra" and keeps only "A"
    node2 = FlowPipe(inputs=["A"], outputs=["B"], action=passthrough_action)

    start.add_downstream(filter_node)
    filter_node.add_downstream(node2)

    manager = FlowManager(start)
    assert manager.validate_flow() == True


def test_successful_execution():
    # A full valid pipeline with execution.
    start = FlowPipe(inputs=[], outputs=["A"], action=lambda _: {"A": 5})
    node2 = FlowPipe(inputs=["A"], outputs=["B"], action=lambda d: {"B": d["A"] * 2})
    node3 = FlowPipe(inputs=["B"], outputs=["C"], action=lambda d: {"C": d["B"] + 3})

    start.add_downstream(node2)
    node2.add_downstream(node3)

    manager = FlowManager(start)
    result = manager.run({})
    print(result)
    assert result == {"C": 13}  # 5 * 2 + 3 = 13


def test_execution_with_filter():
    # Execution where a filter ensures only relevant outputs are passed forward.
    start = FlowPipe(inputs=[], outputs=["A", "B"], action=lambda _: {"A": 5, "B": 10})
    filter_node = FlowOutputFilter(["A"])  # Should block "B"
    node2 = FlowPipe(inputs=["A"], outputs=["C"], action=lambda d: {"C": d["A"] * 3})

    start.add_downstream(filter_node)
    filter_node.add_downstream(node2)

    manager = FlowManager(start)
    result = manager.run({})

    assert result == {"C": 15}  # 5 * 3 = 15


def test_parallel_execution():
    # A split pipeline where two branches execute independently.
    start = FlowPipe(inputs=[], outputs=["A"], action=lambda _: {"A": 2})
    node2 = FlowPipe(inputs=["A"], outputs=["B"], action=lambda d: {"B": d["A"] + 1})
    node3 = FlowPipe(inputs=["A"], outputs=["C"], action=lambda d: {"C": d["A"] * 4})

    start.add_downstream(node2)
    start.add_downstream(node3)

    manager = FlowManager(start)
    result = manager.run({})

    assert result == {"B": 3, "C": 8}  # 2 + 1, 2 * 4


# TODO added external inputs, need to test those as well
# TODO added output mappings, need to test those as well