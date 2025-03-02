import pytest
import sys
import os
import re
from flowline import FlowPipe, FlowOutputFilter, FlowManager, FlowSource
from flowline.flow_builder.flow_builder import build_flow

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

def test_output_mapping():
    """Test that output mapping correctly renames keys between upstream and downstream nodes."""
    # Start node outputs key "X" with value "value_x".
    start = FlowPipe(inputs=[], outputs=["X"], action=lambda _: {"X": "value_x"})
    # Downstream node requires input "y" and outputs "result".
    node = FlowPipe(inputs=["y"], outputs=["result"], action=lambda d: {"result": d["y"]})
    # Map upstream "X" to downstream "y".
    start.add_downstream(node, outputMapping={"X": "y"})
    manager = FlowManager(start)
    result = manager.run({})
    assert result["result"] == "value_x"

def test_flow_source_gets_initial_data():
    """Test that a FlowSource with initial data passes it to the downstream node."""
    source = FlowSource()
    node = FlowPipe(
        inputs=["param"],
        outputs=["result"],
        action=lambda d: {"result": d.get("param")}
    )
    source.add_downstream(node, outputMapping={"initial_param": "param"})
    manager = FlowManager(source)
    result = manager.run({"initial_param": "explicit_value"})
    # Expect the explicit value to be passed to the node.
    assert result["result"] == "explicit_value"

def test_flow_builder_with_complex_flow():
    """Test that our flow builder can generate a complex flow with splits, merges and external inputs."""
    # Define a complex flow configuration with:
    # - A source node providing external inputs
    # - A split into multiple paths
    # - Multiple dependencies between pipes
    # - Both internal and external inputs for pipes
    
    # Helper actions for this test
    def multiply_action(data):
        arr = data["value"]
        multiplier = data["multiplier"]
        return {"result": [x * multiplier for x in arr]}
        
    def sum_action(data):
        aarr = data["a"]
        barr = data["b"]
        return {"sum": sum(aarr) + sum(barr)} 
    
    flow_config = {
        'source_data': {
            'type': FlowPipe,
            'init': {
                'inputs': [],
                'outputs': ['data'],
                'action': lambda _: {"data": [1, 2, 3, 4, 5]}
            },
            'upstream_pipes': {}  # No upstream dependencies
        },
        'filter_odd': {
            'type': FlowPipe,
            'init': {
                'inputs': ['data'],
                'outputs': ['filtered_data'],
                'action': lambda d: {"filtered_data": [x for x in d["data"] if x % 2 == 1]} 
            },
            'upstream_pipes': {
                'source_data': {'data': 'data'}
            }
        },
        'filter_even': {
            'type': FlowPipe,
            'init': {
                'inputs': ['data'],
                'outputs': ['filtered_data'],
                'action': lambda d: {"filtered_data": [x for x in d["data"] if x % 2 == 0]} 
            },
            'upstream_pipes': {
                'source_data': {'data': 'data'}
            }
        },
        'multiply_odd': {
            'type': FlowPipe,
            'init': {
                'inputs': ['value', 'multiplier'],
                'outputs': ['result'],
                'action': multiply_action
            },
            'upstream_pipes': {
                'filter_odd': {'filtered_data': 'value'},
                '*': {'odd_multiplier': 'multiplier'}  # External input
            }
        },
        'multiply_even': {
            'type': FlowPipe,
            'init': {
                'inputs': ['value', 'multiplier'],
                'outputs': ['result'],
                'action': multiply_action
            },
            'upstream_pipes': {
                'filter_even': {'filtered_data': 'value'},
                '*': {'even_multiplier': 'multiplier'}  # External input
            }
        },
        'combine_results': {
            'type': FlowPipe,
            'init': {
                'inputs': ['a', 'b'],
                'outputs': ['sum'],
                'action': sum_action
            },
            'upstream_pipes': {
                'multiply_odd': {'result': 'a'},
                'multiply_even': {'result': 'b'}
            }
        }
    }
    
    # Build the flow using our builder
    manager, pipes = build_flow(flow_config)
    
    # Test execution with external inputs
    external_inputs = {
        'odd_multiplier': 10,
        'even_multiplier': 5
    }
    
    result = manager.run(external_inputs)
    
    # Verify the result
    # Expected:
    # - odd values [1, 3, 5] * 10 = 90
    # - even values [2, 4] * 5 = 30
    # - sum = 120
    assert 'sum' in result
    assert result['sum'] == 120
    
    # Verify the structure of the flow
    source = pipes['*']
    assert len(source.get_downstream()) == 3  # Connected to source_data + 2 multiply nodes
    
    # Check that the flow has the expected number of pipes
    assert len(pipes) == 7  # source + 6 defined pipes
    
    # Check for specific connections
    assert pipes['filter_odd'] in pipes['source_data'].get_downstream()
    assert pipes['filter_even'] in pipes['source_data'].get_downstream()
    assert pipes['multiply_odd'] in pipes['filter_odd'].get_downstream()
    assert pipes['multiply_even'] in pipes['filter_even'].get_downstream()
    assert pipes['combine_results'] in pipes['multiply_odd'].get_downstream()
    assert pipes['combine_results'] in pipes['multiply_even'].get_downstream()