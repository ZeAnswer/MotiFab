import pytest
import sys
import os
import itertools
import multiprocessing as mp
import re
from flowline import FlowPipe, FlowOutputFilter, FlowManager, FlowSource, FlowSubPipeline, FlowSplitJoinPipe, build_flow#, FlowMapPipe

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
    assert manager.initialize_and_validate_flow() == True

def test_valid_split():
    # A valid split: one start node outputs "A"; two downstream nodes each require "A".
    start = FlowPipe(inputs=[], outputs=["A"], action=passthrough_action)
    node2 = FlowPipe(inputs=["A"], outputs=["B"], action=passthrough_action)
    node3 = FlowPipe(inputs=["A"], outputs=["C"], action=passthrough_action)

    start.add_downstream(node2)
    start.add_downstream(node3)

    manager = FlowManager(start)
    assert manager.initialize_and_validate_flow() == True

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
        manager.initialize_and_validate_flow()

def test_deadlock_missing_input():
    # A node requires an input that is never provided.
    start = FlowPipe(inputs=[], outputs=["A"], action=passthrough_action)
    node2 = FlowPipe(inputs=["B"], outputs=["C"], action=passthrough_action)  # Requires "B", but start only outputs "A"

    start.add_downstream(node2)

    manager = FlowManager(start)
    with pytest.raises(RuntimeError, match="requires input 'B' which is not provided"):
        manager.initialize_and_validate_flow()

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
        manager.initialize_and_validate_flow()


def test_duplicate_outputs_in_node():
    # A node declares duplicate outputs.
    start = FlowPipe(inputs=[], outputs=["A", "A"], action=passthrough_action)  # Duplicate "A"

    manager = FlowManager(start)
    with pytest.raises(RuntimeError, match="has duplicate outputs"):
        manager.initialize_and_validate_flow()

def test_duplicate_sink_outputs():
    # Two sink nodes produce the same output.
    start = FlowPipe(inputs=[], outputs=["X"], action=passthrough_action)
    node2 = FlowPipe(inputs=["X"], outputs=["Y"], action=passthrough_action)
    node3 = FlowPipe(inputs=["X"], outputs=["Y"], action=passthrough_action)  # Both node2 and node3 produce "Y"

    start.add_downstream(node2)
    start.add_downstream(node3)

    manager = FlowManager(start)
    with pytest.raises(RuntimeError, match="Output 'Y' is produced by multiple sink nodes"):
        manager.initialize_and_validate_flow()

def test_output_filter_usage():
    # Use a FlowOutputFilter to filter specific outputs before passing them forward.
    start = FlowPipe(inputs=[], outputs=["A", "Extra"], action=passthrough_action)
    filter_node = FlowOutputFilter(["A"])  # Filters out "Extra" and keeps only "A"
    node2 = FlowPipe(inputs=["A"], outputs=["B"], action=passthrough_action)

    start.add_downstream(filter_node)
    filter_node.add_downstream(node2)

    manager = FlowManager(start)
    assert manager.initialize_and_validate_flow() == True

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

# --- Tests for FlowSubPipeline ---
def test_flow_sub_pipeline_simple():
    """Test that a simple sub-pipeline works correctly."""
    # Create a simple sub-pipeline
    sub_start = FlowPipe(inputs=["sub_input"], outputs=["intermediate"], action=lambda d: {"intermediate": d["sub_input"] * 2})
    sub_end = FlowPipe(inputs=["intermediate"], outputs=["sub_output"], action=lambda d: {"sub_output": d["intermediate"] + 1})
    sub_start.add_downstream(sub_end)
    
    sub_manager = FlowManager(sub_start)
    
    # Create a main pipeline with the sub-pipeline as a component
    sub_pipe = FlowSubPipeline(sub_manager, inputs=["sub_input"], outputs=["sub_output"])
    
    # Test directly
    result = sub_pipe.execute({"sub_input": 5})
    assert result["sub_output"] == 11  # 5 * 2 + 1 = 11
    
    # Test in a full pipeline
    start = FlowPipe(inputs=[], outputs=["value"], action=lambda _: {"value": 3})
    start.add_downstream(sub_pipe, outputMapping={"value": "sub_input"})
    
    main_manager = FlowManager(start)
    result = main_manager.run()
    
    assert result["sub_output"] == 7  # 3 * 2 + 1 = 7

def test_flow_sub_pipeline_with_multiple_outputs():
    """Test that a sub-pipeline with multiple outputs works correctly."""
    # Create a sub-pipeline with multiple outputs
    sub_pipe = FlowPipe(
        inputs=["input"], 
        outputs=["squared", "cubed"], 
        action=lambda d: {"squared": d["input"]**2, "cubed": d["input"]**3}
    )
    
    sub_manager = FlowManager(sub_pipe)
    
    # Create a main pipeline with the sub-pipeline
    sub_component = FlowSubPipeline(
        sub_manager, 
        inputs=["input"], 
        outputs=["square_result", "cube_result"],
        output_mapping={"squared": "square_result", "cubed": "cube_result"}
    )
    
    # Test directly
    result = sub_component.execute({"input": 4})
    assert result["square_result"] == 16  # 4^2 = 16
    assert result["cube_result"] == 64    # 4^3 = 64
    
# # --- Tests for FlowMapPipe ---

# def test_flow_map_pipe_simple():
#     """Test that a simple map operation works correctly."""
#     # Create a mapping pipe that doubles its input
#     double_pipe = FlowPipe(inputs=["x"], outputs=["y"], action=lambda d: {"y": d["x"] * 2})
    
#     # Create a map pipe that applies the double_pipe to each element
#     map_pipe = FlowMapPipe(double_pipe)
    
#     # Test directly
#     result = map_pipe.execute({"x": [1, 2, 3, 4, 5]})
#     assert result["y"] == [2, 4, 6, 8, 10]

# def test_flow_map_pipe_with_custom_names():
#     """Test that a map pipe with custom input/output names works correctly."""
#     # Create a mapping pipe that squares its input
#     square_pipe = FlowPipe(inputs=["num"], outputs=["squared"], action=lambda d: {"squared": d["num"]**2})
    
#     # Create a map pipe with custom input/output names
#     map_pipe = FlowMapPipe(square_pipe, input_name="numbers", output_name="squared_numbers")
    
#     # Test directly
#     result = map_pipe.execute({"numbers": [1, 2, 3, 4]})
#     assert result["squared_numbers"] == [1, 4, 9, 16]
    
#     # Test in a full pipeline
#     start = FlowPipe(inputs=[], outputs=["values"], action=lambda _: {"values": [5, 6, 7]})
#     end = FlowPipe(inputs=["squared_values"], outputs=["sum"], action=lambda d: {"sum": sum(d["squared_values"])})
    
#     start.add_downstream(map_pipe, outputMapping={"values": "numbers"})
#     map_pipe.add_downstream(end, outputMapping={"squared_numbers": "squared_values"})
    
#     manager = FlowManager(start)
#     result = manager.run()
    
#     assert result["sum"] == 110  # 5^2 + 6^2 + 7^2 = 25 + 36 + 49 = 110

# def test_flow_map_pipe_validation():
#     """Test that the map pipe properly validates the mapping pipe."""
#     # Test with a pipe that has multiple inputs
#     invalid_pipe1 = FlowPipe(inputs=["a", "b"], outputs=["c"], action=lambda d: {"c": d["a"] + d["b"]})
#     with pytest.raises(ValueError, match="map_pipe must have exactly one input"):
#         FlowMapPipe(invalid_pipe1)
    
#     # Test with a pipe that has multiple outputs
#     invalid_pipe2 = FlowPipe(inputs=["a"], outputs=["b", "c"], action=lambda d: {"b": d["a"], "c": d["a"] * 2})
#     with pytest.raises(ValueError, match="map_pipe must have exactly one output"):
#         FlowMapPipe(invalid_pipe2)
    
#     # Test with a non-pipe object
#     with pytest.raises(TypeError, match="map_pipe must be a FlowPipe instance"):
#         FlowMapPipe("not a pipe")

# def test_flow_map_pipe_input_validation():
#     """Test that the map pipe properly validates its input at execution time."""
#     # Create a valid mapping pipe
#     identity_pipe = FlowPipe(inputs=["x"], outputs=["y"], action=lambda d: {"y": d["x"]})
#     map_pipe = FlowMapPipe(identity_pipe)
    
#     # Test with a non-list input
#     with pytest.raises(ValueError, match="must be a list"):
#         map_pipe.execute({"x": "not a list"})
    
#     # Test with missing input
#     with pytest.raises(ValueError, match="not found in data"):
#         map_pipe.execute({"wrong_name": [1, 2, 3]})
        
# --- Helper Functions for Inner Pipe Actions ---

def add_and_subtract_action(data):
    """Takes inputs 'X' and 'Y' and returns their sum and difference."""
    x = data["X"]
    y = data["Y"]
    return {"sum": x + y, "diff": x - y}

def make_pair_action(data):
    """Returns a tuple (A, B) from inputs 'A' and 'B'."""
    return {"pair": (data["A"], data["B"])}

def add_const_action(data):
    """Adds a scalar 'const' to the input 'X'."""
    return {"result": data["X"] + data["const"]}

def square_action(data):
    """Squares the number provided in 'num'."""
    return {"squared": data["num"] ** 2}

# --- Test Cases for FlowSplitJoinPipe ---

def test_flow_split_join_zip():
    """
    Test zipped iteration when both inputs are mapped to the same index.
    Inputs 'X' and 'Y' are zipped together.
    """
    # Inner pipe takes two inputs and produces two outputs.
    inner_pipe = FlowPipe(
        inputs=["X", "Y"],
        outputs=["sum", "diff"],
        action=add_and_subtract_action
    )
    # Both inputs use the same index label, so they are zipped.
    input_mapping = {"X": "i", "Y": "i"}
    sj_pipe = FlowSplitJoinPipe(inner_pipe, input_mapping=input_mapping, max_parallel=0)
    
    data = {"X": [1, 2, 3], "Y": [4, 5, 6]}
    result = sj_pipe.execute(data)
    
    expected_arr_output = [
        {"sum": 5, "diff": -3},
        {"sum": 7, "diff": -3},
        {"sum": 9, "diff": -3}
    ]
    # Check aggregated outputs as well.
    assert result["arr_output"] == expected_arr_output
    assert result["sum"] == [5, 7, 9]
    assert result["diff"] == [-3, -3, -3]

def test_flow_split_join_cartesian():
    """
    Test Cartesian iteration when inputs have different index labels.
    Input 'A' is iterated with label "i" and 'B' with label "j", producing the full product.
    """
    inner_pipe = FlowPipe(
        inputs=["A", "B"],
        outputs=["pair"],
        action=make_pair_action
    )
    # 'A' and 'B' use different index labels: Cartesian product.
    input_mapping = {"A": "i", "B": "j"}
    sj_pipe = FlowSplitJoinPipe(inner_pipe, input_mapping=input_mapping, max_parallel=0)
    
    data = {"A": [1, 2], "B": [10, 20, 30]}
    result = sj_pipe.execute(data)
    
    expected_arr_output = [
        {"pair": (1, 10)},
        {"pair": (1, 20)},
        {"pair": (1, 30)},
        {"pair": (2, 10)},
        {"pair": (2, 20)},
        {"pair": (2, 30)}
    ]
    assert result["arr_output"] == expected_arr_output
    # Also check the aggregated output.
    expected_pairs = [(1, 10), (1, 20), (1, 30), (2, 10), (2, 20), (2, 30)]
    assert result["pair"] == expected_pairs

def test_flow_split_join_scalar_input():
    """
    Test that scalar inputs (without an index mapping) are passed unchanged.
    'X' is iterated (mapped to "i") and 'const' is scalar.
    """
    inner_pipe = FlowPipe(
        inputs=["X", "const"],
        outputs=["result"],
        action=add_const_action
    )
    # Only 'X' is split; 'const' remains constant.
    input_mapping = {"X": "i", "const": None}
    sj_pipe = FlowSplitJoinPipe(inner_pipe, input_mapping=input_mapping, max_parallel=0)
    
    data = {"X": [10, 20, 30], "const": 5}
    result = sj_pipe.execute(data)
    
    expected_arr_output = [
        {"result": 15},
        {"result": 25},
        {"result": 35}
    ]
    assert result["arr_output"] == expected_arr_output
    assert result["result"] == [15, 25, 35]

def test_flow_split_join_parallel_execution():
    """
    Test parallel execution by setting max_parallel > 0.
    Uses a simple inner pipe that squares its input.
    """
    inner_pipe = FlowPipe(
        inputs=["num"],
        outputs=["squared"],
        action=square_action
    )
    input_mapping = {"num": "i"}
    sj_pipe = FlowSplitJoinPipe(inner_pipe, input_mapping=input_mapping, max_parallel=2)
    
    data = {"num": [1, 2, 3, 4, 5, 6]}
    result = sj_pipe.execute(data)
    
    expected_arr_output = [
        {"squared": 1},
        {"squared": 4},
        {"squared": 9},
        {"squared": 16},
        {"squared": 25},
        {"squared": 36}
    ]
    assert result["arr_output"] == expected_arr_output
    assert result["squared"] == [1, 4, 9, 16, 25, 36]

def test_flow_split_join_invalid_list_length():
    """
    Test that an error is raised if inputs mapped to the same index have mismatched lengths.
    """
    def dummy_action(data):
        return {"out": data["A"]}
    
    inner_pipe = FlowPipe(
        inputs=["A", "B"],
        outputs=["out"],
        action=dummy_action
    )
    # Both 'A' and 'B' are zipped together, so their lists must be the same length.
    input_mapping = {"A": "i", "B": "i"}
    sj_pipe = FlowSplitJoinPipe(inner_pipe, input_mapping=input_mapping, max_parallel=0)
    
    data = {"A": [1, 2, 3], "B": [4, 5]}  # Mismatched lengths.
    with pytest.raises(ValueError, match="differing lengths"):
        sj_pipe.execute(data)