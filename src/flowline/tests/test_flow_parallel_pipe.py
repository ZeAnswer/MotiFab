# import pytest
# from unittest.mock import Mock
# import multiprocessing as mp
# from flowline import FlowParallelPipe, FlowPipe

# # --- Helper functions to create FlowPipe instances with specific actions ---

# def square_action(data):
#     """Simple action that squares the input value."""
#     return {"output": data["input"] ** 2}

# def slow_square_action(data):
#     """Action that squares the input value but with a delay to simulate processing time."""
#     import time
#     time.sleep(0.1)  # Simulate processing time
#     return {"output": data["input"] ** 2}

# def failing_action(data):
#     """Action that raises an exception for testing error handling."""
#     if data["input"] == 3:  # Fail on specific input
#         raise ValueError("Test error for value 3")
#     return {"output": data["input"] ** 2}

# # Additional helper functions for custom name tests
# def square_x_action(data):
#     """Square the x input and return as y output."""
#     return {"y": data["x"] ** 2}

# def return_initial_list(data):
#     """Return a predefined list."""
#     return {"initial_list": [1, 2, 3, 4]}

# def square_value_action(data):
#     """Square the value input and return as squared output."""
#     return {"squared": data["value"] ** 2}

# def sum_values_action(data):
#     """Sum all values in the squared_numbers list."""
#     return {"sum": sum(data["squared_numbers"])}

# # --- Test Cases ---

# def test_flow_parallel_pipe_basic():
#     """Test basic parallel processing of a list."""
#     # Create a simple pipe that squares its input
#     square_pipe = FlowPipe(inputs=["input"], outputs=["output"], action=square_action)
    
#     # Create a parallel pipe that applies the square_pipe to each element in parallel
#     parallel_pipe = FlowParallelPipe(square_pipe, max_parallel=2)
    
#     # Execute the parallel pipe with a list of inputs
#     result = parallel_pipe.execute({"input": [1, 2, 3, 4, 5]})
    
#     # Check the result
#     assert "output" in result
#     assert result["output"] == [1, 4, 9, 16, 25]

# def test_flow_parallel_pipe_custom_names():
#     """Test parallel pipe with custom input/output names."""
#     # Create a simple pipe that squares its input
#     square_pipe = FlowPipe(inputs=["x"], outputs=["y"], action=square_x_action)
    
#     # Create a parallel pipe with custom input/output names
#     parallel_pipe = FlowParallelPipe(
#         square_pipe, 
#         max_parallel=2,
#         input_name="numbers",
#         output_name="squared_numbers"
#     )
    
#     # Execute the parallel pipe with a list of inputs
#     result = parallel_pipe.execute({"numbers": [1, 2, 3, 4, 5]})
    
#     # Check the result
#     assert "squared_numbers" in result
#     assert result["squared_numbers"] == [1, 4, 9, 16, 25]

# def test_flow_parallel_pipe_error_handling():
#     """Test that errors in parallel execution are properly propagated."""
#     # Create a pipe that fails on specific input
#     failing_pipe = FlowPipe(inputs=["input"], outputs=["output"], action=failing_action)
    
#     # Create a parallel pipe
#     parallel_pipe = FlowParallelPipe(failing_pipe, max_parallel=2)
    
#     # Execute the parallel pipe with inputs including one that will cause an error
#     with pytest.raises(RuntimeError) as excinfo:
#         parallel_pipe.execute({"input": [1, 2, 3, 4, 5]})
    
#     # Check that the error message contains the original error
#     assert "Error executing parallel pipe" in str(excinfo.value)
#     assert "Test error for value 3" in str(excinfo.value)

# def test_flow_parallel_pipe_performance():
#     """Test that parallel execution is faster than sequential for CPU-bound tasks."""
#     # Skip this test on CI environments or with limited CPU resources
#     if mp.cpu_count() < 2:
#         pytest.skip("Skipping performance test on system with limited CPU resources")
        
#     # Create a pipe with a slow operation
#     slow_pipe = FlowPipe(inputs=["input"], outputs=["output"], action=slow_square_action)
    
#     # Create both sequential and parallel versions
#     sequential_pipe = FlowParallelPipe(slow_pipe, max_parallel=1)  # Force sequential
#     parallel_pipe = FlowParallelPipe(slow_pipe, max_parallel=mp.cpu_count())  # Use all cores
    
#     # Create a large input list to make the difference more noticeable
#     input_list = list(range(20))
    
#     # Time the sequential execution
#     import time
#     start_time = time.time()
#     sequential_result = sequential_pipe.execute({"input": input_list})
#     sequential_time = time.time() - start_time
    
#     # Time the parallel execution
#     start_time = time.time()
#     parallel_result = parallel_pipe.execute({"input": input_list})
#     parallel_time = time.time() - start_time
    
#     # Verify results are the same
#     assert sequential_result["output"] == parallel_result["output"]
    
#     # Verify parallel is faster (giving some margin for system variability)
#     # When running with multiple cores, parallel should be at least 1.5x faster
#     if mp.cpu_count() > 2:
#         assert parallel_time < sequential_time * 0.8, f"Parallel ({parallel_time:.2f}s) not significantly faster than sequential ({sequential_time:.2f}s)"

# def test_flow_parallel_pipe_validation():
#     """Test validation of the map pipe during initialization."""
#     # Helper function for a pipe with multiple inputs
#     def sum_action(data):
#         return {"c": data["a"] + data["b"]}
    
#     # Helper function for a pipe with multiple outputs
#     def multi_output_action(data):
#         return {"b": data["a"], "c": data["a"] * 2}
    
#     # Test with a pipe that has multiple inputs
#     invalid_pipe1 = FlowPipe(inputs=["a", "b"], outputs=["c"], action=sum_action)
#     with pytest.raises(ValueError, match="map_pipe must have exactly one input"):
#         FlowParallelPipe(invalid_pipe1)
    
#     # Test with a pipe that has multiple outputs
#     invalid_pipe2 = FlowPipe(inputs=["a"], outputs=["b", "c"], action=multi_output_action)
#     with pytest.raises(ValueError, match="map_pipe must have exactly one output"):
#         FlowParallelPipe(invalid_pipe2)
    
#     # Test with a non-pipe object
#     with pytest.raises(TypeError, match="map_pipe must be a FlowPipe instance"):
#         FlowParallelPipe("not a pipe")

# def test_flow_parallel_pipe_input_validation():
#     """Test input validation during execution."""
#     # Create a valid mapping pipe
#     square_pipe = FlowPipe(inputs=["input"], outputs=["output"], action=square_action)
#     parallel_pipe = FlowParallelPipe(square_pipe)
    
#     # Test with a non-list input
#     with pytest.raises(ValueError, match="must be a list"):
#         parallel_pipe.execute({"input": "not a list"})
    
#     # Test with missing input
#     with pytest.raises(ValueError, match="not found in data"):
#         parallel_pipe.execute({"wrong_name": [1, 2, 3]})

# def test_flow_parallel_pipe_in_pipeline():
#     """Test using a parallel pipe as part of a larger pipeline."""
#     # Create a pipeline with a parallel pipe in the middle
#     start = FlowPipe(inputs=[], outputs=["initial_list"], action=return_initial_list)
    
#     square_pipe = FlowPipe(inputs=["value"], outputs=["squared"], action=square_value_action)
#     parallel_square = FlowParallelPipe(square_pipe, input_name="numbers", output_name="squared_numbers")
    
#     end = FlowPipe(
#         inputs=["squared_numbers"], 
#         outputs=["sum"], 
#         action=sum_values_action
#     )
    
#     # Connect the pipes
#     start.add_downstream(parallel_square, outputMapping={"initial_list": "numbers"})
#     parallel_square.add_downstream(end)
    
#     # Create a flow manager and run the pipeline
#     from flowline import FlowManager
#     manager = FlowManager(start)
#     result = manager.run({})
    
#     # Check the result (1^2 + 2^2 + 3^2 + 4^2 = 1 + 4 + 9 + 16 = 30)
#     assert result["sum"] == 30