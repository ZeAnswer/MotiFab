import itertools
import multiprocessing as mp

class FlowPipe:
    """
    Represents a processing unit (a node) in the system.
    """

    def __init__(self, inputs=None, outputs=None, action=None):
        """
        Initialize a FlowPipe.

        :param inputs: List of input names required by this FlowPipe.
        :param outputs: List of output names produced by this FlowPipe.
        :param action: Function that takes a data dictionary and returns a dictionary of outputs.
        """
        self.inputs = inputs or []
        self.outputs = outputs or []
        self.action = action
        self.downstream = []
        self.outputMappings = {}
        self.optional_inputs = []  # Tracks which inputs have default values

    def get_inputs(self):
        """Returns the list of required input names."""
        return self.inputs

    def get_optional_inputs(self):
        """Returns the list of input names that have default values."""
        return self.optional_inputs

    def set_optional_inputs(self, optional_inputs):
        """Sets which inputs are optional (have default values)."""
        self.optional_inputs = optional_inputs

    def get_required_inputs(self):
        """Returns the list of input names that must be provided by upstream nodes."""
        return [inp for inp in self.inputs if inp not in self.optional_inputs]

    def get_outputs(self):
        """Returns the list of output names produced."""
        return self.outputs

    def get_downstream(self):
        """Returns the downstream FlowPipes."""
        return self.downstream

    def set_downstream(self, nodes):
        """Sets the downstream FlowPipes."""
        self.downstream = nodes

    def add_downstream(self, node, outputMapping=None):
        """Adds a downstream FlowPipe.
        If outputMapping is provided, it will be used to map the outputs of this pipe to the inputs of the downstream pipe."""
        self.downstream.append(node)
        if outputMapping:
            self.outputMappings[node] = outputMapping
            
    def get_output_mapping_of(self, downstream_node):
        """Returns the output mapping for the specified downstream node."""
        return self.outputMappings.get(downstream_node)

    def execute(self, data):
        """
        Executes the FlowPipe's action using the provided data.

        :param data: Dictionary containing input values.
        :return: Dictionary containing output values.
        """
        if not self.action:
            raise RuntimeError(f"FlowPipe {self} has no action defined.")
        return self.action(data)

    def __str__(self):
        """Returns a debug-friendly representation of the FlowPipe."""
        return f"FlowPipe(Inputs: {self.inputs}, Outputs: {self.outputs}, Downstream: {len(self.downstream)})"
    
class FlowOutputFilter(FlowPipe):
    """
    A specialized FlowPipe that filters outputs, ensuring only specified outputs are passed forward.
    """

    def __init__(self, outputs):
        """
        Initialize a FlowOutputFilter.

        :param outputs: List of output names to be passed forward.
        """
        super().__init__(inputs=outputs, outputs=outputs, action=self._filter_action)

    def _filter_action(self, data):
        """
        Filters the given data, keeping only the specified outputs.

        :param data: Dictionary containing input values.
        :return: Dictionary containing only the specified outputs.
        """
        return {key: data[key] for key in self.outputs if key in data}

    def __str__(self):
        """Returns a debug-friendly representation of the FlowOutputFilter."""
        return f"FlowOutputFilter(Passing: {self.outputs})"
    
class FlowOutputRenamer(FlowPipe):
    """
    A specialized FlowPipe that renames outputs, ensuring only specified outputs are passed forward.
    
    """

    def __init__(self, output_mapping):
        """
        Initialize a FlowOutputRenamer.

        :param output_map: Dictionary mapping old output names to new output names.
        """
        super().__init__(inputs=list(output_mapping.keys()), outputs=list(output_mapping.values()), action=self._rename_action)
        self.output_map = output_mapping

    def _rename_action(self, data):
        """
        Renames the given outputs according to the output_map.

        :param data: Dictionary containing input values.
        :return: Dictionary containing renamed outputs.
        """
        return {self.output_map[key]: data[key] for key in self.inputs if key in data}

    def __str__(self):
        """Returns a debug-friendly representation of the FlowOutputRenamer."""
        return f"FlowOutputRenamer(Renaming: {self.output_map})"
    
class FlowSource(FlowPipe):
    """
    A specialized FlowPipe that provides initial inputs to the system.
    
    FlowSource doesn't have predefined outputs - instead, it determines what
    outputs it can provide based on the output mappings to its downstream nodes.
    """
    def __init__(self, initial_inputs=None):
        """
        Initialize a FlowSource.

        :param initial_inputs: Dictionary containing initial inputs for the pipeline.
        """
        # Initialize with no inputs/outputs - they'll be determined by downstream connections
        super().__init__(inputs=[], outputs=[], action=self._source_action)
        self.initial_inputs = initial_inputs or {}

    def get_outputs(self):
        """
        Returns all outputs this source can provide, based on output mappings.
        These are determined by the keys in all output mappings to downstream nodes.
        """
        # Collect all output keys from all mappings
        all_outputs = []
        for node in self.downstream:
            mapping = self.get_output_mapping_of(node)
            if mapping:
                all_outputs.extend(mapping.keys())
        return list(set(all_outputs))
    
    def get_inputs(self):
        """ Return the inputs based on the output mappings """
        for node in self.downstream:
            mapping = self.get_output_mapping_of(node)
            if mapping:
                return list(mapping.values())
        return []
    
    def add_downstream(self, node, outputMapping):
        """
        Adds a downstream FlowPipe with required output mapping.
        
        :param node: The downstream FlowPipe to add
        :param outputMapping: Dict mapping source keys to downstream input keys. This is mandatory
        """
        
        if outputMapping is None:
            raise ValueError("FlowSource requires an explicit output mapping (or {}) for downstream nodes.")
        
        super().add_downstream(node, outputMapping or {})

    def _source_action(self, data):
        """
        Provides the initial inputs to the system.

        :param data: Dictionary containing input values.
        :return: Dictionary containing initial inputs.
        """
        return data

    def __str__(self):
        """Returns a debug-friendly representation of the FlowSource."""
        # Include what outputs this source provides based on mappings
        outputs = self.get_outputs()
        return f"FlowSource(Providing: {outputs})"

class FlowSubPipeline(FlowPipe):
    """
    A specialized FlowPipe that encapsulates an entire pipeline as a sub-pipeline.
    
    This allows for composition of pipelines, where one pipeline can be used as a component
    in another pipeline. When executed, it runs the sub-pipeline with its inputs and 
    returns the outputs.
    """
    
    def __init__(self, flow_manager, inputs=None, outputs=None, output_mapping=None):
        """
        Initialize a FlowSubPipeLine.
        
        :param flow_manager: FlowManager instance containing the sub-pipeline to execute.
        :param inputs: List of input names required by this pipe (passed to the sub-pipeline).
        :param outputs: List of output names produced by this pipe (expected from the sub-pipeline).
        :param output_mapping: Optional dict mapping sub-pipeline output names to this pipe's output names.
                              If None, output names are passed through directly.
        """
        self.sub_manager = flow_manager
        self.output_mapping = output_mapping or {}
        
        # Initialize with the provided inputs/outputs and our execution function
        super().__init__(
            inputs=inputs or [], 
            outputs=outputs or [],
            action=self._execute_sub_pipeline
        )
    
    def _execute_sub_pipeline(self, data):
        """
        Executes the sub-pipeline with the provided data.
        
        :param data: Dictionary containing input values for the sub-pipeline.
        :return: Dictionary containing output values from the sub-pipeline, possibly renamed.
        """
        try:
            # Run the sub-pipeline with the provided data
            result = self.sub_manager.run(data)
            
            # Apply output mapping if specified
            if self.output_mapping:
                mapped_result = {}
                for key, value in result.items():
                    # If this key should be mapped to a new name
                    mapped_key = self.output_mapping.get(key, key)
                    mapped_result[mapped_key] = value
                return mapped_result
            else:
                return result
                
        except Exception as e:
            raise RuntimeError(f"Error executing sub-pipeline: {e}")
    
    def __str__(self):
        """Returns a debug-friendly representation of the FlowSubPipeLine."""
        return f"FlowSubPipeLine(Inputs: {self.inputs}, Outputs: {self.outputs})"

# class FlowMapPipe(FlowPipe):
#     """
#     A specialized FlowPipe that applies a mapping operation to each element of an input list.
    
#     This pipe takes exactly one input, which must be a list, and produces one output, which is also a list.
#     The mapping is done by applying the provided pipe to each element of the input list.
#     """
    
#     def __init__(self, map_pipe, input_name=None, output_name=None):
#         """
#         Initialize a FlowMapPipe.
        
#         :param map_pipe: FlowPipe instance to use as the mapping function. Must have exactly one input and one output.
#         :param input_name: Name of the input containing the list to process. If None, uses the map_pipe's first input name.
#         :param output_name: Name of the output to produce. If None, uses the map_pipe's first output name.
#         """
#         # Validate the map_pipe
#         if not isinstance(map_pipe, FlowPipe):
#             raise TypeError("map_pipe must be a FlowPipe instance")
        
#         # Check that map_pipe has exactly one input and one output
#         if len(map_pipe.get_inputs()) != 1:
#             raise ValueError(f"map_pipe must have exactly one input, got {len(map_pipe.get_inputs())}")
#         if len(map_pipe.get_outputs()) != 1:
#             raise ValueError(f"map_pipe must have exactly one output, got {len(map_pipe.get_outputs())}")
        
#         self.map_pipe = map_pipe
        
#         # Get the input and output names from the map_pipe if not provided
#         self.map_input_name = map_pipe.get_inputs()[0]
#         self.map_output_name = map_pipe.get_outputs()[0]
        
#         # Use provided names or default to map_pipe's names
#         input_name = input_name or self.map_input_name
#         output_name = output_name or self.map_output_name
        
#         # Initialize with the determined input/output and our mapping action
#         super().__init__(
#             inputs=[input_name],
#             outputs=[output_name],
#             action=self._map_action
#         )
        
#     def _map_action(self, data):
#         """
#         Maps each element in the input list using the provided map_pipe.
        
#         :param data: Dictionary containing the input list.
#         :return: Dictionary containing the mapped output list.
#         """
#         input_name = self.inputs[0]
#         output_name = self.outputs[0]
        
#         if input_name not in data:
#             raise ValueError(f"Input '{input_name}' not found in data")
        
#         input_list = data[input_name]
        
#         if not isinstance(input_list, list):
#             raise ValueError(f"Input '{input_name}' must be a list, got {type(input_list)}")
        
#         # Apply the map_pipe to each element in the list
#         output_list = []
#         for item in input_list:
#             # Prepare input for the map pipe
#             map_input = {self.map_input_name: item}
            
#             # Execute the map pipe
#             try:
#                 map_result = self.map_pipe.execute(map_input)
#                 if self.map_output_name not in map_result:
#                     raise ValueError(f"Map pipe did not produce expected output '{self.map_output_name}'")
#                 output_list.append(map_result[self.map_output_name])
#             except Exception as e:
#                 raise RuntimeError(f"Error executing map pipe: {e}")
                
#         return {output_name: output_list}
    
#     def __str__(self):
#         """Returns a debug-friendly representation of the FlowMapPipe."""
#         return f"FlowMapPipe(Mapping {self.inputs[0]} to {self.outputs[0]} using {self.map_pipe})"

# class FlowParallelPipe(FlowPipe):
#     """
#     A specialized FlowPipe that applies a pipe to each element of an input list in parallel using subprocesses.
    
#     This pipe takes exactly one input, which must be a list, and produces one output, which is also a list.
#     The mapping is done by applying the provided pipe to each element of the input list in parallel.
#     """
    
#     def __init__(self, map_pipe, max_parallel=5, input_name=None, output_name=None):
#         """
#         Initialize a FlowParallelPipe.
        
#         :param map_pipe: FlowPipe instance to use as the mapping function. Must have exactly one input and one output.
#         :param max_parallel: Maximum number of parallel processes to run simultaneously.
#         :param input_name: Name of the input containing the list to process. If None, uses the map_pipe's first input name.
#         :param output_name: Name of the output to produce. If None, uses the map_pipe's first output name.
#         """
#         # Validate the map_pipe
#         if not isinstance(map_pipe, FlowPipe):
#             raise TypeError("map_pipe must be a FlowPipe instance")
        
#         # Check that map_pipe has exactly one input and one output
#         if len(map_pipe.get_inputs()) != 1:
#             raise ValueError(f"map_pipe must have exactly one input, got {len(map_pipe.get_inputs())}")
#         if len(map_pipe.get_outputs()) != 1:
#             raise ValueError(f"map_pipe must have exactly one output, got {len(map_pipe.get_outputs())}")
        
#         self.map_pipe = map_pipe
#         self.max_parallel = max_parallel
        
#         # Get the input and output names from the map_pipe if not provided
#         self.map_input_name = map_pipe.get_inputs()[0]
#         self.map_output_name = map_pipe.get_outputs()[0]
        
#         # Use provided names or default to map_pipe's names
#         input_name = input_name or self.map_input_name
#         output_name = output_name or self.map_output_name
        
#         # Initialize with the determined input/output and our parallel mapping action
#         super().__init__(
#             inputs=[input_name],
#             outputs=[output_name],
#             action=self._parallel_action
#         )
    
#     def _process_item(self, item):
#         """
#         Process a single item using the map_pipe.
#         This method must be at instance level (not local function) to be properly pickled.
        
#         :param item: The individual item to process.
#         :return: The processed result or Exception if an error occurred.
#         """
#         # Prepare input for the map pipe
#         map_input = {self.map_input_name: item}
        
#         # Execute the map pipe
#         try:
#             map_result = self.map_pipe.execute(map_input)
#             if self.map_output_name not in map_result:
#                 raise ValueError(f"Map pipe did not produce expected output '{self.map_output_name}'")
#             return map_result[self.map_output_name]
#         except Exception as e:
#             # Return exception as result so we can handle it in the main process
#             return e
        
#     def _parallel_action(self, data):
#         """
#         Maps each element in the input list using the provided map_pipe in parallel.
        
#         :param data: Dictionary containing the input list.
#         :return: Dictionary containing the mapped output list.
#         """
#         input_name = self.inputs[0]
#         output_name = self.outputs[0]
        
#         if input_name not in data:
#             raise ValueError(f"Input '{input_name}' not found in data")
        
#         input_list = data[input_name]
        
#         if not isinstance(input_list, list):
#             raise ValueError(f"Input '{input_name}' must be a list, got {type(input_list)}")
        
#         # Process items in parallel batches
#         output_list = []
        
#         # Use a context manager to ensure proper cleanup
#         with mp.Pool(processes=self.max_parallel) as pool:
#             # Use the instance method to process items
#             results = pool.map(self._process_item, input_list)
            
#             # Check for exceptions in results
#             for i, result in enumerate(results):
#                 if isinstance(result, Exception):
#                     raise RuntimeError(f"Error executing parallel pipe for item {i}: {result}")
#                 output_list.append(result)
                
#         return {output_name: output_list}
    
#     def __str__(self):
#         """Returns a debug-friendly representation of the FlowParallelPipe."""
#         return f"FlowParallelPipe(Mapping {self.inputs[0]} to {self.outputs[0]} using {self.map_pipe}, max_parallel={self.max_parallel})"
    
class FlowSplitJoinPipe(FlowPipe):
    """
    A specialized FlowPipe that splits inputs to drive an inner pipe multiple times
    and then joins (aggregates) the outputs.
    
    The inner pipe can have any number of inputs and outputs. The split–join behavior
    is controlled by an input_mapping dict. For each input expected by the inner pipe,
    the mapping value indicates how to treat that input:
      - If the value is a nonempty string (e.g. "i", "j"), the input is expected to be a list.
        Inputs with the same index label are zipped together, while inputs with different index
        labels will be iterated over via a Cartesian product.
      - If the value is None or not provided, the input is treated as a scalar and passed as-is.
    
    Additionally, you may set max_parallel > 0 to run the inner pipe calls in parallel (using
    a process pool). The final output will contain:
      - "arr_output": a list of all inner pipe output dictionaries,
      - Plus, for each output key produced by the inner pipe, an array of its values.
    """
    
    def __init__(self, inner_pipe, input_mapping={}, max_parallel=0):
        """
        Initialize the FlowSplitJoinPipe.
        
        Args:
            inner_pipe (FlowPipe): The pipe to be applied repeatedly.
                                     It can have multiple inputs/outputs.
            input_mapping (dict): A dict mapping each input name (as defined by inner_pipe.get_inputs())
                                  to an index label (string) or None.
                                  For example: {"X": "i", "Y": "j", "z": None}
            max_parallel (int): Maximum number of parallel processes to use (0 means serial execution).
        """
        if not isinstance(inner_pipe, FlowPipe):
            raise TypeError("inner_pipe must be a FlowPipe instance")
        # Save inner pipe and mapping
        self.inner_pipe = inner_pipe
        self.input_mapping = input_mapping or {}
        self.max_parallel = max_parallel

        # The outer pipe’s inputs are assumed to match the inner pipe’s expected inputs.
        inputs = inner_pipe.get_inputs()
        # The outer outputs will include:
        #  - "arr_output": a list of dictionaries (one per inner call)
        #  - plus one output for each inner pipe output.
        outputs = ["arr_output"] + list(inner_pipe.get_outputs())
        
        super().__init__(inputs=inputs, outputs=outputs, action=self._split_join_action)
        
    def _split_join_action(self, data):
        """
        Executes the inner pipe multiple times, based on how the inputs are split.
        
        The process is:
         1. For each input expected by the inner pipe, get the value from `data`.
         2. For each input, decide (using self.input_mapping) whether it should be
            iterated over (if mapped to a nonempty string) or treated as a scalar.
         3. For inputs mapped to an index, group them by index label.
            - Inputs sharing the same index label are zipped (and must have equal lengths).
            - Different index labels produce a Cartesian product of the indices.
         4. For each combination, build an input dictionary for the inner pipe and execute it.
         5. Aggregate all outputs into:
              - "arr_output": list of all inner results,
              - And for each inner output key, an array of the corresponding values.
        """
        # Prepare inner inputs: for each expected input, get its value from the outer data.
        inner_inputs = {}
        for key in self.inner_pipe.get_inputs():
            if key not in data:
                raise ValueError(f"Missing expected input '{key}' for inner pipe.")
            inner_inputs[key] = data[key]
            
        # Group inputs: for those with a mapping (truthy string) treat as list; otherwise scalar.
        groups = {}   # group label -> { input_name: list }
        scalars = {}  # input_name -> scalar value
        for key, value in inner_inputs.items():
            idx_label = self.input_mapping.get(key, None)
            if idx_label:
                if not isinstance(value, list):
                    raise ValueError(f"Input '{key}' is mapped to index '{idx_label}' but is not a list.")
                groups.setdefault(idx_label, {})[key] = value
            else:
                scalars[key] = value
        
        # For each group (i.e. each index label), check that all lists have the same length.
        for label, mapping in groups.items():
            lengths = [len(lst) for lst in mapping.values()]
            if len(set(lengths)) > 1:
                raise ValueError(f"Inputs mapped to index '{label}' have differing lengths: {lengths}")
        group_lengths = {label: len(next(iter(mapping.values()))) for label, mapping in groups.items()}
        
        # Determine the iteration: each distinct index label produces an independent iteration range.
        # They are independent (Cartesian product) if labels differ.
        if groups:
            # Sort the group labels to have a consistent order.
            group_labels = sorted(groups.keys())
            group_ranges = {label: range(group_lengths[label]) for label in group_labels}
            # Cartesian product over the index ranges of each group.
            index_combinations = list(itertools.product(*[group_ranges[label] for label in group_labels]))
        else:
            # If there are no group inputs, then we have a single iteration.
            group_labels = []
            index_combinations = [()]
        
        # Define a helper to execute the inner pipe for a given combination.
        def call_inner(combination):
            # Build the input dict for inner_pipe.
            selected = {}
            # For each group label, pick the corresponding element from each input in that group.
            for label, idx in zip(group_labels, combination):
                mapping = groups[label]
                for key, lst in mapping.items():
                    selected[key] = lst[idx]
            # Include scalar inputs unchanged.
            selected.update(scalars)
            return self.inner_pipe.execute(selected)
        
        results = []
        # If parallel execution is requested, use a process pool.
        if self.max_parallel > 0:
            # To allow parallel processing, we define a top-level helper that is picklable.
            tasks = [
                (self.inner_pipe, group_labels, groups, scalars, combination)
                for combination in index_combinations
            ]
            with mp.Pool(processes=self.max_parallel) as pool:
                parallel_results = pool.map(FlowSplitJoinPipe._process_combination, tasks)
            results = parallel_results
        else:
            # Serial execution.
            for combination in index_combinations:
                try:
                    res = call_inner(combination)
                except Exception as e:
                    raise RuntimeError(f"Error executing inner pipe for combination {combination}: {e}")
                results.append(res)
                
        # Aggregate outputs.
        aggregated = {}
        aggregated["arr_output"] = results
        # For each output key declared by the inner pipe, collect its values from each result.
        for out_key in self.inner_pipe.get_outputs():
            aggregated[out_key] = [res.get(out_key) for res in results]
        return aggregated

    @staticmethod
    def _process_combination(args):
        """
        Helper for parallel execution.
        
        Args:
            args (tuple): Contains (inner_pipe, group_labels, groups, scalars, combination)
        Returns:
            The output dictionary from inner_pipe.execute(selected), where `selected` is built
            from the given combination.
        """
        inner_pipe, group_labels, groups, scalars, combination = args
        selected = {}
        for label, idx in zip(group_labels, combination):
            mapping = groups[label]
            for key, lst in mapping.items():
                selected[key] = lst[idx]
        selected.update(scalars)
        return inner_pipe.execute(selected)

    def __str__(self):
        return (f"FlowSplitJoinPipe(Using inner pipe {self.inner_pipe}, "
                f"input_mapping={self.input_mapping}, "
                f"max_parallel={self.max_parallel})")