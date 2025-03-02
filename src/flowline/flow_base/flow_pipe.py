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

    def __init__(self, output_map):
        """
        Initialize a FlowOutputRenamer.

        :param output_map: Dictionary mapping old output names to new output names.
        """
        super().__init__(inputs=list(output_map.keys()), outputs=list(output_map.values()), action=self._rename_action)
        self.output_map = output_map

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

