class FlowPipe:
    """
    Represents a processing unit (a node) in the system.
    """

    def __init__(self, inputs=None, outputs=None, action=None, external_inputs=None):
        """
        Initialize a FlowPipe.

        :param inputs: List of required input names.
        :param outputs: List of output names produced.
        :param action: Function to process inputs and return outputs.
        :param external_inputs: Optional dictionary of external inputs predefined for this pipe.
                                These values will be available in execution but can be overwritten.
        """
        self.inputs = inputs if inputs is not None else []
        self.outputs = outputs if outputs is not None else []
        self.action = action
        self.external_inputs = external_inputs if external_inputs is not None else {}
        self.downstream = []
        self.outputMappings = {}

    def get_inputs(self):
        """Returns the list of required input names."""
        return self.inputs

    def get_outputs(self):
        """Returns the list of output names produced."""
        return self.outputs
    
    def get_external_inputs(self):
        """Returns the external inputs."""
        return self.external_inputs

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