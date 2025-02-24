class FlowPipe:
    """
    Represents a processing unit (a node) in the system.
    """

    def __init__(self, inputs=None, outputs=None, action=None):
        """
        Initialize a FlowPipe.

        :param inputs: List of required input names.
        :param outputs: List of output names produced.
        :param action: Function to process inputs and return outputs.
        """
        self.inputs = inputs if inputs is not None else []
        self.outputs = outputs if outputs is not None else []
        self.action = action
        self.downstream = []

    def get_inputs(self):
        """Returns the list of required input names."""
        return self.inputs

    def get_outputs(self):
        """Returns the list of output names produced."""
        return self.outputs

    def get_downstream(self):
        """Returns the downstream FlowPipes."""
        return self.downstream

    def set_downstream(self, nodes):
        """Sets the downstream FlowPipes."""
        self.downstream = nodes

    def add_downstream(self, node):
        """Adds a downstream FlowPipe."""
        self.downstream.append(node)

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