import copy
from flowline import FlowPipe

def merge_data(existing, new, mapping=None):
    """
    Merge two dictionaries. If the same key exists in both, then the value is overwritten.
    """
    for key, value in new.items():
        mapped_key = mapping[key] if mapping and key in mapping else key
        existing[mapped_key] = value
    return existing


class FlowManager:
    """
    Manages the flow of FlowPipe nodes.
    
    The FlowManager is initialized with a starting FlowPipe.
    It then maintains a queue of nodes to be executed, and an accumulator (a dict)
    mapping each node to its current input dictionary. Each node is only executed
    when all of its required input names are present. When a node runs, its outputs
    are merged into the accumulated inputs of its downstream nodes. If a node has no
    downstream nodes, its output is saved as a final output.
    """
    def __init__(self, start_node):
        """
        Initialize the FlowManager with the starting FlowPipe.
        
        :param start_node: The first FlowPipe node in the flow.
        """
        # A mapping: FlowPipe -> accumulated inputs (as a dict)
        self.accumulated = {}
        # Use a list as a queue. We’ll store nodes that still need to be processed.
        self.queue = []
        # Dictionary of final outputs (from nodes with no downstreams)
        self.final_outputs = {}
        
        # Store the start node
        self.source = start_node
        
        # Add the starting node to the manager.
        self._enqueue(start_node)
    
    def _enqueue(self, node:FlowPipe):
        """Add a node to the queue and initialize its accumulated data if not already present."""
        if node not in self.accumulated:
            self.accumulated[node] = {}  # Start with an empty dict
            self.queue.append(node)
    
    def run(self, override_data=None):
        """
        Run the flow with the provided initial data.
        
        :param override_data: Dictionary containing initial data to override source defaults.
        :return: Dictionary of final outputs.
        """
        # Validate the flow before running
        self.validate_flow()
        
        # Reset accumulated data and queue
        self.accumulated = {}
        self.queue = []
        self.final_outputs = {}
        
        # Initialize with the source node
        initial_data = {}
        
        # If source is a FlowSource, use its initial_inputs
        if isinstance(self.source, FlowPipe) and hasattr(self.source, 'initial_inputs'):
            initial_data.update(self.source.initial_inputs)
            
        # If override data is provided, it takes precedence
        if override_data:
            initial_data.update(override_data)
            
        # Start with the source node
        self._enqueue(self.source)
        self.accumulated[self.source] = initial_data

        # Track nodes that have been deferred due to missing inputs
        deferred_count = {}  # Count how many times each node has been deferred
        max_deferrals = 100  # Maximum times a node can be deferred before we consider it deadlocked
        
        # Process nodes until the queue is empty
        while self.queue:
            current_node = self.queue.pop(0)
            input_data = self.accumulated[current_node]
            
            # Check if all required inputs are present before executing
            missing_inputs = []
            for required_input in current_node.get_required_inputs():
                if required_input not in input_data:
                    missing_inputs.append(required_input)
            
            # If inputs are missing, defer execution by putting it back in the queue
            if missing_inputs:
                # Increment deferral count for this node
                count = deferred_count.get(current_node, 0) + 1
                deferred_count[current_node] = count
                
                # Check for potential deadlock (node deferred too many times)
                if count > max_deferrals:
                    raise RuntimeError(
                        f"Execution deadlock detected: Node {current_node} has been deferred {max_deferrals} times. "
                        f"Missing inputs: {missing_inputs}. Available inputs: {list(input_data.keys())}. "
                        f"This suggests a potential issue with the flow configuration or input data."
                    )
                
                # Put the node back at the end of the queue for later execution
                self.queue.append(current_node)
                continue
            
            # Execute the node (all required inputs are present)
            output_data = current_node.execute(input_data)
            
            # Ensure output_data is a dictionary, default to empty dict if None
            if output_data is None:
                output_data = {}
            
            # If the node has no downstream nodes, add its outputs to final_outputs
            if not current_node.get_downstream():
                self.final_outputs.update(output_data)
            else:
                # For each downstream node
                for downstream in current_node.get_downstream():
                    # Get the output mapping for this downstream node
                    output_mapping = current_node.get_output_mapping_of(downstream)
                    
                    # Map the outputs to the downstream node's inputs
                    mapped_data = {}
                    merge_data(mapped_data, output_data, output_mapping)
                    
                    # If the downstream node isn't in the accumulated dict, add it
                    if downstream not in self.accumulated:
                        self.accumulated[downstream] = {}
                        self.queue.append(downstream)
                    
                    # Merge the mapped data into the downstream node's accumulated data
                    self.accumulated[downstream].update(mapped_data)
        
        return self.final_outputs

    def validate_flow(self):
        """
        Validate the pipeline flow. This method performs three checks:
          1. Every node (except the start node) can have all its required inputs satisfied by its upstream nodes.
          2. No node receives duplicate input values (i.e. two different upstream nodes producing the same required input name).
          3. There are no cycles in the pipeline.
          4. Each node's declared outputs are unique, and (optionally) across sink nodes there are no duplicates.
        
        Returns:
          True if the pipeline passes validation.
          
        Raises:
          RuntimeError with an explanatory message if validation fails.
        """
        # A helper: build the graph of nodes (node -> list of downstream nodes) starting at self.start_node.
        # Also build a mapping: node -> set of upstream nodes.
        upstream = {}  # node -> set of immediate upstream nodes

        # We also need to detect cycles. We'll use a DFS.
        visited = set()
        rec_stack = set()

        def dfs(node):
            if node in rec_stack:
                raise RuntimeError(f"Cycle detected at node: {node}")
            if node in visited:
                return
            visited.add(node)
            rec_stack.add(node)
            # For each downstream node, add current node as upstream.
            for dn in node.get_downstream():
                upstream.setdefault(dn, set()).add(node)
                dfs(dn)
            rec_stack.remove(node)

        # Assume self.start_node is stored; if not, we take the first node in our queue.
        if not self.queue:
            raise RuntimeError("FlowManager has no nodes in the queue; nothing to validate.")
        start_node = self.queue[0]
        dfs(start_node)

        # Now, for every node in the visited set, simulate its "accumulated inputs".
        # For the start node, if it requires inputs, we simulate that the external system provides exactly one value per input.
        simulated_inputs = {}  # node -> dict: input name -> list of source strings.
        for node in visited:
            simulated_inputs[node] = {}

        # For the start node, assign one unique external value for each required input.
        for req in start_node.get_inputs():
            simulated_inputs[start_node][req] = [f"ext_{req}"]

        # For every other node, accumulate outputs from its upstream nodes.
        # If a required input is not provided by any upstream node, we will check if the node has a default value for it.
        for node in visited:
            if node is start_node:
                continue
                
            # Keep track of which inputs are provided by upstream nodes
            provided_inputs = {}
            
            # Check each upstream node and its outputs
            for up in upstream.get(node, []):
                output_mapping = up.get_output_mapping_of(node)
                
                # If this upstream node has an output mapping for the downstream node
                if output_mapping:
                    for out_name, mapped_name in output_mapping.items():
                        if mapped_name in node.get_inputs():
                            if mapped_name in provided_inputs:
                                raise RuntimeError(
                                    f"Duplicate input '{mapped_name}' for node {node}. "
                                    f"Upstream nodes {provided_inputs[mapped_name]} and {up} both supply it. "
                                    "Consider using an output filter to resolve this."
                                )
                            provided_inputs[mapped_name] = up
                            simulated_inputs[node][mapped_name] = [f"from_{str(up)}"]
                else:
                    # No mapping, check for direct match between outputs and inputs
                    for out_name in up.get_outputs():
                        if out_name in node.get_inputs():
                            if out_name in provided_inputs:
                                raise RuntimeError(
                                    f"Duplicate input '{out_name}' for node {node}. "
                                    f"Upstream nodes {provided_inputs[out_name]} and {up} both supply it. "
                                    "Consider using an output filter to resolve this."
                                )
                            provided_inputs[out_name] = up
                            simulated_inputs[node][out_name] = [f"from_{str(up)}"]
            
            # Check if all required inputs are provided
            required_inputs = node.get_required_inputs()
            for req in node.get_inputs():
                if req not in provided_inputs:
                    # If input is required and not provided, raise error
                    if req in required_inputs:
                        raise RuntimeError(f"Node {node} requires input '{req}' which is not provided by any upstream node.")
                    # Otherwise it's optional, simulate it with default value
                    else:
                        simulated_inputs[node][req] = [f"default_value"]
        
        # Next, check that each node's own outputs list has no duplicates.
        for node in visited:
            outs = node.get_outputs()
            if len(outs) != len(set(outs)):
                raise RuntimeError(f"Node {node} has duplicate outputs in its declaration: {outs}")

        # Finally, check that among sink nodes (nodes with no downstreams), no output name is produced twice.
        sink_outputs = {}
        for node in visited:
            if not node.get_downstream():  # sink node
                for out in node.get_outputs():
                    if out in sink_outputs:
                        raise RuntimeError(
                            f"Output '{out}' is produced by multiple sink nodes: {sink_outputs[out]} and {node}. "
                            "Consider using an output filter to resolve duplicate outputs."
                        )
                    sink_outputs[out] = node

        # If we reached here, validation passed.
        print("Flow validation passed.")
        return True