import copy
import re

def merge_data(existing, new):
    """
    Merge two dictionaries. If the same key exists in both, then the value is merged into a list.
    (This simple merge can be adjusted depending on the expected downstream semantics.)
    """
    for k, v in new.items():
        if k in existing:
            # If not already a list, convert
            if not isinstance(existing[k], list):
                existing[k] = [existing[k]]
            existing[k].append(v)
        else:
            existing[k] = v
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
        
        # Add the starting node to the manager.
        self._enqueue(start_node)
    
    def _enqueue(self, node):
        """Add a node to the queue and initialize its accumulated data if not already present."""
        if node not in self.accumulated:
            self.accumulated[node] = {}  # start with an empty dict
            self.queue.append(node)
    
    def run(self, initial_data):
        """
        Runs the flow, starting with the provided initial_data (a dict).
        The initial_data is assigned to the starting node.
        
        :param initial_data: A dict representing the initial input to the starting FlowPipe.
        :return: A dict of final outputs from nodes with no downstream.
        """
        # We assume that the first node in the queue is the starting node.
        # Set its accumulated input to the provided initial_data.
        if not self.queue:
            raise RuntimeError("No starting node in the pipeline.")
        start_node = self.queue[0]
        self.accumulated[start_node] = copy.deepcopy(initial_data)
        
        # Process until the queue is empty.
        while self.queue:
            progress_made = False
            
            # Iterate over a copy of the queue (we may modify self.queue during iteration).
            for node in list(self.queue):
                # Check if all required inputs are present.
                required = node.get_inputs()
                available = self.accumulated[node]
                if all(name in available for name in required):
                    # Prepare a subset dictionary with only the required inputs.
                    # Use deep copy to avoid accidental modification.
                    node_inputs = {name: copy.deepcopy(available[name]) for name in required}
                    
                    # Execute the node.
                    try:
                        node_output = node.execute(node_inputs)
                    except Exception as e:
                        raise RuntimeError(f"Error executing node {node}: {e}")
                    
                    # Remove the node from the queue and its accumulated entry.
                    self.queue.remove(node)
                    del self.accumulated[node]
                    
                    progress_made = True
                    
                    # If the node has downstream nodes, merge its output into each.
                    downstreams = node.get_downstream()
                    if downstreams:
                        for down_node in downstreams:
                            if down_node not in self.accumulated:
                                self.accumulated[down_node] = {}
                                self.queue.append(down_node)
                            # Merge the current node's output into the downstream's accumulator.
                            merge_data(self.accumulated[down_node], node_output)
                    else:
                        # No downstream nodes; treat its output as final.
                        merge_data(self.final_outputs, node_output)
                    
                    # Break out of the loop to restart checking from the front.
                    break
            
            # If no node was ready to execute in this pass, then there is a deadlock.
            if not progress_made:
                unresolved = [node for node in self.queue]
                raise RuntimeError(f"Deadlock: The following nodes still lack required inputs: " +
                                   ", ".join(str(node.get_inputs()) for node in unresolved))
        
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

        # Now, for every node in the visited set, simulate its “accumulated inputs”.
        # For the start node, if it requires inputs, we simulate that the external system provides exactly one value per input.
        simulated_inputs = {}  # node -> dict: input name -> list of source strings.
        for node in visited:
            simulated_inputs[node] = {}

        # For the start node, assign one unique external value for each required input.
        for req in start_node.get_inputs():
            simulated_inputs[start_node][req] = [f"ext_{req}"]

        # For every other node, accumulate outputs from its upstream nodes.
        # Here we assume that the upstream node's outputs are given by node.get_outputs() (a list of names).
        for node in visited:
            if node is start_node:
                continue
            # For each upstream node, add each output that matches one of node's required inputs.
            acc = {}
            for up in upstream.get(node, []):
                for out_name in up.get_outputs():
                    if out_name in node.get_inputs():
                        # Check for duplicate: if already exists, we flag it.
                        if out_name in acc:
                            raise RuntimeError(
                                f"Duplicate input '{out_name}' for node {node}. "
                                f"Upstream nodes {acc[out_name]} and {up} both supply it. "
                                "Consider using an output filter to resolve this."
                            )
                        acc[out_name] = up  # store the upstream node reference (for error messaging)
            # Save the simulated inputs for this node as a dict mapping input name -> list (of length 1 if present)
            # For our simulation we need to ensure that every required input is provided.
            for req in node.get_inputs():
                if req not in acc:
                    raise RuntimeError(f"Node {node} requires input '{req}' which is not provided by any upstream node.")
                # Wrap it as a list to follow our convention.
                simulated_inputs[node][req] = [f"from_{str(acc[req])}"]
        
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