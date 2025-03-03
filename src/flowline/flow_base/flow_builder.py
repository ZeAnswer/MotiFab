from flowline import (
    FlowSource,
    FlowManager,
)

def build_flow(pipe_config):
    """
    Builds a flow from a dictionary-based configuration of pipes and their connections.
    
    Args:
        pipe_config (dict): A dictionary where:
            - keys are unique names for pipes
            - values are dicts with:
                - 'type': The pipe class (e.g., GenerateRandomMotifsPipe)
                - 'init': Dict of initialization parameters for the pipe
                - 'upstream_pipes': Dict mapping upstream pipe names to their outputs
                  that should be connected to this pipe's inputs
    
    Returns:
        tuple: (FlowManager, dict of pipes) where:
            - FlowManager is initialized with the source node
            - dict of pipes maps pipe names to their instances
    """
    # Create a source node for external inputs
    source = FlowSource()
    
    # Dictionary to store all created pipes
    pipes = {'*': source}
    
    # Keep track of pipes that need to be created later due to dependencies
    pending_pipes = dict(pipe_config)  # Make a copy to avoid modifying the original
    
    # First pass: create all pipes that don't depend on other pipes
    while pending_pipes:
        made_progress = False
        new_pending = {}
        
        for pipe_name, pipe_info in pending_pipes.items():
            # Skip pipes we've already created
            if pipe_name in pipes:
                continue
            
            # Check if all upstream pipes exist
            upstream_names = set(pipe_info.get('upstream_pipes', {}).keys())
            # Remove the source node from dependencies check
            if '*' in upstream_names:
                upstream_names.remove('*')
                
            # If all upstream pipes exist, we can create this pipe
            if all(name in pipes for name in upstream_names):
                pipe_type = pipe_info['type']
                init_params = pipe_info.get('init', {})
                
                # Create the pipe with initialization parameters
                pipes[pipe_name] = pipe_type(**init_params)
                made_progress = True
            else:
                # Store for later creation
                new_pending[pipe_name] = pipe_info
                
        # If we made no progress and there are still pipes to create,
        # we have a circular dependency
        if not made_progress and new_pending:
            missing_deps = {}
            for name, info in new_pending.items():
                upstream = set(info.get('upstream_pipes', {}).keys())
                if '*' in upstream:
                    upstream.remove('*')
                missing = [up for up in upstream if up not in pipes]
                missing_deps[name] = missing
            
            raise ValueError(f"Circular dependency detected in pipe configuration: {missing_deps}")
            
        # Update the pending pipes
        pending_pipes = new_pending
    
    # Second pass: connect all pipes based on upstream-downstream relationships
    for pipe_name, pipe_info in pipe_config.items():
        if pipe_name == '*':  # Skip source node
            continue
            
        pipe = pipes[pipe_name]
        
        # Check if this pipe has no upstream connections defined
        if not pipe_info.get('upstream_pipes'):
            print(f"Warning: Pipe '{pipe_name}' has no upstream connections defined. "
                  f"Automatically connecting to source node.")
            # Add empty mapping to source
            source.add_downstream(pipe, {})
        else:
            # Connect this pipe to its upstream pipes
            for upstream_name, mappings in pipe_info.get('upstream_pipes', {}).items():
                upstream_pipe = pipes.get(upstream_name)
                if not upstream_pipe:
                    raise ValueError(f"Upstream pipe '{upstream_name}' not found for '{pipe_name}'")
                    
                # Add the connection with output mapping
                upstream_pipe.add_downstream(pipe, outputMapping=mappings)

    # Check if we have any orphaned nodes that aren't reachable from the source
    connected_nodes = set()
    
    def collect_downstream(node):
        """Recursively collect all nodes reachable from this node"""
        if node in connected_nodes:
            return
        connected_nodes.add(node)
        for downstream in node.get_downstream():
            collect_downstream(downstream)
    
    # Start the collection from the source
    collect_downstream(source)
    
    # Find orphaned nodes
    orphaned = []
    for name, pipe in pipes.items():
        if name != '*' and pipe not in connected_nodes:
            orphaned.append(name)
    
    if orphaned:
        print(f"Warning: Found orphaned nodes that are not connected to the flow: {orphaned}")
        print("These nodes will not be executed during flow execution.")

    # Create a FlowManager with the source node
    manager = FlowManager(source)
    
    try:
        # Validate the flow to make sure all connections are properly set up
        manager.initialize_and_validate_flow()
        print("Flow validation successful!")
    except RuntimeError as e:
        # If validation fails, provide more debugging information
        print(f"Flow validation failed: {str(e)}")
        print("\nCurrent flow structure:")
        for name, pipe in pipes.items():
            if name == '*':
                print(f"Source node with {len(pipe.get_downstream())} downstream connections")
                continue
            print(f"Pipe '{name}': {pipe}")
            print(f"  Inputs: {pipe.get_inputs()}")
            print(f"  Required inputs: {pipe.get_required_inputs()}")
            print(f"  Optional inputs: {pipe.get_optional_inputs()}")
            print(f"  Outputs: {pipe.get_outputs()}")
            print(f"  Downstream connections: {len(pipe.get_downstream())}")
        raise
    
    return manager, pipes