# resolvers/resolver_manager.py
class ResolverManager:
    """Manages and applies a sequence of resolvers."""
    def __init__(self, resolvers=None):
        """
        Initialize the manager with an optional list of resolvers.
        :param resolvers: A list of resolvers (constraints or modifiers).
        """
        self.resolvers = resolvers or []

    def add_resolver(self, resolver):
        """
        Add a resolver to the manager.
        :param resolver: An instance of a resolver (Constraint or Resolver).
        """
        self.resolvers.append(resolver)

    def apply(self, data):
        """
        Apply all resolvers sequentially to the data.
        :param data: The initial dataset to process.
        :return: The processed dataset after applying all resolvers.
        """
        for resolver in self.resolvers:
            data = resolver.apply(data)
        return data