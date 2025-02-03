# resolvers/resolver.py
class Resolver:
    """Base class for resolvers."""
    def __init__(self, is_global=False, iterator=None, mask=None):
        """
        Initialize the resolver.
        :param is_global: Whether the resolver operates globally or per record.
        :param iterator: Optional custom iterator for per-record resolution. Must include all records.
        :param mask: Optional callable or list/array of booleans to selectively apply the resolver.
        """
        self.is_global = is_global
        self.iterator = iterator or (lambda data: data)
        self.mask = mask
        

    def apply(self, data):
        """
        Apply the resolver based on its type.
        :param data: The data to resolve.
        :return: The resolved data.
        """
        if self.is_global:
            return self._apply_global(data)
        else:
            return self._apply_per_record(data)

    def action(self, data):
        """
        Define the operation performed by the resolver.
        :param data: Either the entire dataset or a single record.
        :return: The result of the operation.
        """
        raise NotImplementedError("Subclasses must implement the `action` method.")

    def _apply_global(self, data):
        """Apply the resolver action globally."""
        return self.action(data)

    def _apply_per_record(self, data):
        """
        Default implementation for per-record resolvers.
        Applies the resolver only to records satisfying the mask.
        """
        results = []
        for i, record in enumerate(self.iterator(data)):
            if self._should_apply(record, i):
                results.append(self.action(record))
            else:
                results.append(record)  # Keep the original record if mask says skip
        return results
    
    def _should_apply(self, record, index):
        """
        Determine whether the resolver should be applied to a record or dataset.
        :param record: The record or dataset being checked.
        :param index: Index of the record (or None for global data).
        :return: True if the resolver should apply, False otherwise.
        """
        if callable(self.mask):
            return self.mask(record)
        elif isinstance(self.mask, (list, tuple)):
            return self.mask[index] if index is not None else True
        return True  # Default: Always apply

    
class GlobalResolver(Resolver):
    """Class for global resolvers."""
    def __init__(self, iterator=None, mask=None):
        """
        Initialize the global resolver.
        :param iterator: Optional custom iterator for global resolution.
        """
        super().__init__(is_global=True, iterator=iterator, mask=mask)
        
class Constraint(Resolver):
    """Class for constraints that filter data."""
    def _apply_per_record(self, data):
        """Filter records that satisfy the condition."""
        return [record for record in self.iterator(data) if self.action(record)]