import random
from flowline import FlowPipe

class UnitAmountConverterPipe(FlowPipe):
    """
    FlowPipe to convert a potentially percentage-based amount into an absolute number.
    
    Inputs:
        - "items" (list): A list of items to calculate the number from.
        - "amount" (int or str): Specifies how many items to select.
          Can be:
            - An integer: Absolute number of items.
            - A percentage string (e.g., "10%"): Percentage of total items.
    
    Outputs:
        - "amount" (int): The calculated absolute amount.
    
    Raises:
        - ValueError if amount format is invalid.
    """
    
    def __init__(self):
        super().__init__(
            inputs=["items", "amount"],
            outputs=["amount"],
            action=self.convert_amount
        )
        
    def convert_amount(self, data):
        items = data["items"]
        amount = data["amount"]
        num_items = len(items)
        
        # Determine absolute amount
        if isinstance(amount, str) and amount.endswith("%"):
            try:
                percentage = float(amount.rstrip("%"))
                absolute_amount = int(round(num_items * percentage / 100))
            except Exception as e:
                raise ValueError(f"Invalid amount format: {amount}") from e
        else:
            absolute_amount = int(amount)
            
        # Ensure amount is not greater than available items
        absolute_amount = min(absolute_amount, num_items)
        
        return {"amount": absolute_amount}
        
    def __str__(self):
        """Returns a debug-friendly representation of the UnitAmountConverterPipe."""
        return "UnitAmountConverterPipe(Converting relative or absolute amount to absolute number)"