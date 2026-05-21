from typing import Any, Union

class PrecisionControlEngine:
    """
    Prevents 'fake precision' in LLM reports.
    Standardizes rounding to institutional norms.
    """
    
    @staticmethod
    def format_percentage(val: float, precision: int = 1, use_range: bool = False) -> str:
        """
        Formats a decimal as a percentage with controlled precision.
        Example: 0.25643 -> '~25.6%' or '25-26%'
        """
        if val is None: return "N/A"
        
        percent = val * 100
        
        if use_range:
            low = int(percent)
            high = low + 1
            return f"{low}-{high}%"
            
        rounded = round(percent, precision)
        return f"~{rounded}%"

    @staticmethod
    def format_currency(val: float) -> str:
        """Formats large numbers into institutional shorthand (e.g. $1.2B)."""
        if val is None: return "N/A"
        
        abs_val = abs(val)
        if abs_val >= 1_000_000_000:
            return f"${val / 1_000_000_000:.2f}B"
        if abs_val >= 1_000_000:
            return f"${val / 1_000_000:.1f}M"
        if abs_val >= 1_000:
            return f"${val / 1_000:.1f}K"
            
        return f"${val:,.2f}"

    @staticmethod
    def apply_realism(val: Any) -> Any:
        """
        Top-level hook to strip unsupported precision from any value.
        """
        if isinstance(val, float):
            # If it's a very small decimal, it's likely a ratio/margin
            if 0 < abs(val) < 1:
                return round(val, 3)
            return round(val, 2)
        return val

precision_engine = PrecisionControlEngine()
