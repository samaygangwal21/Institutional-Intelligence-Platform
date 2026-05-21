import streamlit as st
from typing import Dict, Any, Optional

class TenantService:
    """
    Service Layer for Multi-tenant Institutional Architecture.
    Manages tenant context, isolation, and security boundaries.
    """
    
    @staticmethod
    def get_current_tenant() -> Dict[str, Any]:
        """
        Retrieves the current tenant context from the session state.
        Default to 'Institutional Global' if not set.
        """
        if "tenant" not in st.session_state:
            st.session_state.tenant = {
                "id": "global-1",
                "name": "Institutional Global",
                "tier": "enterprise",
                "features": ["analytics", "research", "vault", "api"]
            }
        return st.session_state.tenant

    @staticmethod
    def set_tenant(tenant_id: str, tenant_name: str, tier: str = "standard"):
        st.session_state.tenant = {
            "id": tenant_id,
            "name": tenant_name,
            "tier": tier
        }

    @staticmethod
    def has_feature(feature: str) -> bool:
        tenant = TenantService.get_current_tenant()
        return feature in tenant.get("features", [])

# Global singleton
tenant_service = TenantService()
