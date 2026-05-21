import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import networkx as nx
from typing import List, Dict, Any, Optional

# --- PREMIUM COLORS ---
TYPE_COLORS = {
    "ACQUISITION": "#f85149", "PARTNERSHIP": "#a371f7", "SUPPLIER": "#3fb950",
    "CUSTOMER": "#58a6ff", "INVESTMENT": "#d29922", "DEFAULT": "#8b949e"
}

class VisualizationService:
    """Institutional Visualization & Dashboard Engine."""
    def __init__(self):
        self.theme = {
            "primary": "#58a6ff", "success": "#3fb950", "warning": "#d29922",
            "danger": "#f85149", "bg": "rgba(0,0,0,0)", "grid": "rgba(255,255,255,0.05)"
        }

    def build_chart(self, data: pd.DataFrame, chart_type: str = "line", title: str = ""):
        cols = data.columns.tolist()
        if len(cols) < 2: return None
        x, y = cols[0], cols[1]
        
        if chart_type == "line": fig = px.line(data, x=x, y=y, title=title, template="plotly_dark", markers=True)
        elif chart_type == "bar": fig = px.bar(data, x=x, y=y, title=title, template="plotly_dark")
        else: fig = px.scatter(data, x=x, y=y, title=title, template="plotly_dark")

        fig.update_layout(
            font_family="Outfit, sans-serif", plot_bgcolor=self.theme["bg"],
            paper_bgcolor=self.theme["bg"], margin=dict(l=10, r=10, t=40, b=10)
        )
        return fig

    def render_ecosystem_graph(self, ticker: str, conns: List[Dict[str, Any]]):
        if not conns: return None
        G = nx.Graph()
        G.add_node(ticker, size=35, color="#f0f6fc", name=ticker)
        for r in conns:
            target = r["target_company"]
            G.add_node(target, size=20, color=TYPE_COLORS.get(r["relationship_type"], TYPE_COLORS["DEFAULT"]), name=target)
            G.add_edge(r["source_ticker"], target, type=r["relationship_type"], detail=r.get("relationship_detail", ""))
        
        try: pos = nx.kamada_kawai_layout(G)
        except: pos = nx.spring_layout(G, k=0.5)

        edge_traces = []
        for u, v, d in G.edges(data=True):
            x0, y0 = pos[u]; x1, y1 = pos[v]
            edge_traces.append(go.Scatter(
                x=[x0, x1, None], y=[y0, y1, None], mode="lines",
                line=dict(width=2, color=TYPE_COLORS.get(d["type"], TYPE_COLORS["DEFAULT"])),
                opacity=0.4, hoverinfo='none'
            ))
        
        node_trace = go.Scatter(
            x=[pos[n][0] for n in G.nodes], y=[pos[n][1] for n in G.nodes],
            mode="markers+text", text=[G.nodes[n].get("name") for n in G.nodes],
            textposition="top center", marker=dict(size=[G.nodes[n].get("size", 20) for n in G.nodes], color=[G.nodes[n].get("color") for n in G.nodes])
        )
        return go.Figure(data=edge_traces + [node_trace], layout=go.Layout(template="plotly_dark", showlegend=False, margin=dict(b=0,l=0,r=0,t=0)))

viz_service = VisualizationService()
render_ecosystem_graph = viz_service.render_ecosystem_graph
