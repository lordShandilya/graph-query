import React, { useState, useEffect, useRef, useCallback } from 'react';
import CytoscapeComponent from 'react-cytoscapejs';
import cytoscape from 'cytoscape';
import dagre from 'cytoscape-dagre';
import axios from 'axios';
import './App.css';

cytoscape.use(dagre);

const API = process.env.REACT_APP_API_URL || 'http://localhost:8000';

const NODE_CONFIG = {
  Customer:     { color: '#6C63FF', icon: '👤', bg: '#EDE9FF' },
  SalesOrder:   { color: '#F59E0B', icon: '📋', bg: '#FEF3C7' },
  OrderItem:    { color: '#10B981', icon: '📦', bg: '#D1FAE5' },
  Product:      { color: '#3B82F6', icon: '🔧', bg: '#DBEAFE' },
  Delivery:     { color: '#8B5CF6', icon: '🚚', bg: '#EDE9FE' },
  Plant:        { color: '#EC4899', icon: '🏭', bg: '#FCE7F3' },
  Invoice:      { color: '#EF4444', icon: '🧾', bg: '#FEE2E2' },
  Payment:      { color: '#14B8A6', icon: '💳', bg: '#CCFBF1' },
  JournalEntry: { color: '#6B7280', icon: '📒', bg: '#F3F4F6' },
};

const EXAMPLE_QUERIES = [
  "Which products appear in the most billing documents?",
  "Show sales orders that have no outbound delivery",
  "List deliveries that were never billed",
  "Which business partners have the highest total billed amount?",
  "Show all cancelled billing documents",
  "Trace the full flow for a sales order — delivery, billing, and payment",
  "Which plants handle the most deliveries?",
  "Show billing documents with no matching payment",
];

export default function App() {
  const [graphData, setGraphData] = useState({ nodes: [], edges: [] });
  const [stats, setStats] = useState({});
  const [selectedNode, setSelectedNode] = useState(null);
  const [messages, setMessages] = useState([
    { role: 'assistant', content: '👋 Hello! I can answer questions about your orders, deliveries, invoices, and payments. Try one of the example queries below or ask your own!' }
  ]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [activeTab, setActiveTab] = useState('graph');
  const [highlightedNodes, setHighlightedNodes] = useState([]);
  const [showSQL, setShowSQL] = useState({});
  const [uploading, setUploading] = useState(false);
  const [uploadStatus, setUploadStatus] = useState(null);
  const cyRef = useRef(null);
  const chatEndRef = useRef(null);
  const fileInputRef = useRef(null);

  useEffect(() => { loadGraph(); loadStats(); }, []);
  useEffect(() => { chatEndRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [messages]);

  const loadGraph = async () => {
    try {
      const res = await axios.get(`${API}/api/graph?limit=20`);
      setGraphData(res.data);
    } catch (e) { console.error('Graph load failed', e); }
  };

  const loadStats = async () => {
    try {
      const res = await axios.get(`${API}/api/stats`);
      setStats(res.data);
    } catch (e) { console.error('Stats failed', e); }
  };

  const handleUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    setUploading(true);
    setUploadStatus(null);
    const form = new FormData();
    form.append('file', file);
    try {
      const res = await axios.post(`${API}/api/upload-dataset`, form);
      setUploadStatus({ ok: true, msg: res.data.message });
      await loadGraph();
      await loadStats();
    } catch (err) {
      setUploadStatus({ ok: false, msg: err.response?.data?.detail || 'Upload failed' });
    } finally {
      setUploading(false);
    }
  };

  const expandNode = async (nodeId) => {
    try {
      const res = await axios.get(`${API}/api/graph/expand/${nodeId}`);
      const { nodes: newNodes, edges: newEdges } = res.data;
      setGraphData(prev => {
        const existingIds = new Set(prev.nodes.map(n => n.id));
        const existingEdges = new Set(prev.edges.map(e => `${e.source}-${e.target}`));
        return {
          nodes: [...prev.nodes, ...newNodes.filter(n => !existingIds.has(n.id))],
          edges: [...prev.edges, ...newEdges.filter(e => !existingEdges.has(`${e.source}-${e.target}`))]
        };
      });
      setHighlightedNodes(newNodes.map(n => n.id));
      setTimeout(() => setHighlightedNodes([]), 2500);
    } catch (e) { console.error('Expand failed', e); }
  };

  const nodeIds = new Set(graphData.nodes.map(n => n.id));
  const cyElements = [
    ...graphData.nodes.map(n => ({
      data: { id: n.id, label: n.label, type: n.type, properties: n.properties, color: NODE_CONFIG[n.type]?.color || '#999' }
    })),
    ...graphData.edges
      .filter(e => nodeIds.has(e.source) && nodeIds.has(e.target))
      .map((e, i) => ({
        data: { id: `e-${i}`, source: e.source, target: e.target, label: e.label }
      }))
  ];

  const cyStylesheet = [
    {
      selector: 'node',
      style: {
        'background-color': 'data(color)', 'label': 'data(label)',
        'color': '#e2e8f0', 'font-size': '9px', 'font-family': '"IBM Plex Mono", monospace',
        'font-weight': '600', 'text-valign': 'bottom', 'text-halign': 'center',
        'text-margin-y': '5px', 'width': 36, 'height': 36, 'border-width': 2,
        'border-color': 'rgba(255,255,255,0.2)', 'text-wrap': 'wrap', 'text-max-width': '80px',
        'shadow-blur': 8, 'shadow-color': 'data(color)', 'shadow-opacity': 0.5,
        'shadow-offset-x': 0, 'shadow-offset-y': 2,
      }
    },
    { selector: 'node:selected', style: { 'border-width': 3, 'border-color': '#fff', 'width': 46, 'height': 46 } },
    {
      selector: 'edge',
      style: {
        'width': 1.5, 'line-color': '#2d3f5c', 'target-arrow-color': '#2d3f5c',
        'target-arrow-shape': 'triangle', 'curve-style': 'bezier', 'label': 'data(label)',
        'font-size': '7px', 'color': '#4a6080', 'font-family': '"IBM Plex Mono", monospace',
        'text-rotation': 'autorotate', 'text-margin-y': -6, 'opacity': 0.8,
      }
    }
  ];

  const handleCyInit = useCallback((cy) => {
    cyRef.current = cy;
    cy.on('tap', 'node', (evt) => {
      const n = evt.target;
      setSelectedNode({ id: n.id(), type: n.data('type'), properties: n.data('properties') });
    });
    cy.on('dbltap', 'node', (evt) => expandNode(evt.target.id()));
  }, []);

  useEffect(() => {
    if (!cyRef.current) return;
    cyRef.current.nodes().style({ 'opacity': 1 });
    if (highlightedNodes.length > 0) {
      cyRef.current.nodes().style({ 'opacity': 0.25 });
      highlightedNodes.forEach(id => {
        const n = cyRef.current.$(`#${id}`);
        if (n.length) n.style({ 'opacity': 1, 'border-width': 4, 'border-color': '#FFD700', 'shadow-opacity': 1 });
      });
    }
  }, [highlightedNodes]);

  const sendMessage = async (query) => {
    const q = query || inputValue.trim();
    if (!q) return;
    setInputValue('');
    setMessages(prev => [...prev, { role: 'user', content: q }]);
    setIsLoading(true);
    try {
      const res = await axios.post(`${API}/api/chat`, { query: q });
      const { answer, sql, results, is_blocked } = res.data;
      setMessages(prev => [...prev, { role: 'assistant', content: answer, sql, results: results || [], is_blocked }]);
      if (results?.length > 0) {
        const ids = Object.values(results[0]).filter(v => typeof v === 'string' && /^(SO|DEL|INV|PAY|JE|C\d|M\d|P\d)/.test(v));
        if (ids.length) { setHighlightedNodes(ids); setTimeout(() => setHighlightedNodes([]), 3000); }
      }
    } catch (e) {
      setMessages(prev => [...prev, { role: 'assistant', content: '⚠️ Connection error. Is the backend running?' }]);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="app">
      <header className="header">
        <div className="header-left">
          <div className="logo">⬡</div>
          <div>
            <h1>GraphQuery</h1>
            <span className="subtitle">Business Data Explorer</span>
          </div>
        </div>
        <div className="stats-bar">
          {[
            { label: 'Orders', value: stats.total_orders, icon: '📋' },
            { label: 'Deliveries', value: stats.total_deliveries, icon: '🚚' },
            { label: 'Invoices', value: stats.total_invoices, icon: '🧾' },
            { label: 'Revenue', value: stats.total_revenue ? `$${Number(stats.total_revenue).toLocaleString()}` : '...', icon: '💰' },
            { label: 'Broken Flows', value: stats.broken_flows, icon: '⚠️', warn: true },
          ].map(s => (
            <div key={s.label} className={`stat-pill ${s.warn && s.value > 0 ? 'warn' : ''}`}>
              <span>{s.icon}</span>
              <span className="stat-val">{s.value ?? '...'}</span>
              <span className="stat-label">{s.label}</span>
            </div>
          ))}
        </div>
        <div className="upload-area">
          <input ref={fileInputRef} type="file" accept=".xlsx,.xls,.zip,.csv" style={{ display: 'none' }} onChange={handleUpload} />
          <button className="upload-btn" onClick={() => fileInputRef.current?.click()} disabled={uploading}>
            {uploading ? '⏳ Loading...' : '📂 Load Real Dataset'}
          </button>
          {uploadStatus && (
            <span className={`upload-status ${uploadStatus.ok ? 'ok' : 'err'}`}>
              {uploadStatus.ok ? '✓' : '✗'} {uploadStatus.msg}
            </span>
          )}
        </div>
      </header>

      <div className="main">
        <div className="panel graph-panel">
          <div className="panel-header">
            <div className="tabs">
              <button className={activeTab === 'graph' ? 'tab active' : 'tab'} onClick={() => setActiveTab('graph')}>🗺️ Graph</button>
              <button className={activeTab === 'legend' ? 'tab active' : 'tab'} onClick={() => setActiveTab('legend')}>🔑 Legend</button>
            </div>
            <span className="graph-hint">Click = inspect · Double-click = expand</span>
          </div>

          {activeTab === 'graph' && (
            <div className="cy-wrapper">
              <CytoscapeComponent
                elements={cyElements}
                stylesheet={cyStylesheet}
                layout={{ name: 'dagre', rankDir: 'LR', nodeSep: 45, rankSep: 90, padding: 24 }}
                style={{ width: '100%', height: '100%' }}
                cy={handleCyInit}
                minZoom={0.2} maxZoom={3}
              />
            </div>
          )}

          {activeTab === 'legend' && (
            <div className="legend">
              {Object.entries(NODE_CONFIG).map(([type, cfg]) => (
                <div key={type} className="legend-item">
                  <div className="legend-dot" style={{ background: cfg.color }}></div>
                  <span>{cfg.icon}</span>
                  <span className="legend-label">{type}</span>
                </div>
              ))}
              <div className="legend-flow">
                <h4>Business Flow</h4>
                <div className="flow-chain">
                  {['Customer', 'SalesOrder', 'Delivery', 'Invoice', 'Payment', 'JournalEntry'].map((t, i, arr) => (
                    <React.Fragment key={t}>
                      <span className="flow-badge" style={{ background: NODE_CONFIG[t].color }}>
                        {NODE_CONFIG[t].icon} {t}
                      </span>
                      {i < arr.length - 1 && <span className="flow-arrow">→</span>}
                    </React.Fragment>
                  ))}
                </div>
              </div>
            </div>
          )}

          {selectedNode && (
            <div className="node-inspector">
              <div className="inspector-header">
                <span>{NODE_CONFIG[selectedNode.type]?.icon}</span>
                <strong style={{ color: NODE_CONFIG[selectedNode.type]?.color }}>{selectedNode.type}</strong>
                <span className="inspector-id">{selectedNode.id}</span>
                <button className="close-btn" onClick={() => setSelectedNode(null)}>✕</button>
              </div>
              <div className="inspector-props">
                {Object.entries(selectedNode.properties || {}).map(([k, v]) => (
                  <div key={k} className="prop-row">
                    <span className="prop-key">{k}</span>
                    <span className="prop-val">{v}</span>
                  </div>
                ))}
              </div>
              <button className="expand-btn" onClick={() => expandNode(selectedNode.id)}>
                Expand Neighbors →
              </button>
            </div>
          )}
        </div>

        <div className="panel chat-panel">
          <div className="panel-header">
            <span>💬 Natural Language Query</span>
            <span className="powered-by">✨ Gemini</span>
          </div>

          <div className="messages">
            {messages.map((msg, i) => (
              <div key={i} className={`message ${msg.role}`}>
                <div className="message-bubble">
                  {msg.is_blocked && <div className="blocked-badge">🚫 Off-topic blocked</div>}
                  <p>{msg.content}</p>
                  {msg.sql && (
                    <div className="sql-section">
                      <button className="sql-toggle" onClick={() => setShowSQL(s => ({ ...s, [i]: !s[i] }))}>
                        {showSQL[i] ? '▼' : '▶'} Generated SQL
                      </button>
                      {showSQL[i] && <pre className="sql-code">{msg.sql}</pre>}
                    </div>
                  )}
                  {msg.results?.length > 0 && (
                    <div className="results-table-wrap">
                      <div className="results-count">{msg.results.length} rows returned</div>
                      <div className="table-scroll">
                        <table className="results-table">
                          <thead><tr>{Object.keys(msg.results[0]).map(k => <th key={k}>{k}</th>)}</tr></thead>
                          <tbody>
                            {msg.results.slice(0, 10).map((row, ri) => (
                              <tr key={ri}>{Object.values(row).map((v, vi) => <td key={vi}>{typeof v === 'number' ? v.toLocaleString() : String(v ?? '')}</td>)}</tr>
                            ))}
                          </tbody>
                        </table>
                        {msg.results.length > 10 && <div className="more-rows">+ {msg.results.length - 10} more rows</div>}
                      </div>
                    </div>
                  )}
                </div>
              </div>
            ))}
            {isLoading && (
              <div className="message assistant">
                <div className="message-bubble loading"><span/><span/><span/></div>
              </div>
            )}
            <div ref={chatEndRef} />
          </div>

          <div className="examples">
            <div className="examples-label">Try asking:</div>
            <div className="examples-list">
              {EXAMPLE_QUERIES.map((q, i) => (
                <button key={i} className="example-chip" onClick={() => sendMessage(q)}>{q}</button>
              ))}
            </div>
          </div>

          <div className="chat-input-area">
            <input
              className="chat-input"
              value={inputValue}
              onChange={e => setInputValue(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && !e.shiftKey && sendMessage()}
              placeholder="Ask about orders, invoices, deliveries..."
              disabled={isLoading}
            />
            <button className="send-btn" onClick={() => sendMessage()} disabled={isLoading || !inputValue.trim()}>
              {isLoading ? '…' : '→'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
