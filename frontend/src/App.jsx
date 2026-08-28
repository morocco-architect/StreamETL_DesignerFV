import React, { useState, useCallback, useEffect, useMemo, useRef } from 'react';
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  addEdge,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';

import KafkaTopicNode from './nodes/KafkaTopicNode.jsx';
import SparkTransformNode from './nodes/SparkTransformNode.jsx';
import ParquetSinkNode from './nodes/ParquetSinkNode.jsx';
import {
  Radio,
  Plus,
  RefreshCw,
  Layers,
  Zap,
  LayoutDashboard,
  Inbox,
  Filter,
  Braces,
  Merge,
  Hash,
  Download,
  FolderDown,
  Play,
  Code,
  X,
  Trash2,
  Save,
  Clock,
  ChevronRight,
  Database,
  CheckCircle,
  AlertCircle,
  Sun,
  Moon,
  Upload,
} from 'lucide-react';

/* ─── Custom node type registry ─── */
const nodeTypes = { 
  kafkaTopic: KafkaTopicNode,
  sparkTransform: SparkTransformNode,
  parquetSink: ParquetSinkNode,
};

/* ─── API helpers ─── */
const API_BASE = '/api';

async function fetchTopics() {
  const res = await fetch(`${API_BASE}/topics`);
  if (!res.ok) throw new Error(`Error ${res.status}`);
  return res.json();
}

async function createTopic(name, partitions = 1) {
  const res = await fetch(`${API_BASE}/topics`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, num_partitions: partitions, replication_factor: 1 }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.detail || `Error ${res.status}`);
  }
  return res.json();
}

/* ─── Components ─── */
const DebouncedInput = ({ value, onChange, delay = 300, ...props }) => {
  const [localValue, setLocalValue] = useState(value || '');

  useEffect(() => {
    setLocalValue(value || '');
  }, [value]);

  useEffect(() => {
    const handler = setTimeout(() => {
      if (localValue !== (value || '')) {
        onChange(localValue);
      }
    }, delay);
    return () => clearTimeout(handler);
  }, [localValue, delay, onChange, value]);

  return (
    <input
      {...props}
      value={localValue}
      onChange={(e) => setLocalValue(e.target.value)}
    />
  );
};

const buildFilterCondition = (filters) => {
  if (!filters || filters.length === 0) return '';
  let result = '';
  let count = 0;
  for (let i = 0; i < filters.length; i++) {
    const f = filters[i];
    if (!f.col || !f.val) continue;
    const currentCond = `${f.col} ${f.op} ${f.val}`;
    if (count === 0) {
      result = currentCond;
    } else {
      if (count >= 2) result = `(${result})`;
      const op = f.logicalOp || 'AND';
      result = `${result} ${op} ${currentCond}`;
    }
    count++;
  }
  return result;
};

/* ─── Main App ─── */
export default function App() {
  const [isDarkMode, setIsDarkMode] = useState(true);
  const [topics, setTopics] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [kafkaConnected, setKafkaConnected] = useState(false);
  const [generatedCode, setGeneratedCode] = useState(null);
  const [executing, setExecuting] = useState(false);
  const [selectedNode, setSelectedNode] = useState(null);
  const [liveData, setLiveData] = useState([]);

  // Save & History state
  const [showSaveDialog, setShowSaveDialog] = useState(false);
  const [saveName, setSaveName] = useState('');
  const [saving, setSaving] = useState(false);
  const [showHistory, setShowHistory] = useState(false);
  const [historyList, setHistoryList] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [toast, setToast] = useState(null);

  // Pipeline Intelligence state
  const [aiPrompt, setAiPrompt] = useState('');
  const [aiBusy, setAiBusy] = useState(false);
  const [intelligenceReport, setIntelligenceReport] = useState(null);

  // React Flow state
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const onConnect = useCallback(
    (params) => setEdges((eds) => addEdge({ ...params, animated: true }, eds)),
    [setEdges],
  );

  const computedNodes = useMemo(() => {
    const outNodes = nodes.map(n => ({
      ...n,
      data: { ...n.data }
    }));
    let changed = true;
    let iterations = 0;
    while (changed && iterations < outNodes.length) {
      changed = false;
      iterations++;
      
      for (const node of outNodes) {
        const incomingEdges = edges.filter(e => e.target === node.id);
        const incomingSchemas = [];
        for (const edge of incomingEdges) {
          const sourceNode = outNodes.find(n => n.id === edge.source);
          if (sourceNode && sourceNode.data.outputSchema) {
            incomingSchemas.push(...sourceNode.data.outputSchema);
          }
        }
        
        const uniqueIncoming = [];
        const seen = new Set();
        for (const f of incomingSchemas) {
          if (!seen.has(f.name)) {
             seen.add(f.name);
             uniqueIncoming.push(f);
          }
        }
        node.data.inputSchema = uniqueIncoming;
        
        let outputSchema = [];
        let hasError = false;
        let errorMessage = null;
        
        if (node.type === 'kafkaTopic') {
          outputSchema = [
            { name: 'key', type: 'BINARY' },
            { name: 'value', type: 'BINARY' },
            { name: 'topic', type: 'STRING' },
            { name: 'partition', type: 'INT' },
            { name: 'offset', type: 'LONG' },
            { name: 'timestamp', type: 'TIMESTAMP' },
            { name: 'timestampType', type: 'INT' }
          ];
        } else if (node.type === 'sparkTransform') {
          const type = node.data.transformType;
          
          if (type === 'Filter') {
            outputSchema = uniqueIncoming;
            const filters = node.data.params?.filters || [];
            const missingCols = filters.filter(f => f.col && !uniqueIncoming.find(inc => inc.name === f.col)).map(f => f.col);
            if (missingCols.length > 0 && uniqueIncoming.length > 0) {
              hasError = true;
              errorMessage = `Columns not found: ${missingCols.join(', ')}`;
            }
          } else if (type === 'ParseJSON') {
            const fields = node.data.params?.schemaFields || [];
            const sourceField = node.data.params?.source_field || 'value';
            
            if (uniqueIncoming.length > 0 && !uniqueIncoming.find(inc => inc.name === sourceField)) {
              hasError = true;
              errorMessage = `Source field "${sourceField}" not found.`;
            }
            
            outputSchema = [
              ...uniqueIncoming.filter(f => f.name !== sourceField),
              ...fields.filter(f => f.name)
            ];
          } else if (type === 'GroupBy') {
            const cols = (node.data.params?.columns || '').split(',').map(s => s.trim()).filter(s => s);
            outputSchema = cols.map(c => {
               const inc = uniqueIncoming.find(f => f.name === c);
               return { name: c, type: inc ? inc.type : 'UNKNOWN' };
            });
            const missingCols = cols.filter(c => !uniqueIncoming.find(inc => inc.name === c));
            if (missingCols.length > 0 && uniqueIncoming.length > 0) {
              hasError = true;
              errorMessage = `Columns not found: ${missingCols.join(', ')}`;
            }
          } else if (type === 'Aggregate') {
            const aggregations = node.data.params?.aggregations || [{ func: 'count', col: '*', alias: '' }];
            const aggOutputCols = aggregations.map(a => {
              const colName = a.alias || `${a.func}_${a.col === '*' ? 'all' : (a.col || 'all')}`;
              return { name: colName, type: 'DOUBLE' };
            });
            outputSchema = [...uniqueIncoming, ...aggOutputCols];
          } else if (type === 'Join') {
             outputSchema = uniqueIncoming;
             const joinKey = node.data.params?.joinKey;
             if (joinKey && uniqueIncoming.length > 0 && !uniqueIncoming.find(inc => inc.name === joinKey)) {
                hasError = true;
                errorMessage = `Join key "${joinKey}" not found.`;
             }
          }
        } else if (node.type === 'parquetSink') {
           node.data.inputSchema = uniqueIncoming;
        }
        
        const oldSchemaStr = JSON.stringify(node.data.outputSchema || []);
        const newSchemaStr = JSON.stringify(outputSchema);
        if (oldSchemaStr !== newSchemaStr) {
           node.data.outputSchema = outputSchema;
           changed = true;
        }
        
        node.data.hasError = hasError;
        node.data.errorMessage = errorMessage;
      }
    }
    return outNodes;
  }, [nodes, edges]);

  // Create topic form
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [newTopicName, setNewTopicName] = useState('');
  const [newTopicPartitions, setNewTopicPartitions] = useState(1);
  const [creating, setCreating] = useState(false);

  /* ── Fetch topics from backend ── */
  const loadTopics = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchTopics();
      setTopics(data);
      setKafkaConnected(true);
    } catch (e) {
      setError(e.message);
      setKafkaConnected(false);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadTopics();
  }, [loadTopics]);

  /* — Initialization & Effects — */
  useEffect(() => {
    if (isDarkMode) {
      document.body.classList.add('dark');
      document.body.classList.remove('light');
    } else {
      document.body.classList.add('light');
      document.body.classList.remove('dark');
    }
  }, [isDarkMode]);

  /* ── Poll live data for selected Kafka Topic ── */
  useEffect(() => {
    let interval;
    if (selectedNode && selectedNode.type === 'kafkaTopic') {
      const fetchPreview = async () => {
        try {
          const res = await fetch(`${API_BASE}/topics/${selectedNode.data.label}/preview`);
          if (res.ok) {
            const data = await res.json();
            setLiveData(data);
          }
        } catch (e) {
          console.error("Preview error:", e);
        }
      };

      fetchPreview();
      interval = setInterval(fetchPreview, 2000); // Poll every 2s
    } else {
      setLiveData([]);
    }
    return () => clearInterval(interval);
  }, [selectedNode]);

  const onDeleteNode = useCallback((id) => {
    setNodes((nds) => nds.filter((n) => n.id !== id));
    setEdges((eds) => eds.filter((e) => e.source !== id && e.target !== id));
    setSelectedNode((sel) => (sel?.id === id ? null : sel));
  }, [setNodes, setEdges]);

  const onNodesChangeWithData = useCallback((changes) => {
    onNodesChange(changes);
  }, [onNodesChange]);

  const onNodeDataChange = useCallback((id, key, value) => {
    setNodes((nds) =>
      nds.map((n) => {
        if (n.id === id) {
          n.data = {
            ...n.data,
            params: { ...n.data.params, [key]: value },
          };
        }
        return n;
      })
    );
  }, [setNodes]);

  const onParquetFieldChange = useCallback((id, key, value) => {
    setNodes((nds) =>
      nds.map((n) => {
        if (n.id === id) {
          n.data = { ...n.data, [key]: value };
        }
        return n;
      })
    );
  }, [setNodes]);

  const onKafkaFieldChange = useCallback((id, key, value) => {
    setNodes((nds) =>
      nds.map((n) => {
        if (n.id === id) {
          n.data = { ...n.data, [key]: value };
        }
        return n;
      })
    );
  }, [setNodes]);

  /* ── Add a topic as a node on the canvas ── */
  const addTopicToCanvas = useCallback(
    (topic) => {
      // Check if already on canvas
      if (nodes.some((n) => n.id === `kafka-${topic.name}`)) return;

      const newNode = {
        id: `kafka-${topic.name}`,
        type: 'kafkaTopic',
        position: {
          x: 100 + nodes.length * 50,
          y: 100 + nodes.length * 80,
        },
        data: {
          label: topic.name,
          partitions: topic.partitions,
          onDelete: onDeleteNode,
          role: 'source',
          params: {},
          onChange: onNodeDataChange,
          onFieldChange: onKafkaFieldChange,
        },
      };
      setNodes((nds) => [...nds, newNode]);
    },
    [nodes, setNodes, onDeleteNode, onNodeDataChange, onKafkaFieldChange],
  );

  /* ── Add a Spark Transform to canvas ── */
  const addTransformToCanvas = useCallback((transformType) => {
    const newNodeId = `spark-${transformType.toLowerCase()}-${Date.now()}`;
    const newNode = {
      id: newNodeId,
      type: 'sparkTransform',
      position: {
        x: 350 + (nodes.length % 3) * 50,
        y: 100 + nodes.length * 40,
      },
      data: {
        transformType,
        params: {},
        onChange: onNodeDataChange, // Pass callback to node
        onDelete: onDeleteNode,
      },
    };
    setNodes((nds) => [...nds, newNode]);
  }, [nodes, setNodes, onNodeDataChange, onDeleteNode]);

  /* ── Add a Parquet Sink to canvas ── */
  const addParquetSinkToCanvas = useCallback(() => {
    const newNodeId = `parquet-${Date.now()}`;
    const newNode = {
      id: newNodeId,
      type: 'parquetSink',
      position: {
        x: 600 + (nodes.length % 3) * 50,
        y: 100 + nodes.length * 40,
      },
      data: {
        id: newNodeId,
        path: '/tmp/parquet_output',
        onChange: onParquetFieldChange,
        onDelete: onDeleteNode,
      },
    };
    setNodes((nds) => [...nds, newNode]);
  }, [nodes, setNodes, onParquetFieldChange, onDeleteNode]);

  /* ── Pipeline Persistence (localStorage) ── */
  const hasLoadedRef = useRef(false);

  const restoreCallbacks = useCallback((savedNodes) =>
    savedNodes.map((n) => ({
      ...n,
      position: n.position || { x: 0, y: 0 },
      data: {
        ...n.data,
        onDelete: onDeleteNode,
        ...(n.type === 'sparkTransform' ? { onChange: onNodeDataChange } : {}),
        ...(n.type === 'parquetSink'    ? { onChange: onParquetFieldChange } : {}),
        ...(n.type === 'kafkaTopic'     ? { onChange: onNodeDataChange, onFieldChange: onKafkaFieldChange } : {}),
      },
    })),
  [onDeleteNode, onNodeDataChange, onParquetFieldChange, onKafkaFieldChange]);

  // Load pipeline: try database first, fallback to localStorage
  useEffect(() => {
    if (hasLoadedRef.current) return;
    hasLoadedRef.current = true;

    const loadPipeline = async () => {
      // 1. Try loading from the database
      try {
        const res = await fetch(`${API_BASE}/pipeline/latest`);
        if (res.ok) {
          const data = await res.json();
          if (Array.isArray(data.nodes) && data.nodes.length > 0) {
            setNodes(restoreCallbacks(data.nodes));
            setEdges(data.edges || []);
            console.log(`✅ Pipeline loaded from DB (id: ${data.id})`);
            return;
          }
        }
      } catch (e) {
        console.warn('⚠️ DB unavailable, falling back to localStorage:', e.message);
      }

      // 2. Fallback: load from localStorage
      try {
        const saved = localStorage.getItem('streametl-pipeline');
        if (!saved) return;

        const { nodes: savedNodes, edges: savedEdges } = JSON.parse(saved);
        if (!Array.isArray(savedNodes) || !Array.isArray(savedEdges)) return;

        setNodes(restoreCallbacks(savedNodes));
        setEdges(savedEdges);
        console.log('✅ Pipeline loaded from localStorage');
      } catch (e) {
        console.error('Failed to load pipeline:', e);
      }
    };

    loadPipeline();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Auto-save pipeline to localStorage on every change
  useEffect(() => {
    if (!hasLoadedRef.current) return; // skip before initial load

    const serializableNodes = nodes.map((n) => ({
      ...n,
      data: Object.fromEntries(
        Object.entries(n.data).filter(([, v]) => typeof v !== 'function')
      ),
    }));

    localStorage.setItem(
      'streametl-pipeline',
      JSON.stringify({ nodes: serializableNodes, edges })
    );
  }, [nodes, edges]);

  /* ── Toast Notification ── */
  const showToast = useCallback((message, type = 'success') => {
    setToast({ message, type, key: Date.now() });
    setTimeout(() => setToast(null), 3000);
  }, []);

  /* ── Pipeline Intelligence helpers ── */
  const getSerializablePipeline = useCallback(() => ({
    nodes: nodes.map(n => ({
      id: n.id,
      type: n.type,
      position: n.position,
      data: Object.fromEntries(
        Object.entries(n.data || {}).filter(([, v]) => typeof v !== 'function')
      ),
    })),
    edges: edges.map((e, index) => ({
      id: e.id || `e-${e.source}-${e.target}-${index}`,
      source: e.source,
      target: e.target,
    })),
  }), [nodes, edges]);

  const layoutSuggestedPipeline = useCallback((suggestedNodes, suggestedEdges) => {
    const nodeIds = suggestedNodes.map(n => n.id);
    const indegree = Object.fromEntries(nodeIds.map(id => [id, 0]));
    const outgoing = Object.fromEntries(nodeIds.map(id => [id, []]));
    suggestedEdges.forEach(e => {
      if (e.source in outgoing && e.target in indegree) {
        outgoing[e.source].push(e.target);
        indegree[e.target] += 1;
      }
    });

    const queue = nodeIds.filter(id => indegree[id] === 0).map(id => [id, 0]);
    const level = {};
    while (queue.length) {
      const [id, depth] = queue.shift();
      level[id] = Math.max(level[id] || 0, depth);
      outgoing[id].forEach(target => {
        indegree[target] -= 1;
        level[target] = Math.max(level[target] || 0, depth + 1);
        if (indegree[target] === 0) queue.push([target, level[target]]);
      });
    }

    const rows = {};
    return suggestedNodes.map((node, index) => {
      const depth = level[node.id] ?? index;
      rows[depth] = (rows[depth] || 0) + 1;
      return {
        ...node,
        position: {
          x: 80 + depth * 250,
          y: 90 + (rows[depth] - 1) * 150,
        },
      };
    });
  }, []);

  const formatApiDetail = useCallback((detail) => {
    if (!detail) return 'Unknown backend error';
    if (typeof detail === 'string') return detail;
    if (Array.isArray(detail)) return detail.map(item => item.msg || JSON.stringify(item)).join('\n');
    if (detail.issues) {
      return detail.issues
        .map(i => `${String(i.severity || 'error').toUpperCase()} ${i.code}: ${i.message}`)
        .join('\n');
    }
    return JSON.stringify(detail, null, 2);
  }, []);

  const analyzeWithIntelligence = useCallback(async () => {
    if (nodes.length === 0) {
      showToast('Add or generate a pipeline before analysis.', 'error');
      return;
    }
    setAiBusy(true);
    try {
      const res = await fetch(`${API_BASE}/intelligence/analyze`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(getSerializablePipeline()),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(formatApiDetail(data.detail));
      setIntelligenceReport(data);
      const { errors = 0, warnings = 0, info = 0 } = data.summary || {};
      showToast(`Analysis: ${errors} error(s), ${warnings} warning(s), ${info} info`, errors ? 'error' : 'success');
    } catch (e) {
      showToast('Intelligence analysis failed: ' + e.message, 'error');
    } finally {
      setAiBusy(false);
    }
  }, [nodes.length, getSerializablePipeline, formatApiDetail, showToast]);

  const buildWithAI = useCallback(async () => {
    if (!aiPrompt.trim()) {
      showToast('Describe the pipeline you want to build.', 'error');
      return;
    }
    setAiBusy(true);
    try {
      const res = await fetch(`${API_BASE}/intelligence/generate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt: aiPrompt.trim() }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(formatApiDetail(data.detail));

      const suggestedNodes = layoutSuggestedPipeline(data.pipeline.nodes || [], data.pipeline.edges || []);
      const suggestedEdges = (data.pipeline.edges || []).map((e, index) => ({
        ...e,
        id: e.id || `ai-e-${e.source}-${e.target}-${index}`,
        animated: true,
      }));
      setNodes(restoreCallbacks(suggestedNodes));
      setEdges(suggestedEdges);
      setGeneratedCode(null);
      setSelectedNode(null);
      setIntelligenceReport(data.validation || null);
      showToast(`AI pipeline created with ${data.provider}${data.model ? ` / ${data.model}` : ''}`, 'success');
    } catch (e) {
      showToast('AI builder unavailable: ' + e.message, 'error');
    } finally {
      setAiBusy(false);
    }
  }, [aiPrompt, formatApiDetail, layoutSuggestedPipeline, restoreCallbacks, setNodes, setEdges, showToast]);

  const applySuggestedRepair = useCallback(async (issue) => {
    if (!issue?.repair_action) return;
    setAiBusy(true);
    try {
      const res = await fetch(`${API_BASE}/intelligence/repair`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          pipeline: getSerializablePipeline(),
          actions: [issue.repair_action],
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(formatApiDetail(data.detail));
      const repairedNodes = layoutSuggestedPipeline(data.pipeline.nodes || [], data.pipeline.edges || []);
      const repairedEdges = (data.pipeline.edges || []).map((e, index) => ({
        ...e,
        id: e.id || `repair-e-${e.source}-${e.target}-${index}`,
        animated: true,
      }));
      setNodes(restoreCallbacks(repairedNodes));
      setEdges(repairedEdges);
      setIntelligenceReport(data.validation || null);
      showToast('Suggested deterministic repair applied.', 'success');
    } catch (e) {
      showToast('Repair failed: ' + e.message, 'error');
    } finally {
      setAiBusy(false);
    }
  }, [getSerializablePipeline, formatApiDetail, layoutSuggestedPipeline, restoreCallbacks, setNodes, setEdges, showToast]);

  const diagnoseSparkError = useCallback(async (logs) => {
    try {
      const res = await fetch(`${API_BASE}/intelligence/diagnose`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          error_message: logs || 'Unknown Spark execution failure',
          pipeline: nodes.length ? getSerializablePipeline() : null,
          use_ai_if_unknown: true,
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(formatApiDetail(data.detail));
      const d = data.diagnosis;
      const actions = (d.recommended_actions || []).map(a => `• ${a}`).join('\n');
      alert(`❌ Spark Execution Error\n\n${d.explanation}${actions ? `\n\nRecommended actions:\n${actions}` : ''}`);
    } catch (e) {
      alert('❌ Spark Execution Error:\n\n' + (logs || e.message));
    }
  }, [nodes.length, getSerializablePipeline, formatApiDetail]);

  /* ── Export Pipeline to JSON ── */
  const exportPipeline = useCallback(() => {
    if (nodes.length === 0) {
      showToast('The canvas is empty — add nodes before exporting.', 'error');
      return;
    }
    const serializableNodes = nodes.map(n => ({
      ...n,
      data: Object.fromEntries(
        Object.entries(n.data).filter(([, v]) => typeof v !== 'function')
      ),
    }));
    const payload = {
      nodes: serializableNodes.map(n => ({ id: n.id, type: n.type, position: n.position, data: n.data })),
      edges: edges.map(e => ({ id: e.id, source: e.source, target: e.target, animated: e.animated })),
    };
    
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `streametl_pipeline_${new Date().getTime()}.json`;
    a.click();
    URL.revokeObjectURL(url);
    showToast('Pipeline exported as JSON', 'success');
  }, [nodes, edges, showToast]);

  /* ── Import Pipeline from JSON ── */
  const fileInputRef = useRef(null);
  
  const handleImportClick = useCallback(() => {
    if (fileInputRef.current) {
      fileInputRef.current.click();
    }
  }, []);

  const handleImportFile = useCallback((event) => {
    const file = event.target.files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const data = JSON.parse(e.target.result);
        if (!data.nodes || !data.edges) throw new Error("Invalid JSON structure");
        
        setShowHistory(false);
        setSelectedNode(null);
        setGeneratedCode(null);
        
        // Restore nodes
        setNodes(restoreCallbacks(data.nodes));
        setEdges(data.edges);
        showToast('Pipeline imported successfully!', 'success');
      } catch (err) {
        showToast('Error importing JSON: ' + err.message, 'error');
      }
      // reset file input
      event.target.value = '';
    };
    reader.readAsText(file);
  }, [restoreCallbacks, setNodes, setEdges, showToast]);

  /* ── Save Pipeline to PostgreSQL ── */
  const savePipeline = useCallback(async () => {
    if (nodes.length === 0) {
      showToast('The canvas is empty — add nodes before saving.', 'error');
      return;
    }
    setSaving(true);
    try {
      const serializableNodes = nodes.map(n => ({
        ...n,
        data: Object.fromEntries(
          Object.entries(n.data).filter(([, v]) => typeof v !== 'function')
        ),
      }));
      
      const now = new Date();
      const defaultName = `Pipeline_${now.toISOString().slice(0,10)}_${now.toTimeString().slice(0,5).replace(':','h')}`;
      
      const res = await fetch(`${API_BASE}/pipeline/save`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: saveName.trim() || defaultName,
          nodes: serializableNodes.map(n => ({ id: n.id, type: n.type, position: n.position, data: n.data })),
          edges: edges.map(e => ({ id: e.id, source: e.source, target: e.target, animated: e.animated })),
        }),
      });
      const data = await res.json();
      if (res.ok) {
        showToast(`Pipeline "${data.name}" saved! (ID: ${data.id})`, 'success');
        setShowSaveDialog(false);
        setSaveName('');
      } else {
        showToast(data.detail || 'Error saving pipeline', 'error');
      }
    } catch (e) {
      showToast('Cannot connect to backend: ' + e.message, 'error');
    } finally {
      setSaving(false);
    }
  }, [nodes, edges, saveName, showToast]);

  /* ── Load History from PostgreSQL ── */
  const loadHistory = useCallback(async () => {
    setHistoryLoading(true);
    try {
      const res = await fetch(`${API_BASE}/pipeline/history`);
      if (res.ok) {
        const data = await res.json();
        setHistoryList(data);
      } else {
        showToast('Error loading history', 'error');
      }
    } catch (e) {
      showToast('Cannot connect to backend: ' + e.message, 'error');
    } finally {
      setHistoryLoading(false);
    }
  }, [showToast]);

  /* ── Restore Pipeline from History ── */
  const restorePipeline = useCallback(async (pipelineId) => {
    try {
      const res = await fetch(`${API_BASE}/pipeline/${pipelineId}`);
      if (!res.ok) {
        showToast('Pipeline not found', 'error');
        return;
      }
      const data = await res.json();
      if (Array.isArray(data.nodes)) {
        setNodes(restoreCallbacks(data.nodes));
        setEdges(data.edges || []);
        setShowHistory(false);
        setSelectedNode(null);
        setGeneratedCode(null);
        showToast(`Pipeline "${data.name}" restored! (${data.nodes.length} nodes)`, 'info');
      }
    } catch (e) {
      showToast('Error: ' + e.message, 'error');
    }
  }, [restoreCallbacks, setNodes, setEdges, showToast]);

  /* ── Toggle History Panel ── */
  const toggleHistory = useCallback(() => {
    if (!showHistory) {
      loadHistory();
    }
    setShowHistory(prev => !prev);
  }, [showHistory, loadHistory]);

  /* ── Spark Error Polling ── */
  useEffect(() => {
    let interval;
    if (executing) {
      interval = setInterval(async () => {
        try {
          const res = await fetch(`${API_BASE}/pipeline/status`);
          if (res.ok) {
            const data = await res.json();
            if (data.crashed) {
              setExecuting(false);
              diagnoseSparkError(data.logs);
            } else if (!data.running) {
              // Might have stopped manually or silently failed
              // We won't force setExecuting(false) here to avoid racing with stop
            }
          }
        } catch (e) {
          console.error("Error polling status:", e);
        }
      }, 3000); // Check every 3 seconds
    }
    return () => clearInterval(interval);
  }, [executing, diagnoseSparkError]);
  /* ── Generate Pipeline Code ── */
  const generatePipeline = useCallback(async () => {
    const pipelineData = {
      nodes: nodes.map(n => ({ id: n.id, type: n.type, data: n.data })),
      edges: edges.map(e => ({ source: e.source, target: e.target }))
    };
    
    try {
      // 1. Generate code
      const res = await fetch(`${API_BASE}/pipeline/generate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(pipelineData)
      });
      const data = await res.json();
      if (res.ok) {
        setGeneratedCode(data.code);
        
        // 2. Stop old job if running
        if (executing) {
          await fetch(`${API_BASE}/pipeline/stop`, { method: 'POST' });
        }
        
        // 3. Auto-execute new pipeline
        setExecuting(true);
        const execRes = await fetch(`${API_BASE}/pipeline/execute`, {
          method: 'POST',
        });
        const execData = await execRes.json();
        if (!execRes.ok) {
          alert("❌ Execution error: " + (execData.detail || "Failed"));
          setExecuting(false);
        }
      } else {
        alert("Error: " + formatApiDetail(data.detail || "Error generating pipeline"));
      }
    } catch (e) {
      alert("Cannot connect to backend: " + e.message);
      setExecuting(false);
    }
  }, [nodes, edges, executing, formatApiDetail]);

  /* ── Execute Pipeline ── */
  const executePipeline = useCallback(async () => {
    setExecuting(true);
    try {
      const res = await fetch(`${API_BASE}/pipeline/execute`, {
        method: 'POST',
      });
      const data = await res.json();
      if (res.ok) {
        // We stay in "executing" state until manually stopped
      } else {
        alert("❌ Error: " + (data.detail || "Execution failed"));
        setExecuting(false);
      }
    } catch (e) {
      alert("Cannot connect to backend: " + e.message);
      setExecuting(false);
    }
  }, []);

  /* ── Stop Pipeline ── */
  const stopPipeline = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/pipeline/stop`, {
        method: 'POST',
      });
      const data = await res.json();
      if (res.ok) {
        setExecuting(false);
      } else {
        alert("❌ Error: " + (data.detail || "Failed to stop pipeline"));
      }
    } catch (e) {
      alert("Cannot connect to backend: " + e.message);
    }
  }, []);

  /* ── Create a new topic ── */
  const handleCreateTopic = useCallback(
    async (e) => {
      e.preventDefault();
      if (!newTopicName.trim()) return;
      setCreating(true);
      try {
        await createTopic(newTopicName.trim(), newTopicPartitions);
        setNewTopicName('');
        setNewTopicPartitions(1);
        setShowCreateForm(false);
        await loadTopics();
      } catch (err) {
        alert(err.message);
      } finally {
        setCreating(false);
      }
    },
    [newTopicName, newTopicPartitions, loadTopics],
  );

  /* ── Delete a topic ── */
  const handleDeleteTopic = useCallback(
    async (topicName) => {
      if (!window.confirm(`Are you sure you want to delete topic "${topicName}"?`)) return;
      try {
        const res = await fetch(`${API_BASE}/topics/${topicName}`, {
          method: 'DELETE',
        });
        if (!res.ok) {
          const data = await res.json().catch(() => ({}));
          throw new Error(data.detail || 'Error deleting topic');
        }
        showToast(`Topic "${topicName}" deleted`, 'success');
        await loadTopics();
      } catch (err) {
        showToast(err.message, 'error');
      }
    },
    [loadTopics, showToast],
  );

  /* ─── Render ─── */
  return (
    <div className={`app-layout ${isDarkMode ? 'dark' : 'light'}`}>
      {/* ======================== SIDEBAR ======================== */}
      <aside className="sidebar">
        {/* Logo & Theme Toggle */}
        <div className="sidebar-header">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div>
              <div className="sidebar-logo">
                <div className="sidebar-logo-icon">
                  <Zap size={16} />
                </div>
                <h1>StreamETL</h1>
              </div>
              <p className="sidebar-subtitle">Pipeline Designer</p>
            </div>
            <button 
              type="button"
              className="theme-toggle-btn"
              onClick={() => setIsDarkMode(prev => !prev)}
              title={isDarkMode ? "Switch to light mode" : "Switch to dark mode"}
            >
              {isDarkMode ? <Sun size={16} /> : <Moon size={16} />}
            </button>
          </div>
        </div>

        {/* Topic list */}
        <div className="sidebar-section">
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              marginBottom: 12,
            }}
          >
            <span className="section-title" style={{ margin: 0 }}>
              <Radio size={12} style={{ marginRight: 6, verticalAlign: -1 }} />
              Kafka Topics
            </span>
            <button
              className="btn btn-ghost"
              style={{ padding: '4px 8px', fontSize: '0.7rem' }}
              onClick={loadTopics}
              title="Refresh"
            >
              <RefreshCw size={12} className={loading ? 'spinner' : ''} />
            </button>
          </div>

          {/* Loading */}
          {loading && (
            <div className="empty-state fade-in">
              <div className="spinner" />
              <p style={{ marginTop: 12 }}>Connecting to Kafka…</p>
            </div>
          )}

          {/* Error */}
          {!loading && error && (
            <div className="empty-state fade-in">
              <div
                className="empty-state-icon"
                style={{ borderColor: 'var(--accent-danger)' }}
              >
                <Zap size={20} />
              </div>
              <p style={{ color: 'var(--accent-danger)' }}>{error}</p>
              <button
                className="btn btn-ghost"
                style={{ marginTop: 12 }}
                onClick={loadTopics}
              >
                Retry
              </button>
            </div>
          )}

          {/* Topics */}
          {!loading && !error && topics.length === 0 && (
            <div className="empty-state fade-in">
              <div className="empty-state-icon">
                <Inbox size={20} />
              </div>
              <p>No topics found.<br />Create your first topic below.</p>
            </div>
          )}

          {!loading &&
            !error &&
            topics.map((topic, i) => (
              <div
                key={topic.name}
                className="topic-card fade-in"
                style={{ animationDelay: `${i * 60}ms` }}
                onClick={() => addTopicToCanvas(topic)}
              >
                <div className="topic-icon">
                  <Radio size={16} />
                </div>
                <div className="topic-info">
                  <div className="topic-name">{topic.name}</div>
                  <div className="topic-meta">
                    {topic.partitions} partition{topic.partitions > 1 ? 's' : ''}
                  </div>
                </div>
                <div style={{ display: 'flex', gap: '4px' }}>
                  <button
                    className="topic-delete-btn"
                    onClick={(e) => {
                      e.stopPropagation();
                      handleDeleteTopic(topic.name);
                    }}
                    title="Delete topic"
                  >
                    <Trash2 size={14} />
                  </button>
                  <button
                    className="topic-add-btn"
                    onClick={(e) => {
                      e.stopPropagation();
                      addTopicToCanvas(topic);
                    }}
                    title="Add to canvas"
                  >
                    <Plus size={14} />
                  </button>
                </div>
              </div>
            ))}
        </div>

        {/* Spark Transforms Section */}
        <div className="sidebar-section" style={{ borderTop: '1px solid var(--border-glass)' }}>
          <span className="section-title">
            <Zap size={12} style={{ marginRight: 6, verticalAlign: -1, color: 'var(--accent-spark)' }} />
            Spark Transforms
          </span>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8, marginTop: 12 }}>
            {[
              { type: 'Filter', icon: <Filter size={14}/> },
              { type: 'ParseJSON', icon: <Braces size={14}/> },
              { type: 'GroupBy', icon: <Layers size={14}/> },
              { type: 'Aggregate', icon: <Hash size={14}/> },
              { type: 'Join', icon: <Merge size={14}/> },
            ].map(t => (
              <button 
                key={t.type}
                className="btn btn-ghost" 
                style={{ flexDirection: 'column', padding: '12px 8px', fontSize: '0.7rem', gap: 6, height: 'auto', color: 'var(--text-primary)' }}
                onClick={() => addTransformToCanvas(t.type)}
                title={`Add ${t.type}`}
              >
                <div style={{color: 'var(--accent-spark)'}}>{t.icon}</div>
                {t.type}
              </button>
            ))}
          </div>
        </div>

        {/* File Destinations Section */}
        <div className="sidebar-section" style={{ borderTop: '1px solid var(--border-glass)' }}>
          <span className="section-title">
            <FolderDown size={12} style={{ marginRight: 6, verticalAlign: -1, color: '#4ade80' }} />
            File Destinations
          </span>
          <div style={{ marginTop: 12 }}>
            <button 
              className="btn btn-ghost btn-full" 
              style={{ justifyContent: 'flex-start', padding: '10px', fontSize: '0.8rem', gap: 10, color: 'var(--text-primary)' }}
              onClick={addParquetSinkToCanvas}
              title="Add Parquet Destination"
            >
              <div style={{color: '#4ade80'}}><FolderDown size={16}/></div>
              Parquet Folder
            </button>
          </div>
        </div>

        {/* Pipeline Intelligence Section */}
        <div className="sidebar-section" style={{ borderTop: '1px solid var(--border-glass)' }}>
          <span className="section-title">
            <Zap size={12} style={{ marginRight: 6, verticalAlign: -1, color: '#a78bfa' }} />
            Pipeline Intelligence
          </span>

          <div style={{ marginTop: 12, display: 'flex', flexDirection: 'column', gap: 8 }}>
            <textarea
              value={aiPrompt}
              onChange={(e) => setAiPrompt(e.target.value)}
              placeholder="Describe a pipeline, e.g. read raw_orders, parse JSON, keep completed orders above 100 and write to filtered_orders."
              rows={4}
              style={{
                width: '100%',
                resize: 'vertical',
                borderRadius: 8,
                border: '1px solid var(--border-glass)',
                background: 'rgba(255,255,255,0.04)',
                color: 'var(--text-primary)',
                padding: 10,
                fontSize: '0.72rem',
                lineHeight: 1.4,
              }}
            />

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
              <button
                className="btn btn-primary"
                style={{ justifyContent: 'center', background: 'linear-gradient(135deg, #8b5cf6, #6366f1)' }}
                onClick={buildWithAI}
                disabled={aiBusy}
                title="Natural language → native StreamETL DAG"
              >
                {aiBusy ? <span className="spinner" style={{width: 13, height: 13, borderWidth: 2}}></span> : <Zap size={13} />}
                Build AI
              </button>
              <button
                className="btn btn-ghost"
                style={{ justifyContent: 'center', background: 'rgba(255,255,255,0.05)' }}
                onClick={analyzeWithIntelligence}
                disabled={aiBusy || nodes.length === 0}
                title="Run deterministic semantic validation"
              >
                <CheckCircle size={13} />
                Analyze
              </button>
            </div>

            {intelligenceReport && (
              <div style={{
                border: '1px solid var(--border-glass)',
                borderRadius: 8,
                padding: 9,
                background: 'rgba(15,23,42,0.28)',
                maxHeight: 230,
                overflowY: 'auto',
              }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, alignItems: 'center', marginBottom: 8 }}>
                  <strong style={{ fontSize: '0.72rem', color: 'var(--text-primary)' }}>
                    Semantic report
                  </strong>
                  <span style={{ fontSize: '0.65rem', color: intelligenceReport.valid ? '#4ade80' : '#f87171' }}>
                    {intelligenceReport.valid ? 'VALID' : 'ATTENTION'}
                  </span>
                </div>

                {(intelligenceReport.issues || []).length === 0 ? (
                  <div style={{ fontSize: '0.68rem', color: 'var(--text-secondary)' }}>
                    No semantic issues detected by the deterministic rule set.
                  </div>
                ) : (
                  (intelligenceReport.issues || []).map((issue, index) => {
                    const issueColor = issue.severity === 'error' ? '#f87171' : issue.severity === 'warning' ? '#fbbf24' : '#60a5fa';
                    return (
                      <div key={`${issue.code}-${issue.node_id || index}`} style={{ marginBottom: 9, paddingBottom: 9, borderBottom: '1px solid rgba(255,255,255,0.06)' }}>
                        <div style={{ fontSize: '0.66rem', fontWeight: 700, color: issueColor }}>
                          {String(issue.severity || 'info').toUpperCase()} · {issue.code}
                        </div>
                        <div style={{ fontSize: '0.66rem', color: 'var(--text-primary)', marginTop: 3, lineHeight: 1.35 }}>
                          {issue.message}
                        </div>
                        {issue.recommendation && (
                          <div style={{ fontSize: '0.63rem', color: 'var(--text-secondary)', marginTop: 3, lineHeight: 1.35 }}>
                            {issue.recommendation}
                          </div>
                        )}
                        {issue.repair_action && (
                          <button
                            className="btn btn-ghost"
                            style={{ marginTop: 6, padding: '5px 8px', height: 'auto', fontSize: '0.62rem' }}
                            onClick={() => applySuggestedRepair(issue)}
                            disabled={aiBusy}
                          >
                            <RefreshCw size={11} />
                            Apply explicit repair
                          </button>
                        )}
                      </div>
                    );
                  })
                )}
              </div>
            )}
          </div>
        </div>

        {/* Global actions */}
        <div className="create-form" style={{ borderTop: '1px solid var(--border-glass)', padding: '16px 20px', display: 'flex', gap: '8px', flexDirection: 'column' }}>
          
          {/* Save Dialog */}
          {showSaveDialog && (
            <div className="save-dialog">
              <div className="input-group">
                <label>Pipeline name</label>
                <input
                  type="text"
                  placeholder={`Pipeline_${new Date().toISOString().slice(0,10)}`}
                  value={saveName}
                  onChange={(e) => setSaveName(e.target.value)}
                  onKeyDown={(e) => { if (e.key === 'Enter') savePipeline(); }}
                  autoFocus
                />
              </div>
              <div className="save-dialog-actions">
                <button
                  className="btn btn-primary"
                  style={{ flex: 1, background: 'linear-gradient(135deg, #6366f1, #8b5cf6)' }}
                  onClick={savePipeline}
                  disabled={saving}
                >
                  {saving ? <span className="spinner" style={{width: 14, height: 14, borderWidth: 2}}></span> : <Database size={14} />}
                  {saving ? 'Saving...' : 'Confirm'}
                </button>
                <button
                  className="btn btn-ghost"
                  onClick={() => { setShowSaveDialog(false); setSaveName(''); }}
                >
                  Cancel
                </button>
              </div>
            </div>
          )}

          {/* Save Button */}
          <button
            className="btn btn-primary btn-full btn-save"
            onClick={() => setShowSaveDialog(prev => !prev)}
          >
            <Save size={14} />
            Save
          </button>

          {/* History Button */}
          <button
            className="btn btn-full btn-history"
            onClick={toggleHistory}
          >
            <Clock size={14} />
            History
          </button>

          {/* Separator */}
          <div style={{ height: 1, background: 'var(--border-glass)', margin: '4px 0' }}></div>

          {/* Export JSON Button */}
          <button
            className="btn btn-ghost btn-full"
            style={{ justifyContent: 'center', background: 'rgba(255,255,255,0.05)' }}
            onClick={exportPipeline}
          >
            <Download size={14} />
            Export JSON
          </button>

          {/* Import JSON Button */}
          <input
            type="file"
            accept=".json"
            ref={fileInputRef}
            style={{ display: 'none' }}
            onChange={handleImportFile}
          />
          <button
            className="btn btn-ghost btn-full"
            style={{ justifyContent: 'center', background: 'rgba(255,255,255,0.05)' }}
            onClick={handleImportClick}
          >
            <Upload size={14} />
            Import JSON
          </button>

          {/* Separator */}
          <div style={{ height: 1, background: 'var(--border-glass)', margin: '4px 0' }}></div>

          <button
            className="btn btn-primary btn-full"
            style={{background: 'linear-gradient(135deg, var(--accent-spark), #4f46e5)', boxShadow: '0 4px 12px var(--accent-spark-glow)'}}
            onClick={generatePipeline}
          >
            <Code size={14} />
            Generate
          </button>
          {!executing ? (
            <button
              className="btn btn-primary btn-full"
              style={{background: 'linear-gradient(135deg, #10b981, #059669)', boxShadow: '0 4px 12px rgba(16, 185, 129, 0.4)'}}
              onClick={executePipeline}
            >
              <Play size={14} />
              Execute
            </button>
          ) : (
            <button
              className="btn btn-primary btn-full"
              style={{background: 'linear-gradient(135deg, #ef4444, #dc2626)', boxShadow: '0 4px 12px rgba(239, 68, 68, 0.4)'}}
              onClick={stopPipeline}
            >
              <span className="spinner" style={{width: 14, height: 14, borderWidth: 2}}></span>
              Stop
            </button>
          )}
        </div>

        {/* Create topic form */}
        <div className="create-form">
          {!showCreateForm ? (
            <button
              className="btn btn-primary btn-full"
              onClick={() => setShowCreateForm(true)}
            >
              <Plus size={16} />
              New Topic
            </button>
          ) : (
            <form onSubmit={handleCreateTopic} className="fade-in">
              <div className="input-group">
                <label htmlFor="topic-name">Topic name</label>
                <input
                  id="topic-name"
                  type="text"
                  placeholder="e.g. raw_orders"
                  value={newTopicName}
                  onChange={(e) => setNewTopicName(e.target.value)}
                  autoFocus
                />
              </div>
              <div className="input-group">
                <label htmlFor="topic-partitions">Partitions</label>
                <input
                  id="topic-partitions"
                  type="number"
                  min="1"
                  max="100"
                  value={newTopicPartitions}
                  onChange={(e) =>
                    setNewTopicPartitions(parseInt(e.target.value, 10) || 1)
                  }
                />
              </div>
              <div style={{ display: 'flex', gap: 8 }}>
                <button
                  type="submit"
                  className="btn btn-primary"
                  style={{ flex: 1 }}
                  disabled={creating}
                >
                  {creating ? <span className="spinner" /> : <Plus size={14} />}
                  Create
                </button>
                <button
                  type="button"
                  className="btn btn-ghost"
                  onClick={() => setShowCreateForm(false)}
                >
                  Cancel
                </button>
              </div>
            </form>
          )}
        </div>
      </aside>

      {/* ======================== CANVAS ======================== */}
      <div className="canvas-area">
        <ReactFlow
          nodes={computedNodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          nodeTypes={nodeTypes}
          onNodeClick={(_, node) => setSelectedNode(node)}
          onPaneClick={() => setSelectedNode(null)}
          fitView
          colorMode={isDarkMode ? 'dark' : 'light'}
          proOptions={{ hideAttribution: true }}
          deleteKeyCode={['Backspace', 'Delete']}
        >
          <Background color="var(--dot-grid)" gap={24} size={1} />
          <Controls />
          <MiniMap
            nodeColor={() => '#ff6b35'}
            maskColor="var(--minimap-mask)"
          />
        </ReactFlow>

        {/* Properties Panel Overlay */}
        {selectedNode && (selectedNode.type === 'sparkTransform' || selectedNode.type === 'kafkaTopic') && (() => {
          const activeNode = computedNodes.find(n => n.id === selectedNode.id) || selectedNode;
          const inputSchema = activeNode.data.inputSchema || [];
          
          return (
          <div className="properties-panel fade-in">
            <div className="properties-panel-header">
              <span>Configuration: {activeNode.data.transformType || 'Kafka Topic'}</span>
              <button className="btn btn-ghost" onClick={() => setSelectedNode(null)}>
                <X size={16} />
              </button>
            </div>
            <div className="properties-panel-content">
              {activeNode.type === 'kafkaTopic' ? (
                <>
                  <div className="input-group">
                    <label>Topic name</label>
                    <input type="text" value={activeNode.data.label} readOnly />
                  </div>

                  <div className="input-group">
                    <label>Role</label>
                    <div className="segmented-toggle">
                      <button
                        type="button"
                        className={`segmented-toggle-btn ${activeNode.data.role !== 'sink' ? 'active producer-active' : ''}`}
                        onClick={() => activeNode.data.onFieldChange && activeNode.data.onFieldChange(activeNode.id, 'role', 'source')}
                      >
                        Producer (source)
                      </button>
                      <button
                        type="button"
                        className={`segmented-toggle-btn ${activeNode.data.role === 'sink' ? 'active consumer-active' : ''}`}
                        onClick={() => activeNode.data.onFieldChange && activeNode.data.onFieldChange(activeNode.id, 'role', 'sink')}
                      >
                        Consumer (sink)
                      </button>
                    </div>
                  </div>

                  {activeNode.data.role !== 'sink' ? (
                    <>
                      <div className="input-group">
                        <label>Read mode</label>
                        <select
                          value={activeNode.data.params?.readMode || 'stream'}
                          onChange={(e) => activeNode.data.onChange && activeNode.data.onChange(activeNode.id, 'readMode', e.target.value)}
                        >
                          <option value="stream">Stream</option>
                          <option value="batch">Batch</option>
                        </select>
                      </div>
                      <div className="input-group">
                        <label>Starting offset</label>
                        <select
                          value={activeNode.data.params?.startingOffset || 'earliest'}
                          onChange={(e) => activeNode.data.onChange && activeNode.data.onChange(activeNode.id, 'startingOffset', e.target.value)}
                          className="form-select"
                        >
                          <option value="earliest">earliest</option>
                          <option value="latest">latest</option>
                        </select>
                      </div>
                      <div className="input-group">
                        <label>Consumer group ID</label>
                        <DebouncedInput
                          type="text"
                          placeholder="e.g. my-group-id"
                          value={activeNode.data.params?.consumerGroupId || ''}
                          onChange={(newVal) => activeNode.data.onChange && activeNode.data.onChange(activeNode.id, 'consumerGroupId', newVal)}
                        />
                        <small className="help-text">Must be unique per pipeline</small>
                      </div>
                    </>
                  ) : (
                    <>
                      <div className="input-group">
                        <label>Acks policy</label>
                        <select
                          value={activeNode.data.params?.acksPolicy || 'all'}
                          onChange={(e) => activeNode.data.onChange && activeNode.data.onChange(activeNode.id, 'acksPolicy', e.target.value)}
                          className="form-select"
                        >
                          <option value="all">all</option>
                          <option value="1">1</option>
                          <option value="0">0</option>
                        </select>
                      </div>
                      <div className="input-group">
                        <label>Compression</label>
                        <select
                          value={activeNode.data.params?.compression || 'none'}
                          onChange={(e) => activeNode.data.onChange && activeNode.data.onChange(activeNode.id, 'compression', e.target.value)}
                          className="form-select"
                        >
                          <option value="none">none</option>
                          <option value="gzip">gzip</option>
                          <option value="snappy">snappy</option>
                          <option value="lz4">lz4</option>
                        </select>
                      </div>
                    </>
                  )}

                  <div className="input-group">
                    <label>Serialization format</label>
                    <div className="segmented-toggle">
                      {['JSON', 'Avro', 'Protobuf', 'String'].map(fmt => (
                        <button
                          key={fmt}
                          type="button"
                          className={`segmented-toggle-btn ${activeNode.data.params?.serializationFormat === fmt || (!activeNode.data.params?.serializationFormat && fmt === 'JSON') ? 'active' : ''}`}
                          onClick={() => activeNode.data.onChange && activeNode.data.onChange(activeNode.id, 'serializationFormat', fmt)}
                        >
                          {fmt}
                        </button>
                      ))}
                    </div>
                  </div>

                  <div className="input-group">
                    <label>Schema registry URL (optional)</label>
                    <DebouncedInput
                      type="text"
                      placeholder="http://localhost:8081"
                      value={activeNode.data.params?.schemaRegistryUrl || ''}
                      onChange={(newVal) => activeNode.data.onChange && activeNode.data.onChange(activeNode.id, 'schemaRegistryUrl', newVal)}
                    />
                  </div>

                  <div className="live-preview-section">
                    <div className="section-title-small">
                      <Radio size={12} className="pulse" /> Live Data Preview
                    </div>
                    <div className="live-data-list">
                      {liveData.length === 0 ? (
                        <p className="empty-preview">Waiting for data…</p>
                      ) : (
                        liveData.map((msg, i) => (
                          <div key={i} className="live-msg-card fade-in">
                            <pre>{JSON.stringify(msg, null, 2)}</pre>
                          </div>
                        ))
                      )}
                    </div>
                  </div>
                </>
              ) : (
                <>
              {activeNode.data.transformType === 'Filter' && (
                <div className="visual-filter-builder">
                  {(activeNode.data.params?.filters || [{ col: '', op: '==', val: '', logicalOp: 'AND' }]).map((filter, index) => (
                    <div key={index} className="filter-row fade-in">
                      {index > 0 && (
                        <div style={{ display: 'flex', justifyContent: 'center', margin: '8px 0' }}>
                          <select 
                            value={filter.logicalOp || 'AND'}
                            onChange={(e) => {
                              const newFilters = [...(activeNode.data.params?.filters || [])];
                              newFilters[index].logicalOp = e.target.value;
                              onNodeDataChange(activeNode.id, 'filters', newFilters);
                              onNodeDataChange(activeNode.id, 'condition', buildFilterCondition(newFilters));
                            }}
                            className="form-select"
                            style={{ width: '80px', textAlign: 'center', padding: '4px', fontSize: '0.7rem' }}
                          >
                            <option value="AND">AND</option>
                            <option value="OR">OR</option>
                          </select>
                        </div>
                      )}
                      <div className="filter-row-header">
                        <span>Condition #{index + 1}</span>
                        {index > 0 && (
                          <button 
                            className="btn-icon-danger"
                            onClick={() => {
                              const newFilters = [...(activeNode.data.params.filters || [])];
                              newFilters.splice(index, 1);
                              onNodeDataChange(activeNode.id, 'filters', newFilters);
                              onNodeDataChange(activeNode.id, 'condition', buildFilterCondition(newFilters));
                            }}
                          >
                            <Trash2 size={12} />
                          </button>
                        )}
                      </div>
                      
                      <div className="input-group">
                        <label>Column</label>
                        {inputSchema.length > 0 ? (
                          <select 
                            value={filter.col}
                            onChange={(e) => {
                              const newFilters = [...(activeNode.data.params?.filters || [{ col: '', op: '==', val: '' }])];
                              newFilters[index].col = e.target.value;
                              onNodeDataChange(activeNode.id, 'filters', newFilters);
                              onNodeDataChange(activeNode.id, 'condition', buildFilterCondition(newFilters));
                            }}
                            className="form-select"
                          >
                            <option value="">Select a column…</option>
                            {inputSchema.map(f => (
                              <option key={f.name} value={f.name}>{f.name} ({f.type})</option>
                            ))}
                          </select>
                        ) : (
                          <DebouncedInput 
                            type="text" 
                            placeholder="e.g. age" 
                            value={filter.col}
                            onChange={(newVal) => {
                              const newFilters = [...(activeNode.data.params?.filters || [{ col: '', op: '==', val: '' }])];
                              newFilters[index].col = newVal;
                              onNodeDataChange(activeNode.id, 'filters', newFilters);
                              onNodeDataChange(activeNode.id, 'condition', buildFilterCondition(newFilters));
                            }}
                          />
                        )}
                      </div>
                      
                      <div className="input-group">
                        <label>Operator</label>
                        <select 
                          value={filter.op}
                          onChange={(e) => {
                            const newFilters = [...(activeNode.data.params?.filters || [{ col: '', op: '==', val: '' }])];
                            newFilters[index].op = e.target.value;
                            onNodeDataChange(activeNode.id, 'filters', newFilters);
                            onNodeDataChange(activeNode.id, 'condition', buildFilterCondition(newFilters));
                          }}
                          className="form-select"
                        >
                          <option value="==">==</option>
                          <option value="!=">!=</option>
                          <option value=">">&gt;</option>
                          <option value="<">&lt;</option>
                          <option value=">=">&gt;=</option>
                          <option value="<=">&lt;=</option>
                          <option value="LIKE">LIKE</option>
                        </select>
                      </div>

                      <div className="input-group">
                        <label>Value</label>
                        <DebouncedInput 
                          type="text" 
                          placeholder="e.g. 18" 
                          value={filter.val}
                          onChange={(newVal) => {
                            const newFilters = [...(activeNode.data.params?.filters || [{ col: '', op: '==', val: '' }])];
                            newFilters[index].val = newVal;
                            onNodeDataChange(activeNode.id, 'filters', newFilters);
                            onNodeDataChange(activeNode.id, 'condition', buildFilterCondition(newFilters));
                          }}
                        />
                      </div>
                    </div>
                  ))}

                  <button 
                    className="btn btn-ghost btn-full" 
                    style={{ marginTop: 8, fontSize: '0.7rem' }}
                    onClick={() => {
                      const newFilters = [...(activeNode.data.params?.filters || [{ col: '', op: '==', val: '' }])];
                      newFilters.push({ col: '', op: '==', val: '', logicalOp: 'AND' });
                      onNodeDataChange(activeNode.id, 'filters', newFilters);
                      onNodeDataChange(activeNode.id, 'condition', buildFilterCondition(newFilters));
                    }}
                  >
                    <Plus size={12} /> Add a condition
                  </button>

                  <div className="condition-preview">
                    <label>Final SQL Condition</label>
                    <code>{activeNode.data.params?.condition || '—'}</code>
                  </div>
                </div>
              )}
              {activeNode.data.transformType === 'ParseJSON' && (
                <div className="parse-json-builder">
                  <div className="section-title-small" style={{marginBottom: 8, fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-secondary)', textTransform: 'uppercase'}}>OUTPUT SCHEMA</div>
                  
                  {(activeNode.data.params?.schemaFields || []).map((field, index) => (
                    <div key={index} className="schema-field-row" style={{ display: 'flex', gap: 8, marginBottom: 8, alignItems: 'center' }}>
                      <DebouncedInput 
                        type="text" 
                        placeholder="Field name" 
                        value={field.name}
                        onChange={(newVal) => {
                          const newFields = [...(activeNode.data.params?.schemaFields || [])];
                          newFields[index].name = newVal;
                          const schemaDDL = newFields.filter(f => f.name).map(f => `${f.name} ${f.type}`).join(', ');
                          onNodeDataChange(activeNode.id, 'schemaFields', newFields);
                          onNodeDataChange(activeNode.id, 'schema', schemaDDL);
                        }}
                        style={{ flex: 1 }}
                      />
                      <select 
                        value={field.type}
                        onChange={(e) => {
                          const newFields = [...(activeNode.data.params?.schemaFields || [])];
                          newFields[index].type = e.target.value;
                          const schemaDDL = newFields.filter(f => f.name).map(f => `${f.name} ${f.type}`).join(', ');
                          onNodeDataChange(activeNode.id, 'schemaFields', newFields);
                          onNodeDataChange(activeNode.id, 'schema', schemaDDL);
                        }}
                        className="form-select"
                        style={{ width: '120px' }}
                      >
                        <option value="STRING">STRING</option>
                        <option value="INT">INT</option>
                        <option value="DOUBLE">DOUBLE</option>
                        <option value="BOOLEAN">BOOLEAN</option>
                        <option value="TIMESTAMP">TIMESTAMP</option>
                      </select>
                      <button 
                        className="btn-icon-danger"
                        style={{ padding: '6px', borderRadius: '4px', border: '1px solid var(--border-glass)', background: 'transparent' }}
                        onClick={() => {
                          const newFields = [...(activeNode.data.params?.schemaFields || [])];
                          newFields.splice(index, 1);
                          const schemaDDL = newFields.filter(f => f.name).map(f => `${f.name} ${f.type}`).join(', ');
                          onNodeDataChange(activeNode.id, 'schemaFields', newFields);
                          onNodeDataChange(activeNode.id, 'schema', schemaDDL);
                        }}
                        title="Delete this field"
                      >
                        <X size={14} />
                      </button>
                    </div>
                  ))}

                  <button 
                    className="btn btn-ghost btn-full" 
                    style={{ marginTop: 4, marginBottom: 16, fontSize: '0.8rem', padding: '8px' }}
                    onClick={() => {
                      const newFields = [...(activeNode.data.params?.schemaFields || [])];
                      newFields.push({ name: '', type: 'STRING' });
                      const schemaDDL = newFields.filter(f => f.name).map(f => `${f.name} ${f.type}`).join(', ');
                      onNodeDataChange(activeNode.id, 'schemaFields', newFields);
                      onNodeDataChange(activeNode.id, 'schema', schemaDDL);
                    }}
                  >
                    <Plus size={12} /> Add a field
                  </button>

                  <div className="section-title-small" style={{marginBottom: 8, fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-secondary)', textTransform: 'uppercase'}}>OPTIONS</div>
                  
                  <div className="checkbox-group" style={{ marginBottom: 12 }}>
                    <label style={{ display: 'flex', alignItems: 'flex-start', gap: 8, cursor: 'pointer' }}>
                      <input 
                        type="checkbox" 
                        checked={activeNode.data.params?.permissive ?? true}
                        onChange={(e) => onNodeDataChange(activeNode.id, 'permissive', e.target.checked)}
                        style={{ marginTop: 2 }}
                      />
                      <div>
                        <div style={{ fontWeight: 500, color: 'var(--text-primary)', marginBottom: 2 }}>Permissive mode</div>
                        <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>Won't fail if a field is missing (null instead)</div>
                      </div>
                    </label>
                  </div>

                  <div className="checkbox-group" style={{ marginBottom: 16 }}>
                    <label style={{ display: 'flex', alignItems: 'flex-start', gap: 8, cursor: 'pointer' }}>
                      <input 
                        type="checkbox" 
                        checked={activeNode.data.params?.flatten ?? false}
                        onChange={(e) => onNodeDataChange(activeNode.id, 'flatten', e.target.checked)}
                        style={{ marginTop: 2 }}
                      />
                      <div>
                        <div style={{ fontWeight: 500, color: 'var(--text-primary)', marginBottom: 2 }}>Flatten nested objects</div>
                        <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>Transforms data.user.id into column data_user_id</div>
                      </div>
                    </label>
                  </div>

                  <div className="input-group">
                    <label>Source field name</label>
                    {inputSchema.length > 0 ? (
                       <select 
                          value={activeNode.data.params?.source_field || 'value'}
                          onChange={(e) => onNodeDataChange(activeNode.id, 'source_field', e.target.value)}
                          className="form-select"
                       >
                          <option value="">-- Select --</option>
                          {inputSchema.map(f => (
                             <option key={f.name} value={f.name}>{f.name} ({f.type})</option>
                          ))}
                       </select>
                    ) : (
                       <DebouncedInput 
                          type="text" 
                          value={activeNode.data.params?.source_field || 'value'}
                          onChange={(newVal) => onNodeDataChange(activeNode.id, 'source_field', newVal)}
                       />
                    )}
                    <small className="help-text">Kafka binary field. Usually value, sometimes key.</small>
                  </div>
                </div>
              )}
              {activeNode.data.transformType === 'GroupBy' && (() => {
                const groupCols = (activeNode.data.params?.columns || '').split(',').map(s => s.trim()).filter(s => s);
                const windowEnabled = activeNode.data.params?.windowEnabled || false;
                
                // Build code preview
                let codePreview = '';
                if (groupCols.length > 0) {
                  const colArgs = groupCols.map(c => /[><=! ]/.test(c) ? `expr("${c}")` : `"${c}"`).join(', ');
                  if (windowEnabled) {
                    const duration = activeNode.data.params?.windowDuration || '';
                    const slide = activeNode.data.params?.windowSlide || '';
                    const tsCol = activeNode.data.params?.timestampCol || '';
                    const watermark = activeNode.data.params?.watermarkDelay || '';
                    let lines = [];
                    if (watermark && tsCol) lines.push(`df = df.withWatermark("${tsCol}", "${watermark}")`);
                    if (duration && tsCol) {
                      const windowExpr = slide ? `window("${tsCol}", "${duration}", "${slide}")` : `window("${tsCol}", "${duration}")`;
                      lines.push(`df_grouped = df.groupBy(${colArgs}, ${windowExpr})`);
                    } else {
                      lines.push(`df_grouped = df.groupBy(${colArgs})`);
                    }
                    codePreview = lines.join('\n');
                  } else {
                    codePreview = `df_grouped = df.groupBy(${colArgs})`;
                  }
                } else {
                  codePreview = '# Add grouping columns';
                }
                
                return (
                <div className="groupby-builder">
                  {/* Header badge */}
                  <div style={{ display: 'flex', alignItems: 'flex-start', gap: 12, marginBottom: 20 }}>
                    <span className="groupby-badge">GroupBy</span>
                    <div>
                      <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.9rem' }}>Group by columns</div>
                      <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)', marginTop: 2 }}>Creates row groups — must be followed by Aggregate</div>
                    </div>
                  </div>

                  {/* Grouping columns */}
                  <div className="input-group">
                    <label style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.8rem', marginBottom: 8 }}>Grouping columns</label>
                    <div className="tag-input-container">
                      {groupCols.map((col, idx) => (
                        <span key={idx} className="tag-chip">
                          {col}
                          <button 
                            className="tag-chip-remove"
                            onClick={() => {
                              const newCols = groupCols.filter((_, i) => i !== idx);
                              onNodeDataChange(activeNode.id, 'columns', newCols.join(', '));
                            }}
                          >×</button>
                        </span>
                      ))}
                      <input 
                        type="text"
                        className="tag-input-field"
                        placeholder="Column + Enter"
                        onKeyDown={(e) => {
                          if (e.key === 'Enter' && e.target.value.trim()) {
                            e.preventDefault();
                            const newCol = e.target.value.trim();
                            if (!groupCols.includes(newCol)) {
                              const newCols = [...groupCols, newCol];
                              onNodeDataChange(activeNode.id, 'columns', newCols.join(', '));
                            }
                            e.target.value = '';
                          }
                        }}
                      />
                    </div>
                    <small className="help-text">Press Enter to add each column. e.g.: country, product_id</small>
                  </div>

                  {/* Available columns from schema */}
                  {inputSchema.length > 0 && (
                    <div style={{ marginBottom: 16, display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                      {inputSchema.filter(f => !groupCols.includes(f.name)).map(f => (
                        <button
                          key={f.name}
                          className="schema-suggest-btn"
                          onClick={() => {
                            const newCols = [...groupCols, f.name];
                            onNodeDataChange(activeNode.id, 'columns', newCols.join(', '));
                          }}
                        >
                          + {f.name}
                        </button>
                      ))}
                    </div>
                  )}

                  {/* TIME WINDOW */}
                  <div className="window-section">
                    <div className="section-title-small" style={{ marginBottom: 12, fontSize: '0.7rem', fontWeight: 600, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>
                      TIME WINDOW (STREAMING)
                    </div>

                    {/* Toggle */}
                    <label className="window-toggle">
                      <div>
                        <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.8rem' }}>Enable window</div>
                        <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)', marginTop: 2 }}>For aggregation on real-time streams (window + watermark)</div>
                      </div>
                      <input 
                        type="checkbox" 
                        checked={windowEnabled}
                        onChange={(e) => onNodeDataChange(activeNode.id, 'windowEnabled', e.target.checked)}
                        className="window-checkbox"
                      />
                    </label>

                    {windowEnabled && (
                      <div className="window-fields fade-in">
                        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
                          <div className="input-group">
                            <label style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.8rem' }}>Window duration</label>
                            <DebouncedInput 
                              type="text" 
                              placeholder="e.g. 10 minutes" 
                              value={activeNode.data.params?.windowDuration || ''}
                              onChange={(newVal) => onNodeDataChange(activeNode.id, 'windowDuration', newVal)}
                            />
                          </div>
                          <div className="input-group">
                            <label style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.8rem' }}>Slide (optional)</label>
                            <DebouncedInput 
                              type="text" 
                              placeholder="e.g. 5 minutes" 
                              value={activeNode.data.params?.windowSlide || ''}
                              onChange={(newVal) => onNodeDataChange(activeNode.id, 'windowSlide', newVal)}
                            />
                          </div>
                        </div>
                        
                        <div className="input-group">
                          <label style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.8rem' }}>Timestamp column</label>
                          {inputSchema.length > 0 ? (
                            <select 
                              value={activeNode.data.params?.timestampCol || ''}
                              onChange={(e) => onNodeDataChange(activeNode.id, 'timestampCol', e.target.value)}
                              className="form-select"
                            >
                              <option value="">-- Select --</option>
                              {inputSchema.map(f => (
                                <option key={f.name} value={f.name}>{f.name} ({f.type})</option>
                              ))}
                            </select>
                          ) : (
                            <DebouncedInput 
                              type="text" 
                              placeholder="e.g. event_time" 
                              value={activeNode.data.params?.timestampCol || ''}
                              onChange={(newVal) => onNodeDataChange(activeNode.id, 'timestampCol', newVal)}
                            />
                          )}
                        </div>

                        <div className="input-group">
                          <label style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.8rem' }}>Watermark delay</label>
                          <DebouncedInput 
                            type="text" 
                            placeholder="e.g. 2 minutes" 
                            value={activeNode.data.params?.watermarkDelay || ''}
                            onChange={(newVal) => onNodeDataChange(activeNode.id, 'watermarkDelay', newVal)}
                          />
                        </div>
                      </div>
                    )}
                  </div>

                  {/* Code Preview */}
                  <div className="groupby-code-preview">
                    <div className="section-title-small" style={{ marginBottom: 8, fontSize: '0.7rem', fontWeight: 600, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>
                      PREVIEW — GENERATED CODE
                    </div>
                    <pre className="code-preview-block"><code>{codePreview}</code></pre>
                  </div>
                </div>
                );
              })()}
              {activeNode.data.transformType === 'Join' && (() => {
                const joinKey = activeNode.data.params?.joinKey || '';
                const rightKey = activeNode.data.params?.rightKey || '';
                const rightDf = activeNode.data.params?.rightDf || '';
                const joinType = activeNode.data.params?.joinType || 'inner';
                const broadcast = activeNode.data.params?.broadcast || false;
                const joinMode = activeNode.data.params?.joinMode || 'simple';

                // Build code preview
                const leftDfName = 'df_current';
                const rightDfName = rightDf || 'other_df';
                let codePreview = '';
                if (joinKey) {
                  const rk = rightKey || joinKey;
                  const condExpr = joinKey === rk
                    ? `"${joinKey}"`
                    : `${leftDfName}["${joinKey}"] == ${rightDfName}["${rk}"]`;
                  const rightRef = broadcast ? `broadcast(${rightDfName})` : rightDfName;
                  codePreview = `df_joined = ${leftDfName}.join(${rightRef}, ${condExpr}, "${joinType}")`;
                } else {
                  codePreview = '# Configure join keys';
                }

                return (
                <div className="join-builder">
                  {/* Header badge */}
                  <div style={{ display: 'flex', alignItems: 'flex-start', gap: 12, marginBottom: 20 }}>
                    <span className="join-badge">Join</span>
                    <div>
                      <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.9rem' }}>Join two sources</div>
                      <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)', marginTop: 2 }}>Combines two DataFrames on a common key</div>
                    </div>
                  </div>

                  {/* DataFrames */}
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 16 }}>
                    <div className="input-group">
                      <label style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.8rem' }}>Left DataFrame (current)</label>
                      <input type="text" value="df_current" readOnly className="join-df-readonly" />
                      <small className="help-text">The incoming stream into this node</small>
                    </div>
                    <div className="input-group">
                      <label style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.8rem' }}>Right DataFrame (source 2)</label>
                      <DebouncedInput
                        type="text"
                        placeholder="e.g. df_ref_products"
                        value={rightDf}
                        onChange={(newVal) => onNodeDataChange(activeNode.id, 'rightDf', newVal)}
                      />
                    </div>
                  </div>

                  {/* Join type */}
                  <div className="input-group" style={{ marginBottom: 16 }}>
                    <label style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.8rem' }}>Join type</label>
                    <select
                      value={joinType}
                      onChange={(e) => onNodeDataChange(activeNode.id, 'joinType', e.target.value)}
                      className="form-select"
                    >
                      <option value="inner">inner — matching rows from both sides</option>
                      <option value="left">left — all rows from left side</option>
                      <option value="right">right — all rows from right side</option>
                      <option value="full">full — all rows from both sides</option>
                      <option value="semi">semi — left rows with a match</option>
                      <option value="anti">anti — left rows without a match</option>
                      <option value="cross">cross — cartesian product</option>
                    </select>
                  </div>

                  {/* JOIN CONDITION */}
                  <div className="window-section" style={{ marginTop: 0 }}>
                    <div className="section-title-small" style={{ marginBottom: 10, fontSize: '0.7rem', fontWeight: 600, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>
                      JOIN CONDITION
                    </div>

                    {/* Mode tabs */}
                    <div className="join-mode-tabs">
                      {[
                        { key: 'simple', label: 'Simple key' },
                        { key: 'multiple', label: 'Multiple keys' },
                        { key: 'expression', label: 'Expression' },
                      ].map(m => (
                        <button
                          key={m.key}
                          className={`join-mode-tab ${joinMode === m.key ? 'active' : ''}`}
                          onClick={() => onNodeDataChange(activeNode.id, 'joinMode', m.key)}
                        >
                          {m.label}
                        </button>
                      ))}
                    </div>

                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginTop: 12 }}>
                      <div className="input-group">
                        <label style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.8rem' }}>Left key</label>
                        <DebouncedInput
                          type="text"
                          placeholder="e.g. user_id"
                          value={joinKey}
                          onChange={(newVal) => onNodeDataChange(activeNode.id, 'joinKey', newVal)}
                        />
                      </div>
                      <div className="input-group">
                        <label style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.8rem' }}>Right key (if different)</label>
                        <DebouncedInput
                          type="text"
                          placeholder="e.g. id (leave empty if same)"
                          value={rightKey}
                          onChange={(newVal) => onNodeDataChange(activeNode.id, 'rightKey', newVal)}
                        />
                      </div>
                    </div>
                  </div>

                  {/* Broadcast hint */}
                  <div className="agg-options-section" style={{ marginTop: 16 }}>
                    <label className="agg-option-row" style={{ borderBottom: 'none' }}>
                      <div>
                        <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.8rem' }}>Broadcast hint (small right table)</div>
                        <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)', marginTop: 2 }}>Optimizes if the right table fits in memory ({'< ~100MB'})</div>
                      </div>
                      <input
                        type="checkbox"
                        checked={broadcast}
                        onChange={(e) => onNodeDataChange(activeNode.id, 'broadcast', e.target.checked)}
                        className="window-checkbox"
                      />
                    </label>
                  </div>

                  {/* WATERMARKS */}
                  <div className="window-section" style={{ marginTop: 16 }}>
                    <div className="section-title-small" style={{ marginBottom: 12, fontSize: '0.7rem', fontWeight: 600, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>
                      WATERMARKS (STREAM-STREAM JOIN)
                    </div>
                    
                    <label className="window-toggle">
                      <div>
                        <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.8rem' }}>Enable watermarks</div>
                        <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)', marginTop: 2 }}>Required to bound state for Stream-Stream joins</div>
                      </div>
                      <input 
                        type="checkbox" 
                        checked={activeNode.data.params?.enableWatermarks || false}
                        onChange={(e) => onNodeDataChange(activeNode.id, 'enableWatermarks', e.target.checked)}
                        className="window-checkbox"
                      />
                    </label>

                    {activeNode.data.params?.enableWatermarks && (
                      <div className="window-fields fade-in" style={{ marginTop: 12 }}>
                        <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.8rem', marginBottom: 8 }}>Left DataFrame</div>
                        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 16 }}>
                          <div className="input-group" style={{ marginBottom: 0 }}>
                            <label>Timestamp column</label>
                            <DebouncedInput
                              type="text"
                              placeholder="e.g. timestamp"
                              value={activeNode.data.params?.leftWatermarkCol || ''}
                              onChange={(newVal) => onNodeDataChange(activeNode.id, 'leftWatermarkCol', newVal)}
                            />
                          </div>
                          <div className="input-group" style={{ marginBottom: 0 }}>
                            <label>Max delay</label>
                            <DebouncedInput
                              type="text"
                              placeholder="e.g. 10 minutes"
                              value={activeNode.data.params?.leftWatermarkDelay || ''}
                              onChange={(newVal) => onNodeDataChange(activeNode.id, 'leftWatermarkDelay', newVal)}
                            />
                          </div>
                        </div>

                        <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.8rem', marginBottom: 8 }}>Right DataFrame</div>
                        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
                          <div className="input-group" style={{ marginBottom: 0 }}>
                            <label>Timestamp column</label>
                            <DebouncedInput
                              type="text"
                              placeholder="e.g. timestamp"
                              value={activeNode.data.params?.rightWatermarkCol || ''}
                              onChange={(newVal) => onNodeDataChange(activeNode.id, 'rightWatermarkCol', newVal)}
                            />
                          </div>
                          <div className="input-group" style={{ marginBottom: 0 }}>
                            <label>Max delay</label>
                            <DebouncedInput
                              type="text"
                              placeholder="e.g. 10 minutes"
                              value={activeNode.data.params?.rightWatermarkDelay || ''}
                              onChange={(newVal) => onNodeDataChange(activeNode.id, 'rightWatermarkDelay', newVal)}
                            />
                          </div>
                        </div>

                        <div style={{ marginTop: 16, paddingTop: 14, borderTop: '1px solid var(--border-color)' }}>
                          <label className="window-toggle">
                            <div>
                              <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.8rem' }}>Bound event-time range</div>
                              <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)', marginTop: 2 }}>Required for safe stream-stream state cleanup</div>
                            </div>
                            <input
                              type="checkbox"
                              checked={activeNode.data.params?.timeBoundEnabled || false}
                              onChange={(e) => onNodeDataChange(activeNode.id, 'timeBoundEnabled', e.target.checked)}
                              className="window-checkbox"
                            />
                          </label>

                          {activeNode.data.params?.timeBoundEnabled && (
                            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginTop: 12 }}>
                              <div className="input-group" style={{ marginBottom: 0 }}>
                                <label>Allowed before right event time</label>
                                <DebouncedInput
                                  type="text"
                                  placeholder="e.g. 10 minutes"
                                  value={activeNode.data.params?.timeRangeBefore || ''}
                                  onChange={(newVal) => onNodeDataChange(activeNode.id, 'timeRangeBefore', newVal)}
                                />
                              </div>
                              <div className="input-group" style={{ marginBottom: 0 }}>
                                <label>Allowed after right event time</label>
                                <DebouncedInput
                                  type="text"
                                  placeholder="e.g. 5 minutes"
                                  value={activeNode.data.params?.timeRangeAfter || ''}
                                  onChange={(newVal) => onNodeDataChange(activeNode.id, 'timeRangeAfter', newVal)}
                                />
                              </div>
                            </div>
                          )}
                        </div>
                      </div>
                    )}
                  </div>

                  {/* Code Preview */}
                  <div className="groupby-code-preview">
                    <div className="section-title-small" style={{ marginBottom: 8, fontSize: '0.7rem', fontWeight: 600, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>
                      PREVIEW — GENERATED CODE
                    </div>
                    <pre className="code-preview-block"><code>{codePreview}</code></pre>
                  </div>
                </div>
                );
              })()}
              {activeNode.data.transformType === 'Aggregate' && (() => {
                const aggregations = activeNode.data.params?.aggregations || [{ func: 'count', col: '*', alias: '' }];
                const includeNulls = activeNode.data.params?.include_nulls || false;
                const dedupBefore = activeNode.data.params?.dedup_before || false;

                // Build code preview
                let codeLines = [];
                if (dedupBefore) codeLines.push('df = df.dropDuplicates()');
                if (aggregations.length > 0) {
                  const aggExprs = aggregations.map(a => {
                    const colArg = (a.col === '*' || !a.col) ? '"*"' : `"${a.col}"`;
                    const funcCall = `${a.func}(${colArg})`;
                    return a.alias ? `${funcCall}.alias("${a.alias}")` : funcCall;
                  });
                  if (aggExprs.length === 1) {
                    codeLines.push(`df_agg = df_grouped.agg(\n    ${aggExprs[0]}\n)`);
                  } else {
                    codeLines.push(`df_agg = df_grouped.agg(\n    ${aggExprs.join(',\n    ')}\n)`);
                  }
                }
                const codePreview = codeLines.join('\n');

                return (
                <div className="aggregate-builder">
                  {/* Header badge */}
                  <div style={{ display: 'flex', alignItems: 'flex-start', gap: 12, marginBottom: 20 }}>
                    <span className="aggregate-badge">Aggregate</span>
                    <div>
                      <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.9rem' }}>Aggregate values</div>
                      <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)', marginTop: 2 }}>Applies aggregation functions on groups or the entire dataset</div>
                    </div>
                  </div>

                  {/* AGGREGATION FUNCTIONS */}
                  <div className="section-title-small" style={{ marginBottom: 10, fontSize: '0.7rem', fontWeight: 600, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>
                    AGGREGATION FUNCTIONS
                  </div>

                  {aggregations.map((agg, index) => (
                    <div key={index} className="agg-row fade-in">
                      <div className="agg-row-fields">
                        <select
                          value={agg.func || 'count'}
                          onChange={(e) => {
                            const newAggs = [...aggregations];
                            newAggs[index] = { ...newAggs[index], func: e.target.value };
                            onNodeDataChange(activeNode.id, 'aggregations', newAggs);
                          }}
                          className="form-select agg-func-select"
                        >
                          <option value="count">count</option>
                          <option value="sum">sum</option>
                          <option value="avg">avg</option>
                          <option value="min">min</option>
                          <option value="max">max</option>
                          <option value="collect_list">collect_list</option>
                          <option value="collect_set">collect_set</option>
                          <option value="first">first</option>
                          <option value="last">last</option>
                          <option value="stddev">stddev</option>
                          <option value="variance">variance</option>
                        </select>

                        <DebouncedInput
                          type="text"
                          placeholder="*"
                          value={agg.col || ''}
                          className="agg-col-input"
                          onChange={(newVal) => {
                            const newAggs = [...aggregations];
                            newAggs[index] = { ...newAggs[index], col: newVal };
                            onNodeDataChange(activeNode.id, 'aggregations', newAggs);
                          }}
                        />

                        <DebouncedInput
                          type="text"
                          placeholder="Alias"
                          value={agg.alias || ''}
                          className="agg-alias-input"
                          onChange={(newVal) => {
                            const newAggs = [...aggregations];
                            newAggs[index] = { ...newAggs[index], alias: newVal };
                            onNodeDataChange(activeNode.id, 'aggregations', newAggs);
                          }}
                        />
                      </div>
                      <button
                        className="agg-remove-btn"
                        onClick={() => {
                          const newAggs = aggregations.filter((_, i) => i !== index);
                          onNodeDataChange(activeNode.id, 'aggregations', newAggs.length > 0 ? newAggs : [{ func: 'count', col: '*', alias: '' }]);
                        }}
                        title="Remove"
                      >
                        <X size={14} />
                      </button>
                    </div>
                  ))}

                  <button
                    className="btn btn-ghost btn-full agg-add-btn"
                    onClick={() => {
                      const newAggs = [...aggregations, { func: 'count', col: '*', alias: '' }];
                      onNodeDataChange(activeNode.id, 'aggregations', newAggs);
                    }}
                  >
                    + Add an aggregation
                  </button>

                  {/* ADVANCED OPTIONS */}
                  <div className="agg-options-section">
                    <div className="section-title-small" style={{ marginBottom: 12, fontSize: '0.7rem', fontWeight: 600, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>
                      ADVANCED OPTIONS
                    </div>

                    <label className="agg-option-row">
                      <div>
                        <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.8rem' }}>Include NULL values</div>
                        <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)', marginTop: 2 }}>count(*) instead of count(col) — counts nulls</div>
                      </div>
                      <input
                        type="checkbox"
                        checked={includeNulls}
                        onChange={(e) => onNodeDataChange(activeNode.id, 'include_nulls', e.target.checked)}
                        className="window-checkbox"
                      />
                    </label>

                    <label className="agg-option-row">
                      <div>
                        <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.8rem' }}>Deduplicate before</div>
                        <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)', marginTop: 2 }}>Applies .dropDuplicates() before aggregation</div>
                      </div>
                      <input
                        type="checkbox"
                        checked={dedupBefore}
                        onChange={(e) => onNodeDataChange(activeNode.id, 'dedup_before', e.target.checked)}
                        className="window-checkbox"
                      />
                    </label>
                  </div>

                  {/* Code Preview */}
                  <div className="groupby-code-preview">
                    <div className="section-title-small" style={{ marginBottom: 8, fontSize: '0.7rem', fontWeight: 600, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>
                      PREVIEW — GENERATED CODE
                    </div>
                    <pre className="code-preview-block"><code>{codePreview}</code></pre>
                  </div>
                </div>
                );
              })()}
            </>
          )}
            </div>
          </div>
          );
        })()}

        {/* Code Generation Panel Overlay */}
        {generatedCode && (
          <div className="code-panel-overlay fade-in">
            <div className="code-panel-header">
              <span><Code size={14} /> Generated PySpark Code</span>
              <button className="btn btn-ghost" onClick={() => setGeneratedCode(null)}>
                <X size={16} />
              </button>
            </div>
            <div className="code-panel-content">
              <pre>
                <code>{generatedCode}</code>
              </pre>
            </div>
          </div>
        )}

        {/* History Panel Overlay */}
        {showHistory && (
          <div className="history-panel">
            <div className="history-panel-header">
              <span><Clock size={14} /> Pipeline History</span>
              <button className="btn btn-ghost" onClick={() => setShowHistory(false)}>
                <X size={16} />
              </button>
            </div>
            <div className="history-panel-content">
              {historyLoading ? (
                <div className="history-loading">
                  <div className="spinner" />
                  <p style={{ color: 'var(--text-secondary)', fontSize: '0.8rem' }}>Loading…</p>
                </div>
              ) : historyList.length === 0 ? (
                <div className="history-empty">
                  <div className="history-empty-icon">
                    <Database size={22} />
                  </div>
                  <p>No pipelines saved yet.<br/>Use the "Save" button to save your first pipeline.</p>
                </div>
              ) : (
                historyList.map((pipeline, i) => (
                  <div
                    key={pipeline.id}
                    className="history-item fade-in"
                    style={{ animationDelay: `${i * 50}ms` }}
                    onClick={() => restorePipeline(pipeline.id)}
                  >
                    <div className="history-item-icon">
                      #{pipeline.id}
                    </div>
                    <div className="history-item-info">
                      <div className="history-item-name">{pipeline.name}</div>
                      <div className="history-item-meta">
                        <span><Layers size={10} /> {pipeline.node_count} node{pipeline.node_count !== 1 ? 's' : ''}</span>
                        <span>{pipeline.created_at ? new Date(pipeline.created_at).toLocaleString('en-US', { day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' }) : '—'}</span>
                      </div>
                    </div>
                    <div className="history-item-arrow">
                      <ChevronRight size={16} />
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>
        )}

        {/* Status bar */}
        <div className="status-bar">
          <div className="status-item">
            <div
              className={`status-dot ${kafkaConnected ? 'connected' : 'disconnected'}`}
            />
            Kafka {kafkaConnected ? 'connected' : 'disconnected'}
          </div>
          <div className="status-item">
            <Layers size={12} />
            {nodes.length} node{nodes.length !== 1 ? 's' : ''}
          </div>
          <div className="status-item">
            <LayoutDashboard size={12} />
            {topics.length} topic{topics.length !== 1 ? 's' : ''}
          </div>
        </div>
      </div>

      {/* Toast Notification */}
      {toast && (
        <div key={toast.key} className={`toast-notification ${toast.type}`}>
          {toast.type === 'success' && <CheckCircle size={16} />}
          {toast.type === 'error' && <AlertCircle size={16} />}
          {toast.type === 'info' && <Database size={16} />}
          {toast.message}
        </div>
      )}
    </div>
  );
}
