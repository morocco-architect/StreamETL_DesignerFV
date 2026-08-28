import React, { memo } from 'react';
import { Handle, Position } from '@xyflow/react';
import { Filter, Braces, Merge, Hash, Trash2, Layers, AlertTriangle } from 'lucide-react';

const icons = {
  Filter: <Filter size={14} />,
  ParseJSON: <Braces size={14} />,
  Join: <Merge size={14} />,
  Aggregate: <Hash size={14} />,
  GroupBy: <Layers size={14} />,
};

const SparkTransformNode = memo(({ data, id }) => {
  const Icon = icons[data.transformType] || <Filter size={14} />;

  // Trigger callback to parent when a parameter changes
  const onChange = (key, value) => {
    if (data.onChange) {
      data.onChange(id, key, value);
    }
  };

  return (
    <div className={`spark-node ${data.hasError ? 'spark-node-error' : ''}`}>
      {/* Input handle from Kafka Topic or previous Transform */}
      <Handle
        type="target"
        position={Position.Left}
        id="input"
      />

      {/* Header */}
      <div className="spark-node-header">
        <div className="spark-node-header-icon">
          {Icon}
        </div>
        <div className="spark-node-header-text">
          <div className="spark-node-type">Spark Transform</div>
          <div className="spark-node-name">{data.transformType}</div>
        </div>
        
        {data.hasError && (
          <div style={{ color: 'var(--accent-warning)', marginRight: 8 }} title={data.errorMessage}>
            <AlertTriangle size={14} />
          </div>
        )}

        <button 
          className="node-delete-btn" 
          onClick={(e) => { e.stopPropagation(); if(data.onDelete) data.onDelete(id); }}
          title="Delete this node"
        >
          <Trash2 size={12} />
        </button>
      </div>

      {/* Configuration Body based on transform type */}
      <div className="spark-node-body" style={{ padding: '8px 12px', fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
        Click to configure
      </div>

      {/* Output handle to next Transform or Kafka Topic Sink */}
      <Handle
        type="source"
        position={Position.Right}
        id="output"
      />
    </div>
  );
});

SparkTransformNode.displayName = 'SparkTransformNode';

export default SparkTransformNode;
