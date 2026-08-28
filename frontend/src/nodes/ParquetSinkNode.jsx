import React, { memo } from 'react';
import { Handle, Position } from '@xyflow/react';
import { FolderDown, Trash2 } from 'lucide-react';

const ParquetSinkNode = memo(({ data, id }) => {
  return (
    <div className="kafka-node">
      {/* Input handle (left side) — for pipeline connections */}
      <Handle
        type="target"
        position={Position.Left}
        id="input"
      />

      {/* Header */}
      <div className="kafka-node-header" style={{ background: 'linear-gradient(90deg, rgba(74, 222, 128, 0.1), transparent)' }}>
        <div className="kafka-node-header-icon" style={{ borderColor: 'rgba(74, 222, 128, 0.3)', color: '#4ade80' }}>
          <FolderDown size={14} />
        </div>
        <div className="kafka-node-header-text">
          <div className="kafka-node-type">Parquet Sink</div>
          <div className="kafka-node-name">{data.path || '/tmp/parquet_output'}</div>
        </div>
        <button 
          className="node-delete-btn" 
          onClick={(e) => { e.stopPropagation(); if(data.onDelete) data.onDelete(id); }}
          title="Delete this node"
        >
          <Trash2 size={12} />
        </button>
      </div>

      {/* Body */}
      <div className="kafka-node-body">
        <div className="kafka-node-stat">
          <span className="kafka-node-stat-label">Format</span>
          <span className="kafka-node-stat-value">Parquet</span>
        </div>
        <div className="kafka-node-stat" style={{ flexDirection: 'column', alignItems: 'flex-start' }}>
          <span className="kafka-node-stat-label">Path</span>
          <input 
            type="text" 
            value={data.path || '/tmp/parquet_output'}
            onChange={(e) => data.onChange && data.onChange(data.id, 'path', e.target.value)}
            className="transform-param-input"
            style={{ marginTop: 4, width: '100%' }}
            placeholder="/tmp/parquet_output"
          />
        </div>
      </div>
    </div>
  );
});

ParquetSinkNode.displayName = 'ParquetSinkNode';

export default ParquetSinkNode;
