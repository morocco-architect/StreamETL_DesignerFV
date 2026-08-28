import React, { memo } from 'react';
import { Handle, Position } from '@xyflow/react';
import { Radio, Trash2 } from 'lucide-react';

/**
 * Custom React Flow node representing a Kafka topic.
 * Shows the topic name, partition count, and a live status indicator.
 */
const KafkaTopicNode = memo(({ data, id }) => {
  const isSink = data.role === 'sink';

  return (
    <div className="kafka-node">
      {/* Input handle (left side) — for future pipeline connections */}
      <Handle
        type="target"
        position={Position.Left}
        id="input"
      />

      {/* Header */}
      <div className="kafka-node-header">
        <div className="kafka-node-header-icon">
          <Radio size={14} />
        </div>
        <div className="kafka-node-header-text">
          <div className="kafka-node-type">Kafka Topic</div>
          <div className="kafka-node-name">{data.label}</div>
          <span className={`role-badge ${isSink ? 'consumer' : 'producer'}`}>
            {isSink ? 'Consumer' : 'Producer'}
          </span>
          {!isSink && (
            <span className="role-badge" style={{ marginLeft: '6px', background: 'rgba(255, 255, 255, 0.05)', color: 'var(--text-secondary)', border: '1px solid var(--border-glass)' }}>
              {data.params?.readMode === 'batch' ? 'BATCH' : 'STREAM'}
            </span>
          )}
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
          <span className="kafka-node-stat-label">Partitions</span>
          <span className="kafka-node-stat-value">{data.partitions ?? '—'}</span>
        </div>
        <div className="kafka-node-stat">
          <span className="kafka-node-stat-label">Status</span>
          <div className="kafka-node-status">
            <div className="kafka-node-status-dot" />
            <span className="kafka-node-stat-value" style={{ color: 'var(--accent-success)' }}>
              Active
            </span>
          </div>
        </div>
        {isSink ? (
          data.params?.acksPolicy ? (
            <div className="kafka-node-stat">
              <span className="kafka-node-stat-label">Acks Policy</span>
              <span className="kafka-node-stat-value">{data.params.acksPolicy}</span>
            </div>
          ) : null
        ) : (
          data.params?.consumerGroupId ? (
            <div className="kafka-node-stat">
              <span className="kafka-node-stat-label">Group ID</span>
              <span className="kafka-node-stat-value">{data.params.consumerGroupId}</span>
            </div>
          ) : null
        )}
      </div>

      {/* Output handle (right side) */}
      <Handle
        type="source"
        position={Position.Right}
        id="output"
      />
    </div>
  );
});

KafkaTopicNode.displayName = 'KafkaTopicNode';

export default KafkaTopicNode;
