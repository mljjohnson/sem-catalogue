"use client";

import React, { useEffect, useState } from "react";
import { Card, Table, Tag, Button, Select, Spin, Typography, Modal } from "antd";
import type { ColumnsType } from "antd/es/table";
import { ArrowLeftOutlined, ReloadOutlined } from "@ant-design/icons";
import Link from "next/link";
import axios from "axios";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

interface TaskLog {
  id: number;
  task_name: string;
  started_at: string;
  completed_at: string | null;
  status: string;
  stats: any;
  error_message: string | null;
  error_traceback: string | null;
  metadata: any;
  duration_seconds: number;
}

interface TaskLogsResponse {
  logs: TaskLog[];
  total: number;
  limit: number;
  offset: number;
}

export default function TaskLogsPage() {
  const [loading, setLoading] = useState(true);
  const [data, setData] = useState<TaskLogsResponse | null>(null);
  const [taskFilter, setTaskFilter] = useState<string | undefined>(undefined);
  const [statusFilter, setStatusFilter] = useState<string | undefined>(undefined);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [errorModal, setErrorModal] = useState<{ visible: boolean; log: TaskLog | null }>({
    visible: false,
    log: null,
  });

  const fetchLogs = async () => {
    setLoading(true);
    try {
      const params: any = {
        limit: pageSize,
        offset: (page - 1) * pageSize,
      };
      if (taskFilter) params.task_name = taskFilter;
      if (statusFilter) params.status = statusFilter;

      const res = await axios.get<TaskLogsResponse>(`${API_BASE}/task-logs`, { params });
      setData(res.data);
    } catch (error) {
      console.error("Failed to fetch task logs:", error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchLogs();
  }, [page, pageSize, taskFilter, statusFilter]);

  const formatDuration = (seconds: number) => {
    if (seconds < 60) return `${seconds}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
    return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
  };

  const columns: ColumnsType<TaskLog> = [
    {
      title: "Task Name",
      dataIndex: "task_name",
      key: "task_name",
      width: "20%",
      render: (name: string) => <strong>{name}</strong>,
    },
    {
      title: "Status",
      dataIndex: "status",
      key: "status",
      width: "10%",
      render: (status: string) => {
        const colors: Record<string, string> = {
          running: "blue",
          completed: "green",
          failed: "red",
        };
        return <Tag color={colors[status] || "default"}>{status.toUpperCase()}</Tag>;
      },
    },
    {
      title: "Started",
      dataIndex: "started_at",
      key: "started_at",
      width: "15%",
      render: (date: string) => new Date(date).toLocaleString(),
    },
    {
      title: "Duration",
      dataIndex: "duration_seconds",
      key: "duration_seconds",
      width: "10%",
      render: (seconds: number) => formatDuration(seconds),
    },
    {
      title: "Stats",
      dataIndex: "stats",
      key: "stats",
      width: "30%",
      render: (stats: any) => {
        if (!stats) return "—";
        return (
          <div style={{ fontSize: "11px", fontFamily: "monospace" }}>
            {JSON.stringify(stats, null, 2)}
          </div>
        );
      },
    },
    {
      title: "Actions",
      key: "actions",
      width: "15%",
      render: (_, record: TaskLog) => {
        if (record.status === "failed" && record.error_message) {
          return (
            <Button
              size="small"
              danger
              onClick={() => setErrorModal({ visible: true, log: record })}
            >
              View Error
            </Button>
          );
        }
        return "—";
      },
    },
  ];

  return (
    <div style={{ padding: "60px 24px 24px", maxWidth: 1280, margin: "0 auto" }}>
      <div style={{ marginBottom: 24, display: "flex", alignItems: "center", gap: 16 }}>
        <Link href="/">
          <Button icon={<ArrowLeftOutlined />}>Back to Catalogue</Button>
        </Link>
        <h1 style={{ margin: 0 }}>Task Execution Logs</h1>
        <Button icon={<ReloadOutlined />} onClick={fetchLogs} loading={loading}>
          Refresh
        </Button>
      </div>

      <Card style={{ marginBottom: 24 }}>
        <div style={{ display: "flex", gap: 16, alignItems: "center" }}>
          <div>
            <Typography.Text type="secondary">Filter by Task:</Typography.Text>
            <Select
              style={{ width: 250, marginLeft: 8 }}
              placeholder="All tasks"
              allowClear
              value={taskFilter}
              onChange={setTaskFilter}
              options={[
                { label: "Daily Airtable Sync", value: "daily_airtable_sync" },
                { label: "Check Updates", value: "check_updates" },
                { label: "Cataloguing", value: "cataloguing" },
              ]}
            />
          </div>
          <div>
            <Typography.Text type="secondary">Status:</Typography.Text>
            <Select
              style={{ width: 150, marginLeft: 8 }}
              placeholder="All statuses"
              allowClear
              value={statusFilter}
              onChange={setStatusFilter}
              options={[
                { label: "Running", value: "running" },
                { label: "Completed", value: "completed" },
                { label: "Failed", value: "failed" },
              ]}
            />
          </div>
        </div>
      </Card>

      <Card>
        {loading && !data ? (
          <div style={{ textAlign: "center", padding: 40 }}>
            <Spin size="large" />
          </div>
        ) : (
          <Table
            dataSource={data?.logs || []}
            columns={columns}
            rowKey={(r) => r.id}
            pagination={{
              current: page,
              pageSize: pageSize,
              total: data?.total || 0,
              showSizeChanger: true,
              pageSizeOptions: [25, 50, 100],
              onChange: (newPage, newPageSize) => {
                setPage(newPage);
                setPageSize(newPageSize);
              },
            }}
            size="small"
          />
        )}
      </Card>

      <Modal
        title="Task Error Details"
        open={errorModal.visible}
        onCancel={() => setErrorModal({ visible: false, log: null })}
        footer={
          <Button onClick={() => setErrorModal({ visible: false, log: null })}>Close</Button>
        }
        width={800}
      >
        {errorModal.log && (
          <div>
            <div style={{ marginBottom: 16 }}>
              <Typography.Text strong>Task:</Typography.Text> {errorModal.log.task_name}
            </div>
            <div style={{ marginBottom: 16 }}>
              <Typography.Text strong>Error Message:</Typography.Text>
              <div
                style={{
                  background: "#fff1f0",
                  border: "1px solid #ffa39e",
                  padding: 12,
                  borderRadius: 4,
                  marginTop: 8,
                }}
              >
                {errorModal.log.error_message}
              </div>
            </div>
            {errorModal.log.error_traceback && (
              <div>
                <Typography.Text strong>Traceback:</Typography.Text>
                <pre
                  style={{
                    background: "#f5f5f5",
                    padding: 12,
                    borderRadius: 4,
                    fontSize: 11,
                    overflow: "auto",
                    maxHeight: 400,
                    marginTop: 8,
                  }}
                >
                  {errorModal.log.error_traceback}
                </pre>
              </div>
            )}
          </div>
        )}
      </Modal>
    </div>
  );
}

