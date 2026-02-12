import { useEffect, useMemo, useState } from "react";
import {
  Button,
  Card,
  Checkbox,
  Col,
  Drawer,
  Form,
  Input,
  Layout,
  Modal,
  Row,
  Select,
  Space,
  Table,
  Tag,
  Typography,
  message,
} from "antd";
import { api } from "./api";

const { Header, Content } = Layout;
const { Title, Text } = Typography;

type Server = {
  id: string;
  name: string;
  ip: string;
  agent_url: string;
  status: string;
  created_at: string;
};

type Report = {
  id: string;
  server_id: string;
  created_at: string;
  payload: any;
};

type ReportAction = {
  id: string;
  report_id: string;
  type: string;
  target: string;
  risk: string;
  reason: string;
  raw: any;
};

const BLOCK_OPTIONS = [
  { label: "System", value: "system" },
  { label: "Buffers/Bgwriter", value: "buffers_bgwriter" },
  { label: "WAL/Replication", value: "wal_replication" },
  { label: "Temp files", value: "temp_files" },
  { label: "Checkpoints", value: "checkpoints_bgwriter" },
  { label: "Sizes", value: "sizes" },
  { label: "Activity", value: "connections_activity" },
  { label: "Indexes/Tables/Statements", value: "indexes_tables_statements" },
];

function statusTag(status: string) {
  const s = (status || "unknown").toLowerCase();
  if (s === "ok") return <Tag color="green">ok</Tag>;
  if (s === "down") return <Tag color="red">down</Tag>;
  return <Tag>unknown</Tag>;
}

function riskTag(risk: string) {
  const r = (risk || "").toLowerCase();
  if (r === "high") return <Tag color="red">high</Tag>;
  if (r === "medium") return <Tag color="orange">medium</Tag>;
  return <Tag color="green">low</Tag>;
}

export default function App() {
  const [servers, setServers] = useState<Server[]>([]);
  const [loading, setLoading] = useState(false);

  const [addOpen, setAddOpen] = useState(false);
  const [addSaving, setAddSaving] = useState(false);
  const [addForm] = Form.useForm();

  // DB list per server
  const [dbOptions, setDbOptions] = useState<Record<string, string[]>>({});
  const [dbLoading, setDbLoading] = useState<Record<string, boolean>>({});

  // selections per server
  const [selectedDbs, setSelectedDbs] = useState<Record<string, string[]>>({});
  const [selectedBlocks, setSelectedBlocks] = useState<Record<string, string[]>>({});

  // collect loading per server
  const [collectLoading, setCollectLoading] = useState<Record<string, boolean>>({});

  // report drawer
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [activeReport, setActiveReport] = useState<Report | null>(null);
  const [actions, setActions] = useState<ReportAction[]>([]);
  const [actionsLoading, setActionsLoading] = useState(false);

  const defaultBlocks = useMemo(
    () => ["system", "connections_activity", "wal_replication"],
    []
  );

  async function loadServers() {
    setLoading(true);
    try {
      const res = await api.get<Server[]>("/api/servers");
      setServers(res.data);
    } catch (e: any) {
      message.error(`Failed to load servers: ${e?.message || e}`);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadServers();
  }, []);

  async function ensureDatabases(server: Server) {
    // if already loaded, skip
    if (dbOptions[server.id]?.length) return;

    setDbLoading((m) => ({ ...m, [server.id]: true }));
    try {
      const res = await api.get<{ databases: string[] }>(
        `/api/servers/${server.id}/databases`
      );
      const dbs = res.data.databases || [];
      setDbOptions((m) => ({ ...m, [server.id]: dbs }));

      // default selections
      setSelectedDbs((m) => ({ ...m, [server.id]: m[server.id] || ["all"] }));
      setSelectedBlocks((m) => ({
        ...m,
        [server.id]: m[server.id] || defaultBlocks,
      }));

      message.success(`Agent reachable: loaded ${dbs.length} DB(s)`);
    } catch (e: any) {
      message.error(
        `Failed to load DBs for ${server.name}: ${
          e?.response?.data?.detail || e?.message || e
        }`
      );
    } finally {
      setDbLoading((m) => ({ ...m, [server.id]: false }));
    }
  }

  async function loadActions(reportId: string) {
    setActionsLoading(true);
    try {
      const res = await api.get<ReportAction[]>(`/api/reports/${reportId}/actions`);
      setActions(res.data || []);
    } catch (e: any) {
      message.error(
        `Load actions failed: ${e?.response?.data?.detail || e?.message || e}`
      );
    } finally {
      setActionsLoading(false);
    }
  }

  async function collect(server: Server) {
    setCollectLoading((m) => ({ ...m, [server.id]: true }));
    try {
      const databases = selectedDbs[server.id] || ["all"];
      const blocks = selectedBlocks[server.id] || defaultBlocks;

      const res = await api.post<Report>(`/api/servers/${server.id}/collect`, {
        databases,
        blocks,
      });

      message.success("Report created");
      setActiveReport(res.data);
      setDrawerOpen(true);

      await loadActions(res.data.id);
    } catch (e: any) {
      message.error(`Collect failed: ${e?.response?.data?.detail || e?.message || e}`);
    } finally {
      setCollectLoading((m) => ({ ...m, [server.id]: false }));
    }
  }

  function downloadReport(reportId: string) {
    window.open(`/api/reports/${reportId}/download`, "_blank");
  }

  async function createServerAndMaybeInstall(values: any) {
    setAddSaving(true);
    try {
      // 1) create server record
      const created = await api.post("/api/servers", {
        name: values.name,
        ip: values.ip,
        agent_url: values.agent_url || `http://${values.ip}:8010`,
      });

      const serverId = created.data.id as string;

      // 2) optional SSH install
      if (values.install_via_ssh) {
        message.info("Installing agent via SSH...");

        const ssh_auth =
          values.ssh_auth_type === "password"
            ? { type: "password", password: values.ssh_password }
            : { type: "private_key", private_key: values.ssh_private_key };

        await api.post(`/api/servers/${serverId}/install-agent`, {
          ssh_user: values.ssh_user,
          ssh_port: Number(values.ssh_port || 22),
          ssh_auth,

          pg_host: values.pg_host || "127.0.0.1",
          pg_port: Number(values.pg_port || 5432),
          pg_user: values.pg_user,
          pg_password: values.pg_password,
          pg_database: values.pg_database || "postgres",

          agent_port: Number(values.agent_port || 8010),
          model: values.model || "gpt-4o-mini",
          max_statements: Number(values.max_statements || 50),

          openai_api_key: values.openai_api_key,
        });

        message.success("Agent installed");
      } else {
        message.success("Server added");
      }

      setAddOpen(false);
      addForm.resetFields();
      await loadServers();
    } catch (e: any) {
      message.error(
        `Create/install failed: ${e?.response?.data?.detail || e?.message || e}`
      );
    } finally {
      setAddSaving(false);
    }
  }

  const columns = [
    {
      title: "Server",
      key: "server",
      width: 280,
      render: (_: any, s: Server) => (
        <Space direction="vertical" size={0}>
          <Text strong>{s.name}</Text>
          <Text type="secondary" style={{ fontSize: 12 }}>
            {s.ip} • {s.agent_url}
          </Text>
          <div>{statusTag(s.status)}</div>
        </Space>
      ),
    },
    {
      title: "Databases",
      key: "dbs",
      width: 280,
      render: (_: any, s: Server) => {
        const opts = dbOptions[s.id] || [];
        const value = selectedDbs[s.id] || ["all"];

        return (
          <Select
            mode="multiple"
            style={{ width: "100%" }}
            placeholder="Select databases"
            loading={!!dbLoading[s.id]}
            onDropdownVisibleChange={(open) => open && ensureDatabases(s)}
            value={value}
            onChange={(vals) => {
              const v = vals as string[];

              // "all" is exclusive
              if (v.includes("all") && v.length > 1) {
                setSelectedDbs((m) => ({ ...m, [s.id]: ["all"] }));
              } else if (v.length === 0) {
                setSelectedDbs((m) => ({ ...m, [s.id]: ["all"] }));
              } else {
                setSelectedDbs((m) => ({ ...m, [s.id]: v }));
              }
            }}
            options={[
              { label: "All databases", value: "all" },
              ...opts.map((d) => ({ label: d, value: d })),
            ]}
          />
        );
      },
    },
    {
      title: "Metrics blocks",
      key: "blocks",
      width: 420,
      render: (_: any, s: Server) => (
        <Checkbox.Group
          options={BLOCK_OPTIONS}
          value={selectedBlocks[s.id] || defaultBlocks}
          onChange={(vals) =>
            setSelectedBlocks((m) => ({ ...m, [s.id]: vals as string[] }))
          }
        />
      ),
    },
    {
      title: "Actions",
      key: "actions",
      width: 180,
      render: (_: any, s: Server) => (
        <Space direction="vertical">
          <Button
            type="primary"
            loading={!!collectLoading[s.id]}
            onClick={async () => {
              await ensureDatabases(s);
              await collect(s);
            }}
          >
            Collect
          </Button>
          <Button
            onClick={async () => {
              await ensureDatabases(s);
            }}
          >
            Test agent
          </Button>
        </Space>
      ),
    },
  ];

  return (
    <Layout style={{ minHeight: "100vh" }}>
      <Header
        style={{
          background: "#001529",
          display: "flex",
          justifyContent: "space-between",
        }}
      >
        <Title style={{ color: "white", margin: 0 }} level={3}>
          pgAI
        </Title>

        <Space>
          <Button onClick={loadServers}>Refresh</Button>
          <Button type="primary" onClick={() => setAddOpen(true)}>
            Add server
          </Button>
        </Space>
      </Header>

      <Content style={{ padding: 16 }}>
        <Row gutter={[16, 16]}>
          <Col span={24}>
            <Card title="Servers" bodyStyle={{ padding: 0 }}>
              <Table
                rowKey="id"
                loading={loading}
                dataSource={servers}
                columns={columns as any}
                pagination={{ pageSize: 10 }}
              />
            </Card>
          </Col>
        </Row>

        {/* Add server modal */}
        <Modal
          title="Add server"
          open={addOpen}
          onCancel={() => setAddOpen(false)}
          okText={addSaving ? "Working..." : "Save"}
          confirmLoading={addSaving}
          onOk={() => addForm.submit()}
        >
          <Form
            form={addForm}
            layout="vertical"
            onFinish={createServerAndMaybeInstall}
            initialValues={{
              agent_url: "http://192.168.2.102:8010",
              install_via_ssh: false,
              ssh_port: 22,
              ssh_auth_type: "private_key",
              pg_host: "127.0.0.1",
              pg_port: 5432,
              pg_database: "postgres",
              agent_port: 8010,
              model: "gpt-4o-mini",
              max_statements: 50,
            }}
          >
            <Form.Item name="name" label="Server name" rules={[{ required: true }]}>
              <Input placeholder="pg-01" />
            </Form.Item>

            <Form.Item name="ip" label="Server IP/Host" rules={[{ required: true }]}>
              <Input placeholder="192.168.2.102" />
            </Form.Item>

            <Form.Item name="agent_url" label="Agent URL">
              <Input placeholder="http://192.168.2.102:8010" />
            </Form.Item>

            <Form.Item name="install_via_ssh" valuePropName="checked">
              <Checkbox>Install agent via SSH</Checkbox>
            </Form.Item>

            {/* Conditional sections */}
            <Form.Item shouldUpdate noStyle>
              {({ getFieldValue }) => {
                const on = !!getFieldValue("install_via_ssh");
                if (!on) {
                  return (
                    <Text type="secondary">
                      MVP: если SSH install выключен — агент должен быть уже запущен на сервере.
                    </Text>
                  );
                }

                return (
                  <>
                    <Card size="small" title="SSH settings" style={{ marginTop: 12 }}>
                      <Form.Item name="ssh_user" label="SSH user" rules={[{ required: true }]}>
                        <Input placeholder="ubuntu" />
                      </Form.Item>

                      <Form.Item name="ssh_port" label="SSH port">
                        <Input />
                      </Form.Item>

                      <Form.Item
                        name="ssh_auth_type"
                        label="Auth type"
                        rules={[{ required: true }]}
                      >
                        <Select
                          options={[
                            { label: "Private key", value: "private_key" },
                            { label: "Password", value: "password" },
                          ]}
                        />
                      </Form.Item>

                      <Form.Item shouldUpdate noStyle>
                        {({ getFieldValue }) => {
                          const t = getFieldValue("ssh_auth_type");
                          if (t === "password") {
                            return (
                              <Form.Item
                                name="ssh_password"
                                label="SSH password"
                                rules={[{ required: true }]}
                              >
                                <Input.Password />
                              </Form.Item>
                            );
                          }
                          return (
                            <Form.Item
                              name="ssh_private_key"
                              label="SSH private key (PEM)"
                              rules={[{ required: true }]}
                            >
                              <Input.TextArea rows={6} placeholder="-----BEGIN ...-----" />
                            </Form.Item>
                          );
                        }}
                      </Form.Item>
                    </Card>

                    <Card size="small" title="Postgres settings" style={{ marginTop: 12 }}>
                      <Form.Item name="pg_host" label="PG host">
                        <Input />
                      </Form.Item>
                      <Form.Item name="pg_port" label="PG port">
                        <Input />
                      </Form.Item>
                      <Form.Item name="pg_database" label="PG database">
                        <Input />
                      </Form.Item>
                      <Form.Item name="pg_user" label="PG user" rules={[{ required: true }]}>
                        <Input />
                      </Form.Item>
                      <Form.Item
                        name="pg_password"
                        label="PG password"
                        rules={[{ required: true }]}
                      >
                        <Input.Password />
                      </Form.Item>
                    </Card>

                    <Card size="small" title="Agent & LLM settings" style={{ marginTop: 12 }}>
                      <Form.Item name="agent_port" label="Agent port">
                        <Input />
                      </Form.Item>
                      <Form.Item name="model" label="LLM model">
                        <Input />
                      </Form.Item>
                      <Form.Item name="max_statements" label="MAX_STATEMENTS">
                        <Input />
                      </Form.Item>
                      <Form.Item
                        name="openai_api_key"
                        label="OPENAI_API_KEY (used on target agent)"
                        rules={[{ required: true }]}
                      >
                        <Input.Password />
                      </Form.Item>

                      <Text type="secondary">
                        Сейчас ключ передаётся агенту на целевую машину (для MVP). Позже сделаем вариант,
                        когда LLM вызывается только из control-plane.
                      </Text>
                    </Card>
                  </>
                );
              }}
            </Form.Item>
          </Form>
        </Modal>

        {/* Report drawer */}
        <Drawer
          title="Report"
          open={drawerOpen}
          onClose={() => setDrawerOpen(false)}
          width={920}
          extra={
            activeReport ? (
              <Space>
                <Button onClick={() => loadActions(activeReport.id)} loading={actionsLoading}>
                  Refresh actions
                </Button>
                <Button type="primary" onClick={() => downloadReport(activeReport.id)}>
                  Download JSON
                </Button>
              </Space>
            ) : null
          }
        >
          {!activeReport ? (
            <Text type="secondary">No report selected.</Text>
          ) : (
            <>
              <Card size="small" style={{ marginBottom: 12 }}>
                <Space direction="vertical" style={{ width: "100%" }}>
                  <Text>
                    <Text strong>Report ID:</Text> {activeReport.id}
                  </Text>
                  <Text>
                    <Text strong>Created:</Text> {activeReport.created_at}
                  </Text>
                  <Text>
                    <Text strong>Selected:</Text>{" "}
                    {JSON.stringify(activeReport.payload?.selected || {}, null, 0)}
                  </Text>
                </Space>
              </Card>

              <Card
                title={
                  <Space>
                    <span>Recommendations</span>
                    <Tag>{actions.length}</Tag>
                  </Space>
                }
                bodyStyle={{ padding: 0 }}
              >
                <Table
                  rowKey="id"
                  loading={actionsLoading}
                  dataSource={actions}
                  pagination={{ pageSize: 20 }}
                  columns={[
                    { title: "Type", dataIndex: "type", width: 180 },
                    { title: "Target", dataIndex: "target", width: 260 },
                    {
                      title: "Risk",
                      dataIndex: "risk",
                      width: 100,
                      render: (v: string) => riskTag(v),
                    },
                    { title: "Reason", dataIndex: "reason" },
                  ]}
                  expandable={{
                    expandedRowRender: (r) => (
                      <pre style={{ margin: 0, whiteSpace: "pre-wrap" }}>
                        {JSON.stringify(r.raw, null, 2)}
                      </pre>
                    ),
                  }}
                />
              </Card>

              <Card title="Notes" style={{ marginTop: 12 }}>
                <pre style={{ margin: 0, whiteSpace: "pre-wrap" }}>
                  {(activeReport.payload?.notes || []).join("\n")}
                </pre>
              </Card>
            </>
          )}
        </Drawer>
      </Content>
    </Layout>
  );
}
