export type HostAuthType = 'password' | 'key' | 'arcos';

export interface Host {
  id: string;
  hostname: string;
  ip_address: string;
  ssh_port: number;
  username: string;
  auth_type: HostAuthType;
  os_info: string | null;
  status: 'online' | 'offline' | 'unknown';
  created_at: string;
}

export interface HostCreate {
  hostname: string;
  ip_address: string;
  ssh_port: number;
  username: string;
  auth_type: HostAuthType;
  credential: string;
}

export interface HostTestResult {
  success: boolean;
  message: string;
  os_info: string | null;
}

export interface PrereqCheck {
  name: string;
  status: 'pass' | 'fail' | 'warn';
  message: string;
  details: string | null;
}

export interface PrereqResult {
  host_id: string;
  checks: PrereqCheck[];
  all_passed: boolean;
}

export interface ServiceAssignment {
  host_id: string;
  role: string;
  node_id: number;
}

export interface ClusterConfig {
  replication_factor: number;
  num_partitions: number;
  log_dirs: string;
  listener_port: number;
  controller_port: number;
  ssl_listener_port?: number;     // populated when ssl_enabled is on
  schema_registry_port?: number;  // optional component
  heap_size: string;
  ksqldb_port: number;
  connect_port: number;
  connect_rest_port: number;
  /**
   * Optional operator-supplied Kafka binary install directory.
   *
   * When omitted (or empty string) the backend auto-derives a unique path
   * from the cluster UUID, e.g. /opt/kafka-prod-a1b2c3d4, so that multiple
   * clusters on the same broker host coexist without collision.
   *
   * Must be an absolute path if provided. Validated server-side.
   */
  kafka_install_dir?: string;
  /**
   * Optional operator-supplied Kafka data directory (log.dirs in
   * server.properties).
   *
   * When omitted the backend auto-derives a unique path, e.g.
   * /var/lib/kafka-prod-a1b2c3d4/data. Must be an absolute path if
   * provided. Validated server-side.
   */
  kafka_data_dir?: string;
}

/**
 * One ACL rule to pre-seed during cluster deployment.
 * Applied by the deployer immediately after the broker TCP port becomes
 * reachable (up to 60 s wait). Failures are warnings, not errors.
 */
export interface InitialAcl {
  principal: string;       // "User:myapp" or bare "myapp"
  resource_type: string;   // "topic" | "group" | "cluster" | "transactional-id"
  resource_name: string;   // specific name, prefix, or "*"
  pattern_type: string;    // "literal" | "prefixed"
  operations: string[];    // ["Read", "Write", "Describe", ...]
  permission_type: string; // "Allow" | "Deny"
  host: string;            // source IP filter, "*" = any
  cpu_quota?: string;
  memory_max?: string;
  retention_hours?: number;
  jvm_performance_opts?: string;
  jmx_port?: number;
  gc_logging_enabled?: boolean;
}

export interface ClusterCreate {
  name: string;
  kafka_version: string;
  mode: 'kraft' | 'zookeeper';
  services: ServiceAssignment[];
  config: ClusterConfig;
  environment?: string;
  /** ACLs to apply right after the broker comes up. Optional. */
  initial_acls?: InitialAcl[];
}

export interface Cluster {
  id: string;
  name: string;
  kafka_version: string;
  mode: string;
  state: string;
  config_json: string | null;
  created_at: string;
  // PR #3: external clusters share the same listing, distinguished by `kind`.
  kind?: 'managed' | 'external';
  // QA #51: optional env tag (dev / qa / prod / etc).
  environment?: string;
  // v1.4.5: resolved deploy paths — present on managed clusters created with
  // per-cluster path support (v1.3.5+). Null for legacy rows (use defaults).
  kafka_install_dir?: string | null;
  kafka_data_dir?: string | null;
  kafka_unit_name?: string | null;
}

export interface ServiceInfo {
  id: string;
  cluster_id: string;
  host_id: string;
  role: string;
  node_id: number;
  config_overrides: string | null;
  status: string;
}

export interface ClusterDetail {
  cluster: Cluster;
  services: ServiceInfo[];
}

export interface DeploymentTask {
  task_id: string;
  cluster_id: string;
  status: string;
  current_step?: string;
  logs?: string[];
  error_message?: string;
  started_at?: string;
  finished_at?: string;
}

export interface ServiceAction {
  service_id: string;
  action: string;
  success: boolean;
  message: string;
}

export interface ServiceStatus {
  service_id: string;
  host: string;
  hostname: string;
  role: string;
  node_id: number;
  status: string;
  error?: string;
}

// ---- Kafka Version types ----

export interface KafkaVersionInfo {
  version: string;
  scala_version: string;
  filename: string;
  size_mb: number;
  available: boolean;
  release_date: string | null;
  features: string[] | null;
  security_fixes: string[] | null;
  upgrade_notes: string | null;
}

export interface ConnectPluginFile {
  name: string;
  filename: string;
  size_mb: number;
}

// ---- Topic / Consumer / Producer types ----

export interface TopicCreate {
  name: string;
  partitions: number;
  replication_factor: number;
  config?: Record<string, string>;
}

export interface TopicInfo {
  name: string;
  partitions: number;
  replication_factor: number;
  configs?: Record<string, string>;
}

export interface TopicDetail extends TopicInfo {
  partition_details: Array<Record<string, unknown>>;
}

export interface ConsumerGroupInfo {
  group_id: string;
  state: string;
  members: number;
  topics: string[];
}

export interface ConsumerGroupDetail extends ConsumerGroupInfo {
  offsets: Array<Record<string, unknown>>;
}

export interface ProduceRequest {
  topic: string;
  key?: string;
  value: string;
  headers?: Record<string, string>;
}

export interface ProduceResponse {
  success: boolean;
  message: string;
}

// ---- Consume types ----

export interface ConsumeRequest {
  topic: string;
  from_beginning: boolean;
  max_messages: number;
  group_id?: string;
  timeout_ms: number;
}

export interface ConsumedMessage {
  timestamp: number | string | null;
  partition: number | null;
  offset: number | null;
  key: string | null;
  value: string;
  headers: string | null;
}

export interface ConsumeResponse {
  messages: ConsumedMessage[];
  count: number;
}

// ---- Validation types ----

export interface ValidationStep {
  step: string;
  success: boolean;
  message: string;
  data?: unknown[];
}

export interface ValidationResult {
  steps: ValidationStep[];
  success: boolean;
}

// ---- Security types ----

export interface KafkaUserInfo {
  id: string;
  cluster_id: string;
  username: string;
  mechanism: string;
  created_at: string;
  updated_at: string;
}

export interface KafkaUserCreateRequest {
  username: string;
  password?: string;
  mechanism: string;
}

export interface KafkaUserCreatedResponse {
  id: string;
  username: string;
  mechanism: string;
  password: string;
  message: string;
}

export interface KafkaUserRotateRequest {
  password?: string;
}

export interface KafkaUserRotateResponse {
  username: string;
  mechanism: string;
  password: string;
  message: string;
}

export interface KafkaUserDeleteResponse {
  username: string;
  deleted: boolean;
  message: string;
}

export interface AclEntry {
  principal: string;
  resource_type: string;
  resource_name: string;
  pattern_type: string;
  operation: string;
  permission_type: string;
  host: string;
}

export interface AclCreateRequest {
  principal: string;
  resource_type: string;
  resource_name: string;
  pattern_type: string;
  operations: string[];
  permission_type: string;
  host: string;
}

export interface AclCreateResponse {
  success: boolean;
  message: string;
  acls_added: number;
}

export interface AclDeleteRequest {
  principal: string;
  resource_type: string;
  resource_name: string;
  pattern_type: string;
  operations: string[];
  permission_type: string;
  host: string;
}

export interface AclDeleteResponse {
  success: boolean;
  message: string;
}

export interface AclListResponse {
  acls: AclEntry[];
  count: number;
}

export interface AuditLogEntry {
  id: string;
  cluster_id: string;
  action: string;
  resource_type: string;
  resource_name: string;
  details: string | null;
  actor_username: string | null;
  created_at: string;
}

// ---- Kafka Connect types ----

export interface ConnectorCreate {
  name: string;
  config: Record<string, string>;
}

export interface ConnectorStatus {
  name: string;
  connector: Record<string, unknown>;
  tasks: Array<Record<string, unknown>>;
  type: string;
}

export interface ConnectorPluginInfo {
  class_name: string;
  type: string;
  version: string | null;
}

export interface CdcTemplateField {
  key: string;
  label: string;
  required?: boolean;
  default?: string;
  placeholder?: string;
  secret?: boolean;
}

export interface CdcTemplate {
  id: string;
  name: string;
  connector_class: string;
  description: string;
  fields: CdcTemplateField[];
  fixed?: Record<string, string>;
}

// ---- ksqlDB types ----

export interface KsqlExecuteRequest {
  sql: string;
  timeout?: number;
}

export interface KsqlExecuteResponse {
  type: 'statement' | 'query' | 'error';
  status?: string;
  message?: string;
  statementText?: string;
  columns?: string[];
  rows?: unknown[][];
  is_push_query?: boolean;
  query_id?: string;
  row_count?: number;
  entities?: unknown[];
  errorCode?: number;
}

export interface KsqlStreamStartResponse {
  stream_id: string;
  status: string;
}

export interface KsqlStreamPollResponse {
  columns: string[];
  rows: unknown[][];
  total_rows: number;
  status: string;
  error: string | null;
  query_id: string | null;
  done: boolean;
}

export interface KsqlServerInfo {
  version: string;
  kafkaClusterId: string;
  ksqlServiceId: string;
  status: string;
}

export interface KsqlEntity {
  name: string;
  type: 'STREAM' | 'TABLE';
  topic: string;
  keyFormat: string;
  valueFormat: string;
}

export interface KsqlEntitiesResponse {
  streams: KsqlEntity[];
  tables: KsqlEntity[];
}

export interface KsqlQueryHistory {
  id: string;
  cluster_id: string;
  sql: string;
  name?: string;
  status: string;
  created_at: string;
}

// ---- Auth types ----

export interface TokenResponse {
  access_token: string;
  refresh_token: string;
  token_type: string;
  role: string;
}

export interface UserResponse {
  id: string;
  username: string;
  role: string;
  is_active: boolean;
  auth_source?: string;          // "local" | "ldap"
  ldap_dn?: string | null;
  created_at: string;
  last_login: string | null;
}

export interface UserCreate {
  username: string;
  password: string;
  role: string;
}

export interface UserUpdate {
  role?: string;
  password?: string;
  is_active?: boolean;
}

// ---- Service Logs types ----

export interface LogResponse {
  service_id: string;
  host_ip: string;
  hostname: string;
  role: string;
  lines: string[];
  line_count: number;
}

// ---- Monitoring types ----

export interface MonitoringStatus {
  prometheus_installed: boolean;
  grafana_installed: boolean;
  prometheus_running: boolean;
  grafana_running: boolean;
  prometheus_port: number;
  grafana_port: number;
  grafana_url: string | null;
  prometheus_url: string | null;
}

export interface GrafanaDashboard {
  name: string;
  title: string;
  url: string;
}

export interface ExporterStatus {
  host_ip: string;
  hostname: string;
  node_exporter: string;
  jmx_exporter: string;
}

export interface CapacityForecast {
  available: boolean;
  reason?: string;
  history: Array<{ t: number; used_bytes: number }>;
  forecast: Array<{ t: number; used_bytes: number }>;
  total_bytes: number;
  current_used_bytes: number;
  current_used_pct: number;
  growth_bytes_per_day: number;
  eta_to_threshold_unix: number | null;
  eta_to_threshold_days: number | null;
  full_threshold: number;
}
