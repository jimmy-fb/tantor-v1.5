import axios from 'axios';
import { getAccessToken, getRefreshToken, setTokens, clearTokens } from './auth';
import type {
  Host, HostCreate, HostTestResult, PrereqResult,
  Cluster, ClusterCreate, ClusterDetail, DeploymentTask,
  ServiceAction, ServiceStatus, ServiceAssignment, ServiceInfo,
  KafkaVersionInfo, ConnectPluginFile,
  TopicInfo, TopicDetail, TopicCreate, ConsumerGroupInfo, ConsumerGroupDetail,
  ProduceRequest, ProduceResponse,
  ConsumeRequest, ConsumeResponse, ValidationResult,
  ConnectorCreate, ConnectorStatus, ConnectorPluginInfo,
  KafkaUserInfo, KafkaUserCreateRequest, KafkaUserCreatedResponse,
  KafkaUserRotateRequest, KafkaUserRotateResponse, KafkaUserDeleteResponse,
  AclListResponse, AclCreateRequest, AclCreateResponse,
  AclDeleteRequest, AclDeleteResponse, AuditLogEntry,
  KsqlExecuteResponse, KsqlStreamStartResponse, KsqlStreamPollResponse,
  KsqlServerInfo, KsqlEntitiesResponse, KsqlQueryHistory,
  TokenResponse, UserResponse, UserCreate, UserUpdate,
  LogResponse,
} from '../types';

const api = axios.create({
  baseURL: '/api',
});

// ── Request interceptor: attach JWT token ────────────
api.interceptors.request.use((config) => {
  const token = getAccessToken();
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// ── Response interceptor: handle 401, attempt token refresh ──
let isRefreshing = false;
let failedQueue: Array<{ resolve: (token: string) => void; reject: (err: unknown) => void }> = [];

const processQueue = (error: unknown, token: string | null = null) => {
  failedQueue.forEach(({ resolve, reject }) => {
    if (error) reject(error);
    else if (token) resolve(token);
  });
  failedQueue = [];
};

api.interceptors.response.use(
  (response) => response,
  async (error) => {
    const originalRequest = error.config;

    // Don't intercept login/refresh calls
    if (originalRequest?.url?.includes('/auth/login') || originalRequest?.url?.includes('/auth/refresh')) {
      return Promise.reject(error);
    }

    if (error.response?.status === 401 && !originalRequest._retry) {
      if (isRefreshing) {
        return new Promise((resolve, reject) => {
          failedQueue.push({
            resolve: (token: string) => {
              originalRequest.headers.Authorization = `Bearer ${token}`;
              resolve(api(originalRequest));
            },
            reject,
          });
        });
      }

      originalRequest._retry = true;
      isRefreshing = true;

      const refreshToken = getRefreshToken();
      if (!refreshToken) {
        clearTokens();
        window.location.href = '/login';
        return Promise.reject(error);
      }

      try {
        const { data } = await axios.post<TokenResponse>('/api/auth/refresh', { refresh_token: refreshToken });
        setTokens(data.access_token, data.refresh_token, data.role, '');
        processQueue(null, data.access_token);
        originalRequest.headers.Authorization = `Bearer ${data.access_token}`;
        return api(originalRequest);
      } catch (refreshError) {
        processQueue(refreshError, null);
        clearTokens();
        window.location.href = '/login';
        return Promise.reject(refreshError);
      } finally {
        isRefreshing = false;
      }
    }

    return Promise.reject(error);
  }
);

// ── Auth ─────────────────────────────────────────────
export const login = (username: string, password: string) =>
  api.post<TokenResponse>('/auth/login', { username, password }).then(r => r.data);
export const refreshAuthToken = (refresh_token: string) =>
  api.post<TokenResponse>('/auth/refresh', { refresh_token }).then(r => r.data);
export const getMe = () => api.get<UserResponse>('/auth/me').then(r => r.data);
export const getUsers = () => api.get<UserResponse[]>('/auth/users').then(r => r.data);
export const createAuthUser = (data: UserCreate) =>
  api.post<UserResponse>('/auth/users', data).then(r => r.data);
export const updateAuthUser = (userId: string, data: UserUpdate) =>
  api.put<UserResponse>(`/auth/users/${userId}`, data).then(r => r.data);
export const deleteAuthUser = (userId: string) =>
  api.delete(`/auth/users/${userId}`).then(r => r.data);
export const getHealthInfo = () =>
  api.get<{ status: string; version: string }>('/health').then(r => r.data);

// ── Hosts ────────────────────────────────────────────
export const getHosts = () => api.get<Host[]>('/hosts').then(r => r.data);
export const getHost = (id: string) => api.get<Host>(`/hosts/${id}`).then(r => r.data);
export const createHost = (data: HostCreate) => api.post<Host>('/hosts', data).then(r => r.data);
export const updateHost = (id: string, data: Partial<HostCreate>) =>
  api.put<Host>(`/hosts/${id}`, data).then(r => r.data);
export const deleteHost = (id: string) => api.delete(`/hosts/${id}`);
export const testHost = (id: string) => api.post<HostTestResult>(`/hosts/${id}/test`).then(r => r.data);
export const checkPrereqs = (id: string) => api.post<PrereqResult>(`/hosts/${id}/prerequisites`).then(r => r.data);

// ── Clusters ─────────────────────────────────────────
export const getClusters = () => api.get<Cluster[]>('/clusters').then(r => r.data);
export const getCluster = (id: string) => api.get<ClusterDetail>(`/clusters/${id}`).then(r => r.data);
export const createCluster = (data: ClusterCreate) => api.post<Cluster>('/clusters', data).then(r => r.data);
export const deleteCluster = (id: string) => api.delete(`/clusters/${id}`);
export const deployCluster = (id: string) => api.post<DeploymentTask>(`/clusters/${id}/deploy`).then(r => r.data);
export const getDeploymentStatus = (clusterId: string, taskId: string) =>
  api.get<DeploymentTask>(`/clusters/${clusterId}/deploy/${taskId}`).then(r => r.data);
export const startCluster = (id: string) => api.post<ServiceAction[]>(`/clusters/${id}/start`).then(r => r.data);
export const stopCluster = (id: string) => api.post<ServiceAction[]>(`/clusters/${id}/stop`).then(r => r.data);
export const getClusterStatus = (id: string) => api.get<ServiceStatus[]>(`/clusters/${id}/status`).then(r => r.data);

// ── Cluster node scaling ─────────────────────────────
export const addServices = (clusterId: string, services: ServiceAssignment[]) =>
  api.post<ServiceInfo[]>(`/clusters/${clusterId}/services`, services).then(r => r.data);
export const removeService = (clusterId: string, serviceId: string, force: boolean = false) =>
  api.delete(`/clusters/${clusterId}/services/${serviceId}?force=${force}`).then(r => r.data);

// ── Kafka Versions ───────────────────────────────────
export const getKafkaVersions = () => api.get<KafkaVersionInfo[]>('/versions/kafka').then(r => r.data);
export const getKafkaVersion = (ver: string) => api.get<KafkaVersionInfo>(`/versions/kafka/${ver}`).then(r => r.data);
export const uploadKafkaBinary = (file: File) => {
  const form = new FormData();
  form.append('file', file);
  return api.post<{ filename: string; size_mb: number; uploaded: boolean }>('/versions/kafka/upload', form, {
    headers: { 'Content-Type': 'multipart/form-data' },
  }).then(r => r.data);
};
export const getConnectPluginFiles = () => api.get<ConnectPluginFile[]>('/versions/connect-plugins').then(r => r.data);

// ── Topics ───────────────────────────────────────────
export const getTopics = (clusterId: string, search?: string) =>
  api.get<TopicInfo[]>(`/clusters/${clusterId}/topics`, { params: search ? { search } : {} }).then(r => r.data);
export const getTopic = (clusterId: string, name: string) =>
  api.get<TopicDetail>(`/clusters/${clusterId}/topics/${name}`).then(r => r.data);
export const createTopic = (clusterId: string, data: TopicCreate) =>
  api.post(`/clusters/${clusterId}/topics`, data).then(r => r.data);
export const deleteTopic = (clusterId: string, name: string) =>
  api.delete(`/clusters/${clusterId}/topics/${name}`).then(r => r.data);
export const updateTopicConfig = (clusterId: string, topicName: string, configs: Record<string, string>) =>
  api.put(`/clusters/${clusterId}/topics/${encodeURIComponent(topicName)}/config`, { configs }).then(r => r.data);
export const updateTopicPartitions = (clusterId: string, topicName: string, count: number) =>
  api.put(`/clusters/${clusterId}/topics/${encodeURIComponent(topicName)}/partitions`, { count }).then(r => r.data);

// ── Consumer Groups ──────────────────────────────────
export const getConsumerGroups = (clusterId: string) =>
  api.get<ConsumerGroupInfo[]>(`/clusters/${clusterId}/consumer-groups`).then(r => r.data);
export const getConsumerGroup = (clusterId: string, groupId: string) =>
  api.get<ConsumerGroupDetail>(`/clusters/${clusterId}/consumer-groups/${groupId}`).then(r => r.data);

// ── Produce ──────────────────────────────────────────
export const produceMessage = (clusterId: string, data: ProduceRequest) =>
  api.post<ProduceResponse>(`/clusters/${clusterId}/produce`, data).then(r => r.data);

// ── Consume ──────────────────────────────────────────
export const consumeMessages = (clusterId: string, data: ConsumeRequest) =>
  api.post<ConsumeResponse>(`/clusters/${clusterId}/consume`, data).then(r => r.data);

// ── Validation ───────────────────────────────────────
export const validateCluster = (clusterId: string, createTestTopic: boolean = true) =>
  api.post<ValidationResult>(`/clusters/${clusterId}/validate?create_test_topic=${createTestTopic}`).then(r => r.data);

// ── Kafka Connect ────────────────────────────────────
export const getConnectors = (clusterId: string) =>
  api.get<ConnectorStatus[]>(`/clusters/${clusterId}/connect/connectors`).then(r => r.data);
export const createConnector = (clusterId: string, data: ConnectorCreate) =>
  api.post(`/clusters/${clusterId}/connect/connectors`, data).then(r => r.data);
export const getConnectorStatus = (clusterId: string, name: string) =>
  api.get<ConnectorStatus>(`/clusters/${clusterId}/connect/connectors/${name}/status`).then(r => r.data);
export const deleteConnector = (clusterId: string, name: string) =>
  api.delete(`/clusters/${clusterId}/connect/connectors/${name}`).then(r => r.data);
export const pauseConnector = (clusterId: string, name: string) =>
  api.put(`/clusters/${clusterId}/connect/connectors/${name}/pause`).then(r => r.data);
export const resumeConnector = (clusterId: string, name: string) =>
  api.put(`/clusters/${clusterId}/connect/connectors/${name}/resume`).then(r => r.data);
export const restartConnector = (clusterId: string, name: string) =>
  api.post(`/clusters/${clusterId}/connect/connectors/${name}/restart`).then(r => r.data);
export const getConnectPlugins = (clusterId: string) =>
  api.get<ConnectorPluginInfo[]>(`/clusters/${clusterId}/connect/plugins`).then(r => r.data);

// ── Security - Kafka Users ───────────────────────────
export const getKafkaUsers = (clusterId: string) =>
  api.get<KafkaUserInfo[]>(`/clusters/${clusterId}/security/users`).then(r => r.data);
export const createKafkaUser = (clusterId: string, data: KafkaUserCreateRequest) =>
  api.post<KafkaUserCreatedResponse>(`/clusters/${clusterId}/security/users`, data).then(r => r.data);
export const deleteKafkaUser = (clusterId: string, username: string) =>
  api.delete<KafkaUserDeleteResponse>(`/clusters/${clusterId}/security/users/${username}`).then(r => r.data);
export const rotateKafkaUserPassword = (clusterId: string, username: string, data: KafkaUserRotateRequest) =>
  api.post<KafkaUserRotateResponse>(`/clusters/${clusterId}/security/users/${username}/rotate`, data).then(r => r.data);

// ── Security - ACLs ──────────────────────────────────
export const getAcls = (clusterId: string, params?: { principal?: string; resource_type?: string; resource_name?: string }) =>
  api.get<AclListResponse>(`/clusters/${clusterId}/security/acls`, { params }).then(r => r.data);
export const getTopicAcls = (clusterId: string, topicName: string) =>
  api.get<AclListResponse>(`/clusters/${clusterId}/security/acls/topic/${topicName}`).then(r => r.data);
export const createAcl = (clusterId: string, data: AclCreateRequest) =>
  api.post<AclCreateResponse>(`/clusters/${clusterId}/security/acls`, data).then(r => r.data);
export const deleteAcl = (clusterId: string, data: AclDeleteRequest) =>
  api.delete<AclDeleteResponse>(`/clusters/${clusterId}/security/acls`, { data }).then(r => r.data);

// ── Security - Audit Log ─────────────────────────────
export const getAuditLog = (clusterId: string, params?: { limit?: number; offset?: number; action?: string }) =>
  api.get<AuditLogEntry[]>(`/clusters/${clusterId}/security/audit-log`, { params }).then(r => r.data);

// ── ksqlDB ───────────────────────────────────────────
export const getKsqlStatus = (clusterId: string) =>
  api.get<KsqlServerInfo>(`/clusters/${clusterId}/ksqldb/status`).then(r => r.data);
export const executeKsql = (clusterId: string, sql: string, timeout?: number) =>
  api.post<KsqlExecuteResponse>(`/clusters/${clusterId}/ksqldb/execute`, { sql, timeout }).then(r => r.data);
export const startKsqlStream = (clusterId: string, sql: string) =>
  api.post<KsqlStreamStartResponse>(`/clusters/${clusterId}/ksqldb/stream/start`, { sql }).then(r => r.data);
export const pollKsqlStream = (clusterId: string, streamId: string) =>
  api.get<KsqlStreamPollResponse>(`/clusters/${clusterId}/ksqldb/stream/${streamId}/poll`).then(r => r.data);
export const stopKsqlStream = (clusterId: string, streamId: string) =>
  api.post(`/clusters/${clusterId}/ksqldb/stream/${streamId}/stop`).then(r => r.data);
export const getKsqlEntities = (clusterId: string) =>
  api.get<KsqlEntitiesResponse>(`/clusters/${clusterId}/ksqldb/entities`).then(r => r.data);
export const terminateKsqlQuery = (clusterId: string, queryId: string) =>
  api.post(`/clusters/${clusterId}/ksqldb/terminate/${queryId}`).then(r => r.data);
export const getKsqlHistory = (clusterId: string, limit = 50) =>
  api.get<KsqlQueryHistory[]>(`/clusters/${clusterId}/ksqldb/history`, { params: { limit } }).then(r => r.data);
export const saveKsqlQuery = (clusterId: string, sql: string, name: string) =>
  api.post<KsqlQueryHistory>(`/clusters/${clusterId}/ksqldb/history`, { sql, name }).then(r => r.data);
export const deleteKsqlHistory = (clusterId: string, historyId: string) =>
  api.delete(`/clusters/${clusterId}/ksqldb/history/${historyId}`).then(r => r.data);

// ── Service Logs ─────────────────────────────────────
export const getServiceLogs = (clusterId: string, params: {
  service_id?: string; role?: string; lines?: number;
  since?: string; priority?: string; grep?: string;
}) => api.get<LogResponse>(`/clusters/${clusterId}/logs`, { params }).then(r => r.data);

// ── Partition Rebalancing ────────────────────────────
export const getPartitionDistribution = (clusterId: string) =>
  api.get(`/clusters/${clusterId}/partitions/distribution`).then(r => r.data);
export const generateReassignmentPlan = (clusterId: string, data: { topics: string[]; broker_ids: number[] }) =>
  api.post(`/clusters/${clusterId}/partitions/generate-plan`, data).then(r => r.data);
export const executeReassignment = (clusterId: string, data: { reassignment: Record<string, unknown> }) =>
  api.post(`/clusters/${clusterId}/partitions/execute`, data).then(r => r.data);
export const verifyReassignment = (clusterId: string, data: { reassignment: Record<string, unknown> }) =>
  api.post(`/clusters/${clusterId}/partitions/verify`, data).then(r => r.data);

// ── Monitoring (built-in + Grafana) ──────────────────
export const getMonitoringStatus = () => api.get('/monitoring/status').then(r => r.data);
export const getClusterMetrics = (clusterId: string) =>
  api.get(`/monitoring/clusters/${clusterId}/metrics`).then(r => r.data);
export const deployMonitoring = (clusterId: string, data: { monitoring_host_id: string; grafana_port?: number; prometheus_port?: number }) =>
  api.post(`/monitoring/clusters/${clusterId}/deploy`, data).then(r => r.data);
export const getGrafanaInfo = (clusterId: string) =>
  api.get(`/monitoring/clusters/${clusterId}/grafana`).then(r => r.data);

// ── LDAP / Active Directory ─────────────────────────
export const getLdapConfig = () => api.get('/ldap/config').then(r => r.data);
export const updateLdapConfig = (data: Record<string, unknown>) => api.put('/ldap/config', data).then(r => r.data);
export const testLdapConnection = (data: { username: string; password: string }) => api.post('/ldap/test', data).then(r => r.data);
export const syncLdapUsers = () => api.post('/ldap/sync-users').then(r => r.data);

// ── Activity feed ───────────────────────────────────
export interface ActivityEntry {
  id: string;
  kind: 'security' | 'config';
  cluster_id: string | null;
  cluster_name: string | null;
  action: string;
  resource: string;
  actor: string | null;
  details: string | null;
  occurred_at: string;
}
export interface ActivityResponse {
  entries: ActivityEntry[];
  count: number;
  has_more: boolean;
}
export const getActivity = (params: {
  cluster_id?: string;
  kind?: 'security' | 'config';
  q?: string;
  since?: string;
  limit?: number;
  offset?: number;
} = {}) => api.get<ActivityResponse>('/activity', { params }).then(r => r.data);

// ── Alerting ────────────────────────────────────────
export type Severity = 'info' | 'warning' | 'critical';
export type ChannelKind = 'slack' | 'webhook' | 'email' | 'tantor_internal';

export interface RuleTemplate {
  id: string;
  name: string;
  severity: Severity;
  for_seconds: number;
  expr: string;
  summary: string;
  description: string;
}
export interface AlertRule {
  id: string;
  cluster_id: string;
  name: string;
  expr: string;
  for_seconds: number;
  severity: Severity;
  summary: string | null;
  description: string | null;
  channel_ids: string[];
  template: string | null;
  enabled: boolean;
  created_at: string;
  updated_at: string;
}
export interface AlertRuleCreate {
  name: string;
  expr: string;
  for_seconds: number;
  severity: Severity;
  summary?: string | null;
  description?: string | null;
  channel_ids: string[];
  template?: string | null;
  enabled?: boolean;
}
export interface FiringAlert {
  fingerprint: string;
  alert_name: string;
  severity: Severity;
  state: 'firing' | 'pending' | 'resolved';
  started_at: string | null;
  ends_at: string | null;
  summary: string | null;
  description: string | null;
  labels: Record<string, string>;
}
export interface FiringAlertsResponse {
  alerts: FiringAlert[];
  count: number;
  alertmanager_url: string | null;
  alertmanager_reachable: boolean;
}
export interface AlertIncident {
  id: string;
  fingerprint: string;
  cluster_id: string | null;
  rule_id: string | null;
  alert_name: string;
  severity: Severity;
  status: 'firing' | 'resolved';
  summary: string | null;
  description: string | null;
  started_at: string;
  resolved_at: string | null;
  last_seen_at: string;
}
export interface NotificationChannel {
  id: string;
  name: string;
  kind: ChannelKind;
  enabled: boolean;
  config_redacted: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}
export interface NotificationChannelCreate {
  name: string;
  kind: ChannelKind;
  enabled: boolean;
  config: Record<string, unknown>;
}

export const getRuleTemplates = (clusterId: string) =>
  api.get<RuleTemplate[]>(`/clusters/${clusterId}/alerts/rule-templates`).then(r => r.data);
export const getAlertRules = (clusterId: string) =>
  api.get<AlertRule[]>(`/clusters/${clusterId}/alerts/rules`).then(r => r.data);
export const createAlertRule = (clusterId: string, data: AlertRuleCreate) =>
  api.post<AlertRule>(`/clusters/${clusterId}/alerts/rules`, data).then(r => r.data);
export const updateAlertRule = (clusterId: string, ruleId: string, data: Partial<AlertRuleCreate>) =>
  api.put<AlertRule>(`/clusters/${clusterId}/alerts/rules/${ruleId}`, data).then(r => r.data);
export const deleteAlertRule = (clusterId: string, ruleId: string) =>
  api.delete(`/clusters/${clusterId}/alerts/rules/${ruleId}`).then(r => r.data);
export const getFiringAlerts = (clusterId: string) =>
  api.get<FiringAlertsResponse>(`/clusters/${clusterId}/alerts/firing`).then(r => r.data);
export const getAlertIncidents = (clusterId: string, status?: 'firing' | 'resolved', limit = 100) =>
  api.get<AlertIncident[]>(`/clusters/${clusterId}/alerts/incidents`, { params: { status, limit } }).then(r => r.data);
export const getNotificationChannels = () =>
  api.get<NotificationChannel[]>('/notification-channels').then(r => r.data);
export const createNotificationChannel = (data: NotificationChannelCreate) =>
  api.post<NotificationChannel>('/notification-channels', data).then(r => r.data);
export const updateNotificationChannel = (id: string, data: Partial<NotificationChannelCreate>) =>
  api.put<NotificationChannel>(`/notification-channels/${id}`, data).then(r => r.data);
export const deleteNotificationChannel = (id: string) =>
  api.delete(`/notification-channels/${id}`).then(r => r.data);
export const testNotificationChannel = (id: string, body: { severity?: Severity; summary?: string; description?: string } = {}) =>
  api.post<{ success: boolean; message: string }>(`/notification-channels/${id}/test`, body).then(r => r.data);
