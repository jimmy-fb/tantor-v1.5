import { BrowserRouter, Routes, Route } from 'react-router-dom';
import Shell from './components/layout/Shell';
import ProtectedRoute from './components/auth/ProtectedRoute';
import Login from './pages/Login';
import Dashboard from './pages/Dashboard';
import Hosts from './pages/Hosts';
import Clusters from './pages/Clusters';
import NewCluster from './pages/NewCluster';
import ClusterDetail from './pages/ClusterDetail';
import KafkaVersions from './pages/KafkaVersions';
import UserManagement from './pages/UserManagement';
import Monitoring from './pages/Monitoring';

import SecurityScan from './pages/SecurityScan';
import ClusterLinking from './pages/ClusterLinking';
import LdapSettings from './pages/LdapSettings';
import Activity from './pages/Activity';
import Alerts from './pages/Alerts';
import SchemaRegistry from './pages/SchemaRegistry';
import ExternalClusters from './pages/ExternalClusters';

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/login" element={<Login />} />
        <Route element={<ProtectedRoute />}>
          <Route element={<Shell />}>
            <Route path="/" element={<Dashboard />} />
            <Route path="/hosts" element={<Hosts />} />
            <Route path="/clusters" element={<Clusters />} />
            <Route path="/clusters/new" element={<NewCluster />} />
            <Route path="/clusters/:id" element={<ClusterDetail />} />
            <Route path="/versions" element={<KafkaVersions />} />
            <Route path="/users" element={<UserManagement />} />
            <Route path="/monitoring" element={<Monitoring />} />

            <Route path="/security-scan" element={<SecurityScan />} />
            <Route path="/cluster-linking" element={<ClusterLinking />} />
            <Route path="/ldap-settings" element={<LdapSettings />} />
            <Route path="/activity" element={<Activity />} />
            <Route path="/alerts" element={<Alerts />} />
            <Route path="/schema-registry" element={<SchemaRegistry />} />
            <Route path="/external-clusters" element={<ExternalClusters />} />
          </Route>
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
