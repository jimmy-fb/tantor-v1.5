import { useState, useEffect } from 'react';
import { NavLink, useNavigate } from 'react-router-dom';
import { LayoutDashboard, Server, Network, Plus, Package, Users, BarChart3, LogOut, User, Shield, Link2, KeyRound, Activity, Bell } from 'lucide-react';
import { isAdmin, getUsername, clearTokens } from '../../lib/auth';
import { getHealthInfo } from '../../lib/api';

export default function Sidebar() {
  const navigate = useNavigate();
  const [version, setVersion] = useState('1.0.0');
  const username = getUsername();
  const admin = isAdmin();

  useEffect(() => {
    getHealthInfo().then(d => setVersion(d.version)).catch(() => {});
  }, []);

  const handleLogout = () => {
    clearTokens();
    navigate('/login', { replace: true });
  };

  const navItems = [
    { to: '/', icon: LayoutDashboard, label: 'Dashboard' },
    { to: '/hosts', icon: Server, label: 'Hosts' },
    { to: '/clusters', icon: Network, label: 'Clusters' },
    { to: '/clusters/new', icon: Plus, label: 'New Cluster', adminOnly: true },
    { to: '/versions', icon: Package, label: 'Kafka Versions' },
    { to: '/monitoring', icon: BarChart3, label: 'Monitoring' },

    { to: '/security-scan', icon: Shield, label: 'Security Scan' },
    { to: '/cluster-linking', icon: Link2, label: 'Cluster Linking' },
    { to: '/activity', icon: Activity, label: 'Activity' },
    { to: '/alerts', icon: Bell, label: 'Alerts' },
    ...(admin ? [
      { to: '/users', icon: Users, label: 'Users', adminOnly: true },
      { to: '/ldap-settings', icon: KeyRound, label: 'LDAP / AD', adminOnly: true },
    ] : []),
  ];

  return (
    <aside className="w-64 bg-gray-900 text-white flex flex-col h-screen overflow-y-auto">
      <div className="p-6 border-b border-gray-800">
        <img src="/tantor-logo.png" alt="Tantor" className="h-8" />
        <p className="text-xs text-gray-400 mt-1">Kafka Cluster Manager</p>
      </div>

      <nav className="flex-1 p-4 space-y-1">
        {navItems.map(({ to, icon: Icon, label }) => (
          <NavLink
            key={to}
            to={to}
            end={to === '/'}
            className={({ isActive }) =>
              `flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm transition-colors ${
                isActive
                  ? 'bg-blue-600 text-white'
                  : 'text-gray-300 hover:bg-gray-800 hover:text-white'
              }`
            }
          >
            <Icon size={18} />
            {label}
          </NavLink>
        ))}
      </nav>

      {/* User info + logout */}
      <div className="p-4 border-t border-gray-800 space-y-3">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 bg-gray-700 rounded-full flex items-center justify-center">
            <User size={14} className="text-gray-300" />
          </div>
          <div className="flex-1 min-w-0">
            <p className="text-sm font-medium text-gray-200 truncate">{username}</p>
            <p className="text-xs text-gray-500 capitalize">{admin ? 'Admin' : 'Monitor'}</p>
          </div>
          <button
            onClick={handleLogout}
            className="p-1.5 text-gray-500 hover:text-red-400 rounded transition-colors"
            title="Sign out"
          >
            <LogOut size={16} />
          </button>
        </div>
        <div className="text-xs text-gray-600">
          v{version}
        </div>
      </div>
    </aside>
  );
}
