import logging
import os
import ssl
import tempfile

from ldap3 import Server, Connection, ALL, SUBTREE, Tls

logger = logging.getLogger("tantor.ldap")


class LdapService:
    """Handles LDAP/AD authentication and user search operations."""

    @staticmethod
    def _build_tls(config) -> Tls | None:
        """Build a Tls object honoring tls_validate_cert + optional tls_ca_cert."""
        if not config.use_ssl:
            return None

        # Existing rows from before this column was added will read None for
        # tls_validate_cert; treat that as the secure default.
        validate_cert = getattr(config, "tls_validate_cert", True)
        if validate_cert is None:
            validate_cert = True
        ca_cert_pem = getattr(config, "tls_ca_cert", None)

        if not validate_cert:
            logger.warning(
                "LDAPS server-cert validation is DISABLED for %s — vulnerable to MITM. "
                "Configure tls_ca_cert and re-enable validation in production.",
                config.server_url,
            )
            return Tls(validate=ssl.CERT_NONE)

        if ca_cert_pem:
            # ldap3's Tls reads the CA from a file path, so spill the PEM to a
            # restricted temp file. The OS cleans these up on reboot, and we
            # rewrite on every config save so stale files are harmless.
            fd, path = tempfile.mkstemp(prefix="tantor-ldap-ca-", suffix=".pem")
            with os.fdopen(fd, "w") as f:
                f.write(ca_cert_pem)
            os.chmod(path, 0o600)
            return Tls(validate=ssl.CERT_REQUIRED, ca_certs_file=path)

        # CERT_REQUIRED with the system trust store (works for public CAs).
        return Tls(validate=ssl.CERT_REQUIRED)

    @staticmethod
    def _create_server(config) -> Server:
        """Create an ldap3 Server object from config."""
        tls = LdapService._build_tls(config)
        return Server(
            config.server_url,
            use_ssl=config.use_ssl,
            tls=tls,
            get_info=ALL,
            connect_timeout=config.connection_timeout,
        )

    @staticmethod
    def test_connection(config, bind_password: str) -> dict:
        """Test connectivity by binding with the service account."""
        try:
            server = LdapService._create_server(config)
            conn = Connection(
                server,
                user=config.bind_dn,
                password=bind_password,
                auto_bind=True,
                read_only=True,
                receive_timeout=config.connection_timeout,
            )
            server_info = str(conn.server.info) if conn.server.info else "Connected"
            conn.unbind()
            return {"success": True, "message": f"Successfully connected to {config.server_url}. {server_info[:200]}"}
        except Exception as e:
            logger.error(f"LDAP connection test failed: {e}")
            return {"success": False, "message": f"Connection failed: {str(e)}"}

    @staticmethod
    def authenticate(username: str, password: str, config, bind_password: str) -> dict | None:
        """
        Authenticate a user via LDAP.

        1. Connect with bind DN (service account)
        2. Search for user by search filter
        3. Re-bind as found user DN + their password
        4. Get groups (memberOf for AD, or search group entries for OpenLDAP)
        5. Return user info dict or None
        """
        try:
            server = LdapService._create_server(config)

            # Step 1: Bind with service account
            service_conn = Connection(
                server,
                user=config.bind_dn,
                password=bind_password,
                auto_bind=True,
                read_only=True,
                receive_timeout=config.connection_timeout,
            )

            # Step 2: Search for user
            search_filter = config.user_search_filter.replace("{username}", username)
            # Use '*' to fetch all attributes — safe for both AD and OpenLDAP
            service_conn.search(
                search_base=config.user_search_base,
                search_filter=search_filter,
                search_scope=SUBTREE,
                attributes=["*"],
            )

            if not service_conn.entries:
                logger.info(f"LDAP user not found: {username}")
                service_conn.unbind()
                return None

            user_entry = service_conn.entries[0]
            user_dn = str(user_entry.entry_dn)

            # Extract display name
            display_name = username
            if hasattr(user_entry, "displayName") and user_entry.displayName.value:
                display_name = str(user_entry.displayName.value)
            elif hasattr(user_entry, "cn") and user_entry.cn.value:
                display_name = str(user_entry.cn.value)

            # Extract groups from memberOf (works for AD and OpenLDAP with memberOf overlay)
            groups = []
            if hasattr(user_entry, "memberOf") and user_entry.memberOf.values:
                groups = [str(g) for g in user_entry.memberOf.values]

            service_conn.unbind()

            # Step 3: Re-bind as the user to verify their password
            try:
                user_conn = Connection(
                    server,
                    user=user_dn,
                    password=password,
                    auto_bind=True,
                    read_only=True,
                    receive_timeout=config.connection_timeout,
                )
                user_conn.unbind()
            except Exception:
                logger.info(f"LDAP password verification failed for: {username}")
                return None

            # Step 4: If no groups from memberOf, try searching group entries (OpenLDAP style)
            if not groups and config.group_search_base:
                try:
                    group_conn = Connection(
                        server,
                        user=config.bind_dn,
                        password=bind_password,
                        auto_bind=True,
                        read_only=True,
                        receive_timeout=config.connection_timeout,
                    )
                    group_conn.search(
                        search_base=config.group_search_base,
                        search_filter=f"(|(member={user_dn})(uniqueMember={user_dn}))",
                        search_scope=SUBTREE,
                        attributes=["cn"],
                    )
                    groups = [str(entry.entry_dn) for entry in group_conn.entries]
                    group_conn.unbind()
                except Exception as e:
                    logger.warning(f"LDAP group search failed: {e}")

            return {
                "dn": user_dn,
                "groups": groups,
                "display_name": display_name,
                "username": username,
            }

        except Exception as e:
            logger.error(f"LDAP authentication error for {username}: {e}")
            return None

    @staticmethod
    def determine_role(groups: list[str], config) -> str:
        """Determine user role based on group membership."""
        if config.admin_group_dn:
            # Case-insensitive comparison for AD compatibility
            admin_dn_lower = config.admin_group_dn.lower()
            for group in groups:
                if group.lower() == admin_dn_lower:
                    return "admin"

        if config.monitor_group_dn:
            monitor_dn_lower = config.monitor_group_dn.lower()
            for group in groups:
                if group.lower() == monitor_dn_lower:
                    return "monitor"

        return config.default_role or "monitor"

    @staticmethod
    def search_users(config, bind_password: str, search_filter: str = "*") -> list:
        """Search LDAP directory for users matching a filter."""
        try:
            server = LdapService._create_server(config)
            conn = Connection(
                server,
                user=config.bind_dn,
                password=bind_password,
                auto_bind=True,
                read_only=True,
                receive_timeout=config.connection_timeout,
            )

            # Build the search filter
            if search_filter == "*":
                ldap_filter = config.user_search_filter.replace("{username}", "*")
            else:
                ldap_filter = config.user_search_filter.replace("{username}", f"*{search_filter}*")

            conn.search(
                search_base=config.user_search_base,
                search_filter=ldap_filter,
                search_scope=SUBTREE,
                attributes=["*"],
                size_limit=100,
            )

            users = []
            for entry in conn.entries:
                username = None
                if hasattr(entry, "sAMAccountName") and entry.sAMAccountName.value:
                    username = str(entry.sAMAccountName.value)
                elif hasattr(entry, "uid") and entry.uid.value:
                    username = str(entry.uid.value)
                elif hasattr(entry, "cn") and entry.cn.value:
                    username = str(entry.cn.value)

                display_name = username
                if hasattr(entry, "displayName") and entry.displayName.value:
                    display_name = str(entry.displayName.value)
                elif hasattr(entry, "cn") and entry.cn.value:
                    display_name = str(entry.cn.value)

                if username:
                    users.append({
                        "dn": str(entry.entry_dn),
                        "username": username,
                        "display_name": display_name,
                    })

            conn.unbind()
            return users

        except Exception as e:
            logger.error(f"LDAP user search failed: {e}")
            return []
