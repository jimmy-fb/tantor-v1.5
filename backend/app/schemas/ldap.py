from pydantic import BaseModel


class LdapConfigCreate(BaseModel):
    enabled: bool = False
    server_url: str
    use_ssl: bool = False
    tls_validate_cert: bool = True
    tls_ca_cert: str | None = None
    bind_dn: str
    bind_password: str
    user_search_base: str
    user_search_filter: str = "(sAMAccountName={username})"
    group_search_base: str | None = None
    admin_group_dn: str | None = None
    monitor_group_dn: str | None = None
    default_role: str = "monitor"
    connection_timeout: int = 10


class LdapConfigUpdate(BaseModel):
    enabled: bool | None = None
    server_url: str | None = None
    use_ssl: bool | None = None
    tls_validate_cert: bool | None = None
    tls_ca_cert: str | None = None
    bind_dn: str | None = None
    bind_password: str | None = None
    user_search_base: str | None = None
    user_search_filter: str | None = None
    group_search_base: str | None = None
    admin_group_dn: str | None = None
    monitor_group_dn: str | None = None
    default_role: str | None = None
    connection_timeout: int | None = None


class LdapConfigResponse(BaseModel):
    id: str
    enabled: bool
    server_url: str | None
    use_ssl: bool
    tls_validate_cert: bool
    # Whether a CA cert is configured. The PEM body itself is not returned.
    tls_ca_cert_present: bool = False
    bind_dn: str | None
    user_search_base: str | None
    user_search_filter: str
    group_search_base: str | None
    admin_group_dn: str | None
    monitor_group_dn: str | None
    default_role: str
    connection_timeout: int

    model_config = {"from_attributes": True}


class LdapTestRequest(BaseModel):
    username: str
    password: str


class LdapTestResponse(BaseModel):
    success: bool
    message: str
    user_dn: str | None = None
    groups: list[str] = []
