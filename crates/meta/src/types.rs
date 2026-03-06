use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Account
// ---------------------------------------------------------------------------

/// Organizational account that owns tenants and users.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub account_id: u64,
    pub name: String,
    /// Nanos since epoch.
    pub created_at: i64,
}

// ---------------------------------------------------------------------------
// User
// ---------------------------------------------------------------------------

/// A user with credentials and account memberships.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub user_id: u64,
    pub username: String,
    /// Argon2 password hash.
    pub password_hash: Vec<u8>,
    /// Nanos since epoch.
    pub created_at: i64,
    pub disabled: bool,
}

/// A user's membership and role within an account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountMembership {
    pub user_id: u64,
    pub account_id: u64,
    pub role: AccountRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountRole {
    /// Can manage everything in the account (all tenants, users, keys).
    Admin,
    /// Access only to specifically granted tenants/catalogs.
    Member,
}

impl fmt::Display for AccountRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AccountRole::Admin => write!(f, "admin"),
            AccountRole::Member => write!(f, "member"),
        }
    }
}

/// Grants a member-role user access to a specific tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantGrant {
    pub user_id: u64,
    pub tenant_id: u64,
    pub catalog_access: CatalogAccess,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CatalogAccess {
    /// Full access to all catalogs in the tenant.
    All,
    /// Access to specific catalogs with per-catalog permissions.
    Specific(Vec<CatalogGrant>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogGrant {
    pub catalog_name: String,
    pub read_only: bool,
}

// ---------------------------------------------------------------------------
// Tenant
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenant {
    pub tenant_id: u64,
    /// Owning account.
    pub account_id: u64,
    pub name: String,
    pub limits: TenantLimits,
    /// API key IDs owned by this tenant (not the full ApiKey — avoids duplication).
    pub api_keys: Vec<u64>,
    /// Nanos since epoch.
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TenantLimits {
    pub max_disk_bytes: u64,
    pub max_deep_storage_bytes: u64,
    pub max_concurrent_queries: u32,
    pub max_query_memory_bytes: u64,
    pub max_catalogs: u32,
}

impl Default for TenantLimits {
    fn default() -> Self {
        Self {
            max_disk_bytes: 100 * 1024 * 1024 * 1024,        // 100 GiB
            max_deep_storage_bytes: 1024 * 1024 * 1024 * 1024, // 1 TiB
            max_concurrent_queries: 64,
            max_query_memory_bytes: 8 * 1024 * 1024 * 1024,   // 8 GiB
            max_catalogs: 64,
        }
    }
}

// ---------------------------------------------------------------------------
// Catalog
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EngineType {
    Lance,
    Mq,
    LibSql,
}

impl fmt::Display for EngineType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineType::Lance => write!(f, "lance"),
            EngineType::Mq => write!(f, "mq"),
            EngineType::LibSql => write!(f, "libsql"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatalogStatus {
    Creating,
    Active,
    Deleting,
    Deleted,
}

impl fmt::Display for CatalogStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CatalogStatus::Creating => write!(f, "creating"),
            CatalogStatus::Active => write!(f, "active"),
            CatalogStatus::Deleting => write!(f, "deleting"),
            CatalogStatus::Deleted => write!(f, "deleted"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub catalog_id: u64,
    pub tenant_id: u64,
    pub name: String,
    pub engine: EngineType,
    pub raft_group_id: u64,
    pub placement: Vec<u64>,
    /// Engine-specific configuration (stored as JSON string for bincode compatibility).
    pub config: String,
    pub status: CatalogStatus,
    /// Nanos since epoch.
    pub created_at: i64,
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingEntry {
    pub catalog_id: u64,
    pub raft_group_id: u64,
    pub leader_node_id: Option<u64>,
    pub node_addresses: HashMap<u64, String>,
}

// ---------------------------------------------------------------------------
// API Keys / Security
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    pub key_id: u64,
    pub tenant_id: u64,
    pub key_hash: Vec<u8>,
    pub scopes: Vec<Scope>,
    /// Nanos since epoch.
    pub created_at: i64,
    /// Optional expiry (nanos since epoch).
    pub expires_at: Option<i64>,
    pub revoked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Scope {
    /// Global admin across all accounts and tenants.
    SuperAdmin,
    /// Admin for a specific account (sees all tenants in that account).
    AccountAdmin(u64),
    /// Full access to a specific catalog.
    Catalog(String),
    /// Read-only access to a specific catalog.
    CatalogRead(String),
    /// Admin access to the entire tenant.
    TenantAdmin,
}

// ---------------------------------------------------------------------------
// Usage
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageReport {
    pub catalog_id: u64,
    pub disk_bytes: u64,
    pub deep_storage_bytes: u64,
}

// ---------------------------------------------------------------------------
// Commands & Responses
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaCommand {
    // Account CRUD
    CreateAccount {
        name: String,
    },
    DeleteAccount {
        account_id: u64,
    },

    // User CRUD
    CreateUser {
        username: String,
        password_hash: Vec<u8>,
    },
    DisableUser {
        user_id: u64,
    },
    UpdatePassword {
        user_id: u64,
        password_hash: Vec<u8>,
    },

    // Account membership
    AddAccountMembership {
        user_id: u64,
        account_id: u64,
        role: AccountRole,
    },
    RemoveAccountMembership {
        user_id: u64,
        account_id: u64,
    },
    SetAccountRole {
        user_id: u64,
        account_id: u64,
        role: AccountRole,
    },

    // Tenant grants
    GrantTenantAccess {
        user_id: u64,
        tenant_id: u64,
        catalog_access: CatalogAccess,
    },
    RevokeTenantAccess {
        user_id: u64,
        tenant_id: u64,
    },

    // Tenant CRUD
    CreateTenant {
        account_id: u64,
        name: String,
        limits: TenantLimits,
    },
    UpdateTenantLimits {
        tenant_id: u64,
        limits: TenantLimits,
    },
    DeleteTenant {
        tenant_id: u64,
    },

    // Catalog lifecycle
    CreateCatalog {
        tenant_id: u64,
        name: String,
        engine: EngineType,
        config: String,
    },
    UpdateCatalogStatus {
        catalog_id: u64,
        status: CatalogStatus,
    },
    UpdateCatalogPlacement {
        catalog_id: u64,
        placement: Vec<u64>,
    },
    DeleteCatalog {
        tenant_id: u64,
        catalog_id: u64,
    },

    // Routing (from engine nodes reporting leadership changes)
    UpdateLeader {
        raft_group_id: u64,
        leader_node_id: Option<u64>,
    },

    // Security
    CreateApiKey {
        tenant_id: u64,
        scopes: Vec<Scope>,
    },
    RevokeApiKey {
        key_id: u64,
    },

    // Usage reporting
    ReportUsage {
        catalog_id: u64,
        disk_bytes: u64,
        deep_storage_bytes: u64,
    },
}

impl fmt::Display for MetaCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetaCommand::CreateAccount { name } => write!(f, "CreateAccount({name})"),
            MetaCommand::DeleteAccount { account_id } => {
                write!(f, "DeleteAccount({account_id})")
            }
            MetaCommand::CreateUser { username, .. } => write!(f, "CreateUser({username})"),
            MetaCommand::DisableUser { user_id } => write!(f, "DisableUser({user_id})"),
            MetaCommand::UpdatePassword { user_id, .. } => {
                write!(f, "UpdatePassword({user_id})")
            }
            MetaCommand::AddAccountMembership {
                user_id,
                account_id,
                role,
            } => write!(f, "AddAccountMembership({user_id}, {account_id}, {role})"),
            MetaCommand::RemoveAccountMembership {
                user_id,
                account_id,
            } => write!(f, "RemoveAccountMembership({user_id}, {account_id})"),
            MetaCommand::SetAccountRole {
                user_id,
                account_id,
                role,
            } => write!(f, "SetAccountRole({user_id}, {account_id}, {role})"),
            MetaCommand::GrantTenantAccess {
                user_id,
                tenant_id,
                ..
            } => write!(f, "GrantTenantAccess({user_id}, {tenant_id})"),
            MetaCommand::RevokeTenantAccess {
                user_id,
                tenant_id,
            } => write!(f, "RevokeTenantAccess({user_id}, {tenant_id})"),
            MetaCommand::CreateTenant { name, .. } => write!(f, "CreateTenant({name})"),
            MetaCommand::UpdateTenantLimits { tenant_id, .. } => {
                write!(f, "UpdateTenantLimits({tenant_id})")
            }
            MetaCommand::DeleteTenant { tenant_id } => write!(f, "DeleteTenant({tenant_id})"),
            MetaCommand::CreateCatalog {
                tenant_id,
                name,
                engine,
                ..
            } => write!(f, "CreateCatalog({tenant_id}, {name}, {engine})"),
            MetaCommand::UpdateCatalogStatus {
                catalog_id, status, ..
            } => write!(f, "UpdateCatalogStatus({catalog_id}, {status})"),
            MetaCommand::UpdateCatalogPlacement { catalog_id, .. } => {
                write!(f, "UpdateCatalogPlacement({catalog_id})")
            }
            MetaCommand::DeleteCatalog {
                tenant_id,
                catalog_id,
            } => write!(f, "DeleteCatalog({tenant_id}, {catalog_id})"),
            MetaCommand::UpdateLeader {
                raft_group_id,
                leader_node_id,
            } => write!(f, "UpdateLeader({raft_group_id}, {leader_node_id:?})"),
            MetaCommand::CreateApiKey { tenant_id, .. } => {
                write!(f, "CreateApiKey({tenant_id})")
            }
            MetaCommand::RevokeApiKey { key_id } => write!(f, "RevokeApiKey({key_id})"),
            MetaCommand::ReportUsage { catalog_id, .. } => {
                write!(f, "ReportUsage({catalog_id})")
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaResponse {
    Ok,
    AccountCreated { account_id: u64 },
    UserCreated { user_id: u64 },
    TenantCreated { tenant_id: u64 },
    CatalogCreated { catalog_id: u64, raft_group_id: u64 },
    ApiKeyCreated { key_id: u64, raw_key: String },
    Error(String),
}

impl fmt::Display for MetaResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetaResponse::Ok => write!(f, "Ok"),
            MetaResponse::AccountCreated { account_id } => {
                write!(f, "AccountCreated({account_id})")
            }
            MetaResponse::UserCreated { user_id } => {
                write!(f, "UserCreated({user_id})")
            }
            MetaResponse::TenantCreated { tenant_id } => {
                write!(f, "TenantCreated({tenant_id})")
            }
            MetaResponse::CatalogCreated {
                catalog_id,
                raft_group_id,
            } => write!(f, "CatalogCreated({catalog_id}, group={raft_group_id})"),
            MetaResponse::ApiKeyCreated { key_id, .. } => {
                write!(f, "ApiKeyCreated({key_id})")
            }
            MetaResponse::Error(e) => write!(f, "Error({e})"),
        }
    }
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaSnapshotData {
    pub accounts: HashMap<u64, Account>,
    pub users: HashMap<u64, User>,
    pub memberships: Vec<AccountMembership>,
    pub tenant_grants: Vec<TenantGrant>,
    pub tenants: HashMap<u64, Tenant>,
    pub catalogs: HashMap<u64, CatalogEntry>,
    pub api_keys: HashMap<u64, ApiKey>,
    pub routing: HashMap<u64, RoutingEntry>,
    pub usage: HashMap<u64, UsageReport>,
    pub next_account_id: u64,
    pub next_user_id: u64,
    pub next_tenant_id: u64,
    pub next_catalog_id: u64,
    pub next_key_id: u64,
    pub next_raft_group_id: u64,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn bincode_roundtrip<T: Serialize + for<'de> Deserialize<'de> + fmt::Debug>(val: &T) -> T {
        let encoded = bincode::serde::encode_to_vec(val, bincode::config::standard()).unwrap();
        let (decoded, _): (T, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        decoded
    }

    #[test]
    fn test_meta_command_bincode_roundtrip() {
        let cmds = vec![
            MetaCommand::CreateTenant {
                account_id: 1,
                name: "acme".into(),
                limits: TenantLimits::default(),
            },
            MetaCommand::UpdateTenantLimits {
                tenant_id: 1,
                limits: TenantLimits::default(),
            },
            MetaCommand::DeleteTenant { tenant_id: 1 },
            MetaCommand::CreateCatalog {
                tenant_id: 1,
                name: "analytics".into(),
                engine: EngineType::Lance,
                config: r#"{"key":"value"}"#.into(),
            },
            MetaCommand::UpdateCatalogStatus {
                catalog_id: 1,
                status: CatalogStatus::Active,
            },
            MetaCommand::UpdateCatalogPlacement {
                catalog_id: 1,
                placement: vec![1, 2, 3],
            },
            MetaCommand::DeleteCatalog {
                tenant_id: 1,
                catalog_id: 1,
            },
            MetaCommand::UpdateLeader {
                raft_group_id: 100,
                leader_node_id: Some(1),
            },
            MetaCommand::CreateApiKey {
                tenant_id: 1,
                scopes: vec![Scope::TenantAdmin, Scope::Catalog("analytics".into())],
            },
            MetaCommand::RevokeApiKey { key_id: 42 },
            MetaCommand::ReportUsage {
                catalog_id: 1,
                disk_bytes: 1024,
                deep_storage_bytes: 4096,
            },
        ];

        for cmd in &cmds {
            let decoded = bincode_roundtrip(cmd);
            assert_eq!(format!("{cmd}"), format!("{decoded}"));
        }
    }

    #[test]
    fn test_meta_response_bincode_roundtrip() {
        let resps = vec![
            MetaResponse::Ok,
            MetaResponse::TenantCreated { tenant_id: 42 },
            MetaResponse::CatalogCreated {
                catalog_id: 1,
                raft_group_id: 100,
            },
            MetaResponse::ApiKeyCreated {
                key_id: 7,
                raw_key: "secret123".into(),
            },
            MetaResponse::Error("something went wrong".into()),
        ];

        for resp in &resps {
            let decoded = bincode_roundtrip(resp);
            assert_eq!(format!("{resp}"), format!("{decoded}"));
        }
    }

    #[test]
    fn test_snapshot_data_bincode_roundtrip() {
        let data = MetaSnapshotData {
            accounts: HashMap::from([(
                1,
                Account {
                    account_id: 1,
                    name: "acme-org".into(),
                    created_at: 1000,
                },
            )]),
            users: HashMap::new(),
            memberships: vec![],
            tenant_grants: vec![],
            tenants: HashMap::from([(
                1,
                Tenant {
                    tenant_id: 1,
                    account_id: 1,
                    name: "acme".into(),
                    limits: TenantLimits::default(),
                    api_keys: vec![1, 2],
                    created_at: 1000,
                },
            )]),
            catalogs: HashMap::from([(
                1,
                CatalogEntry {
                    catalog_id: 1,
                    tenant_id: 1,
                    name: "analytics".into(),
                    engine: EngineType::Lance,
                    raft_group_id: 100,
                    placement: vec![1, 2, 3],
                    config: "{}".into(),
                    status: CatalogStatus::Active,
                    created_at: 1000,
                },
            )]),
            api_keys: HashMap::new(),
            routing: HashMap::new(),
            usage: HashMap::new(),
            next_account_id: 2,
            next_user_id: 1,
            next_tenant_id: 2,
            next_catalog_id: 2,
            next_key_id: 1,
            next_raft_group_id: 101,
        };

        let decoded = bincode_roundtrip(&data);
        assert_eq!(decoded.tenants.len(), 1);
        assert_eq!(decoded.catalogs.len(), 1);
        assert_eq!(decoded.next_tenant_id, 2);
        assert_eq!(decoded.next_raft_group_id, 101);
    }

    #[test]
    fn test_meta_command_display() {
        let cmd = MetaCommand::CreateCatalog {
            tenant_id: 1,
            name: "events".into(),
            engine: EngineType::Mq,
            config: "{}".into(),
        };
        assert_eq!(format!("{cmd}"), "CreateCatalog(1, events, mq)");
    }

    #[test]
    fn test_meta_command_display_all_variants() {
        let displays = vec![
            (
                MetaCommand::CreateTenant {
                    account_id: 1,
                    name: "acme".into(),
                    limits: TenantLimits::default(),
                },
                "CreateTenant(acme)",
            ),
            (
                MetaCommand::UpdateTenantLimits {
                    tenant_id: 5,
                    limits: TenantLimits::default(),
                },
                "UpdateTenantLimits(5)",
            ),
            (
                MetaCommand::DeleteTenant { tenant_id: 3 },
                "DeleteTenant(3)",
            ),
            (
                MetaCommand::CreateCatalog {
                    tenant_id: 1,
                    name: "db".into(),
                    engine: EngineType::LibSql,
                    config: "{}".into(),
                },
                "CreateCatalog(1, db, libsql)",
            ),
            (
                MetaCommand::UpdateCatalogStatus {
                    catalog_id: 7,
                    status: CatalogStatus::Active,
                },
                "UpdateCatalogStatus(7, active)",
            ),
            (
                MetaCommand::UpdateCatalogPlacement {
                    catalog_id: 2,
                    placement: vec![1, 2],
                },
                "UpdateCatalogPlacement(2)",
            ),
            (
                MetaCommand::DeleteCatalog {
                    tenant_id: 1,
                    catalog_id: 3,
                },
                "DeleteCatalog(1, 3)",
            ),
            (
                MetaCommand::UpdateLeader {
                    raft_group_id: 10,
                    leader_node_id: Some(5),
                },
                "UpdateLeader(10, Some(5))",
            ),
            (
                MetaCommand::UpdateLeader {
                    raft_group_id: 10,
                    leader_node_id: None,
                },
                "UpdateLeader(10, None)",
            ),
            (
                MetaCommand::CreateApiKey {
                    tenant_id: 2,
                    scopes: vec![Scope::TenantAdmin],
                },
                "CreateApiKey(2)",
            ),
            (
                MetaCommand::RevokeApiKey { key_id: 99 },
                "RevokeApiKey(99)",
            ),
            (
                MetaCommand::ReportUsage {
                    catalog_id: 4,
                    disk_bytes: 100,
                    deep_storage_bytes: 200,
                },
                "ReportUsage(4)",
            ),
        ];

        for (cmd, expected) in displays {
            assert_eq!(format!("{cmd}"), expected, "Display mismatch for {cmd:?}");
        }
    }

    #[test]
    fn test_meta_response_display_all_variants() {
        assert_eq!(format!("{}", MetaResponse::Ok), "Ok");
        assert_eq!(
            format!("{}", MetaResponse::TenantCreated { tenant_id: 5 }),
            "TenantCreated(5)"
        );
        assert_eq!(
            format!(
                "{}",
                MetaResponse::CatalogCreated {
                    catalog_id: 1,
                    raft_group_id: 100,
                }
            ),
            "CatalogCreated(1, group=100)"
        );
        assert_eq!(
            format!(
                "{}",
                MetaResponse::ApiKeyCreated {
                    key_id: 7,
                    raw_key: "secret".into(),
                }
            ),
            "ApiKeyCreated(7)"
        );
        assert_eq!(
            format!("{}", MetaResponse::Error("oops".into())),
            "Error(oops)"
        );
    }

    #[test]
    fn test_engine_type_display() {
        assert_eq!(format!("{}", EngineType::Lance), "lance");
        assert_eq!(format!("{}", EngineType::Mq), "mq");
        assert_eq!(format!("{}", EngineType::LibSql), "libsql");
    }

    #[test]
    fn test_catalog_status_display() {
        assert_eq!(format!("{}", CatalogStatus::Creating), "creating");
        assert_eq!(format!("{}", CatalogStatus::Active), "active");
        assert_eq!(format!("{}", CatalogStatus::Deleting), "deleting");
        assert_eq!(format!("{}", CatalogStatus::Deleted), "deleted");
    }

    #[test]
    fn test_tenant_limits_default() {
        let limits = TenantLimits::default();
        assert_eq!(limits.max_disk_bytes, 100 * 1024 * 1024 * 1024);
        assert_eq!(limits.max_deep_storage_bytes, 1024 * 1024 * 1024 * 1024);
        assert_eq!(limits.max_concurrent_queries, 64);
        assert_eq!(limits.max_query_memory_bytes, 8 * 1024 * 1024 * 1024);
        assert_eq!(limits.max_catalogs, 64);
    }

    #[test]
    fn test_tenant_limits_equality() {
        let a = TenantLimits::default();
        let b = TenantLimits::default();
        assert_eq!(a, b);

        let c = TenantLimits {
            max_catalogs: 128,
            ..TenantLimits::default()
        };
        assert_ne!(a, c);
    }

    #[test]
    fn test_scope_equality() {
        assert_eq!(Scope::TenantAdmin, Scope::TenantAdmin);
        assert_eq!(Scope::Catalog("x".into()), Scope::Catalog("x".into()));
        assert_ne!(Scope::Catalog("x".into()), Scope::CatalogRead("x".into()));
        assert_ne!(Scope::Catalog("x".into()), Scope::Catalog("y".into()));
    }

    #[test]
    fn test_snapshot_data_empty_roundtrip() {
        let data = MetaSnapshotData {
            accounts: HashMap::new(),
            users: HashMap::new(),
            memberships: vec![],
            tenant_grants: vec![],
            tenants: HashMap::new(),
            catalogs: HashMap::new(),
            api_keys: HashMap::new(),
            routing: HashMap::new(),
            usage: HashMap::new(),
            next_account_id: 1,
            next_user_id: 1,
            next_tenant_id: 1,
            next_catalog_id: 1,
            next_key_id: 1,
            next_raft_group_id: 1,
        };
        let decoded = bincode_roundtrip(&data);
        assert!(decoded.tenants.is_empty());
        assert_eq!(decoded.next_tenant_id, 1);
    }
}
