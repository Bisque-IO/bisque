//! Control plane state with optional MDBX persistence.
//!
//! `MetaEngine` holds the registries of tenants, catalogs, API keys, and routing
//! entries that the state machine reads and mutates. Analogous to `BisqueLance`
//! in the lance crate.
//!
//! When opened with a data directory via [`MetaEngine::open`], state is persisted
//! to MDBX on every mutation. On restart, state is restored from MDBX automatically.

use std::collections::HashMap;
use std::path::Path;

use base64::Engine as _;
use libmdbx::{Database, DatabaseOptions, Mode, NoWriteMap, ReadWriteOptions, TableFlags, WriteFlags};
use parking_lot::RwLock;
use tracing::{info, warn};

use crate::config::MetaConfig;
use crate::error::{Error, Result};
use crate::token;
use crate::types::*;

const META_TABLE: &str = "bisque_meta";
const SNAPSHOT_KEY: &[u8] = b"snapshot";

pub struct MetaEngine {
    config: MetaConfig,
    accounts: RwLock<HashMap<u64, Account>>,
    users: RwLock<HashMap<u64, User>>,
    memberships: RwLock<Vec<AccountMembership>>,
    tenant_grants: RwLock<Vec<TenantGrant>>,
    tenants: RwLock<HashMap<u64, Tenant>>,
    catalogs: RwLock<HashMap<u64, CatalogEntry>>,
    api_keys: RwLock<HashMap<u64, ApiKey>>,
    routing: RwLock<HashMap<u64, RoutingEntry>>,
    usage: RwLock<HashMap<u64, UsageReport>>,
    next_account_id: RwLock<u64>,
    next_user_id: RwLock<u64>,
    next_tenant_id: RwLock<u64>,
    next_catalog_id: RwLock<u64>,
    next_key_id: RwLock<u64>,
    next_raft_group_id: RwLock<u64>,
    /// Optional MDBX database for persistent storage.
    db: Option<Database<NoWriteMap>>,
}

impl MetaEngine {
    /// Create an in-memory-only MetaEngine (no persistence). Used in tests.
    pub fn new(config: MetaConfig) -> Self {
        Self {
            config,
            accounts: RwLock::new(HashMap::new()),
            users: RwLock::new(HashMap::new()),
            memberships: RwLock::new(Vec::new()),
            tenant_grants: RwLock::new(Vec::new()),
            tenants: RwLock::new(HashMap::new()),
            catalogs: RwLock::new(HashMap::new()),
            api_keys: RwLock::new(HashMap::new()),
            routing: RwLock::new(HashMap::new()),
            usage: RwLock::new(HashMap::new()),
            next_account_id: RwLock::new(1),
            next_user_id: RwLock::new(1),
            next_tenant_id: RwLock::new(1),
            next_catalog_id: RwLock::new(1),
            next_key_id: RwLock::new(1),
            next_raft_group_id: RwLock::new(1),
            db: None,
        }
    }

    /// Open a persistent MetaEngine backed by MDBX at `data_dir`.
    ///
    /// If the directory contains existing data, state is restored from MDBX.
    /// All subsequent mutations are persisted to disk.
    pub fn open(config: MetaConfig, data_dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(data_dir)?;

        let db = Database::<NoWriteMap>::open_with_options(
            data_dir,
            DatabaseOptions {
                mode: Mode::ReadWrite(ReadWriteOptions {
                    sync_mode: libmdbx::SyncMode::Durable,
                    ..Default::default()
                }),
                max_tables: Some(4),
                ..Default::default()
            },
        )
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        // Ensure the table exists.
        {
            let txn = db
                .begin_rw_txn()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
            let _table = txn
                .create_table(Some(META_TABLE), TableFlags::empty())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
            txn.commit()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        }

        let engine = Self {
            config,
            accounts: RwLock::new(HashMap::new()),
            users: RwLock::new(HashMap::new()),
            memberships: RwLock::new(Vec::new()),
            tenant_grants: RwLock::new(Vec::new()),
            tenants: RwLock::new(HashMap::new()),
            catalogs: RwLock::new(HashMap::new()),
            api_keys: RwLock::new(HashMap::new()),
            routing: RwLock::new(HashMap::new()),
            usage: RwLock::new(HashMap::new()),
            next_account_id: RwLock::new(1),
            next_user_id: RwLock::new(1),
            next_tenant_id: RwLock::new(1),
            next_catalog_id: RwLock::new(1),
            next_key_id: RwLock::new(1),
            next_raft_group_id: RwLock::new(1),
            db: Some(db),
        };

        // Restore from MDBX if data exists.
        if let Some(snapshot) = engine.load_snapshot()? {
            engine.restore_from_snapshot(snapshot);
            info!("meta engine restored from MDBX persistence");
        } else {
            info!("meta engine opened with empty state (new database)");
        }

        Ok(engine)
    }

    /// Load persisted snapshot from MDBX, if any.
    fn load_snapshot(&self) -> std::io::Result<Option<MetaSnapshotData>> {
        let db = match &self.db {
            Some(db) => db,
            None => return Ok(None),
        };

        let txn = db
            .begin_ro_txn()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        let table = txn
            .open_table(Some(META_TABLE))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        let bytes: Option<Vec<u8>> = txn
            .get(&table, SNAPSHOT_KEY)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        match bytes {
            Some(bytes) => {
                let (data, _): (MetaSnapshotData, _) =
                    bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                        .map_err(|e| {
                            std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                        })?;
                Ok(Some(data))
            }
            None => Ok(None),
        }
    }

    /// Persist current state to MDBX. No-op if no database is configured.
    fn persist(&self) {
        let db = match &self.db {
            Some(db) => db,
            None => return,
        };

        let snapshot = self.snapshot_data();
        let bytes = match bincode::serde::encode_to_vec(&snapshot, bincode::config::standard()) {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "failed to encode meta snapshot for persistence");
                return;
            }
        };

        let result = (|| -> std::result::Result<(), libmdbx::Error> {
            let txn = db.begin_rw_txn()?;
            let table = txn.open_table(Some(META_TABLE))?;
            txn.put(&table, SNAPSHOT_KEY, &bytes, WriteFlags::empty())?;
            drop(table);
            txn.commit()?;
            Ok(())
        })();

        if let Err(e) = result {
            warn!(error = %e, "failed to persist meta engine state to MDBX");
        }
    }

    pub fn config(&self) -> &MetaConfig {
        &self.config
    }

    // ── Account operations ─────────────────────────────────────────────

    pub fn create_account(&self, name: String) -> Result<u64> {
        let accounts = self.accounts.read();
        if accounts.values().any(|a| a.name == name) {
            return Err(Error::AlreadyExists(format!("account '{name}'")));
        }
        drop(accounts);

        let mut id_guard = self.next_account_id.write();
        let account_id = *id_guard;
        *id_guard += 1;
        drop(id_guard);

        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        self.accounts.write().insert(
            account_id,
            Account {
                account_id,
                name: name.clone(),
                created_at: now,
            },
        );
        info!(account_id, name = %name, "created account");
        self.persist();
        Ok(account_id)
    }

    pub fn get_account(&self, account_id: u64) -> Option<Account> {
        self.accounts.read().get(&account_id).cloned()
    }

    pub fn list_accounts(&self) -> Vec<Account> {
        self.accounts.read().values().cloned().collect()
    }

    pub fn delete_account(&self, account_id: u64) -> Result<()> {
        // Check no tenants still reference this account
        let tenants = self.tenants.read();
        let count = tenants
            .values()
            .filter(|t| t.account_id == account_id)
            .count();
        if count > 0 {
            return Err(Error::InvalidState(format!(
                "account {account_id} has {count} tenant(s)"
            )));
        }
        drop(tenants);

        self.accounts
            .write()
            .remove(&account_id)
            .ok_or(Error::NotFound(format!("account {account_id}")))?;
        info!(account_id, "deleted account");
        self.persist();
        Ok(())
    }

    // ── User operations ───────────────────────────────────────────────

    pub fn create_user(&self, username: String, password_hash: Vec<u8>) -> Result<u64> {
        let users = self.users.read();
        if users.values().any(|u| u.username == username) {
            return Err(Error::AlreadyExists(format!("user '{username}'")));
        }
        drop(users);

        let mut id_guard = self.next_user_id.write();
        let user_id = *id_guard;
        *id_guard += 1;
        drop(id_guard);

        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        self.users.write().insert(
            user_id,
            User {
                user_id,
                username: username.clone(),
                password_hash,
                created_at: now,
                disabled: false,
            },
        );
        info!(user_id, username = %username, "created user");
        self.persist();
        Ok(user_id)
    }

    pub fn get_user(&self, user_id: u64) -> Option<User> {
        self.users.read().get(&user_id).cloned()
    }

    pub fn find_user_by_username(&self, username: &str) -> Option<User> {
        self.users
            .read()
            .values()
            .find(|u| u.username == username)
            .cloned()
    }

    pub fn disable_user(&self, user_id: u64) -> Result<()> {
        let mut users = self.users.write();
        let user = users
            .get_mut(&user_id)
            .ok_or(Error::NotFound(format!("user {user_id}")))?;
        user.disabled = true;
        drop(users);
        info!(user_id, "disabled user");
        self.persist();
        Ok(())
    }

    pub fn update_password(&self, user_id: u64, password_hash: Vec<u8>) -> Result<()> {
        let mut users = self.users.write();
        let user = users
            .get_mut(&user_id)
            .ok_or(Error::NotFound(format!("user {user_id}")))?;
        user.password_hash = password_hash;
        drop(users);
        info!(user_id, "updated password");
        self.persist();
        Ok(())
    }

    /// Verify a user's password. Returns the User if verification succeeds.
    pub fn verify_user_password(&self, username: &str, password: &[u8]) -> Option<User> {
        let user = self.find_user_by_username(username)?;
        if user.disabled {
            return None;
        }
        if token::verify_password(&user.password_hash, password) {
            Some(user)
        } else {
            None
        }
    }

    // ── Membership operations ─────────────────────────────────────────

    pub fn add_membership(&self, user_id: u64, account_id: u64, role: AccountRole) -> Result<()> {
        if !self.users.read().contains_key(&user_id) {
            return Err(Error::NotFound(format!("user {user_id}")));
        }
        if !self.accounts.read().contains_key(&account_id) {
            return Err(Error::NotFound(format!("account {account_id}")));
        }

        let mut memberships = self.memberships.write();
        if memberships
            .iter()
            .any(|m| m.user_id == user_id && m.account_id == account_id)
        {
            return Err(Error::AlreadyExists(format!(
                "membership user={user_id} account={account_id}"
            )));
        }
        memberships.push(AccountMembership {
            user_id,
            account_id,
            role,
        });
        drop(memberships);
        info!(user_id, account_id, %role, "added account membership");
        self.persist();
        Ok(())
    }

    pub fn remove_membership(&self, user_id: u64, account_id: u64) -> Result<()> {
        let mut memberships = self.memberships.write();
        let len_before = memberships.len();
        memberships.retain(|m| !(m.user_id == user_id && m.account_id == account_id));
        if memberships.len() == len_before {
            return Err(Error::NotFound(format!(
                "membership user={user_id} account={account_id}"
            )));
        }
        drop(memberships);
        info!(user_id, account_id, "removed account membership");
        self.persist();
        Ok(())
    }

    pub fn set_account_role(&self, user_id: u64, account_id: u64, role: AccountRole) -> Result<()> {
        let mut memberships = self.memberships.write();
        let m = memberships
            .iter_mut()
            .find(|m| m.user_id == user_id && m.account_id == account_id)
            .ok_or(Error::NotFound(format!(
                "membership user={user_id} account={account_id}"
            )))?;
        m.role = role;
        drop(memberships);
        info!(user_id, account_id, %role, "set account role");
        self.persist();
        Ok(())
    }

    pub fn get_user_memberships(&self, user_id: u64) -> Vec<AccountMembership> {
        self.memberships
            .read()
            .iter()
            .filter(|m| m.user_id == user_id)
            .cloned()
            .collect()
    }

    pub fn get_account_members(&self, account_id: u64) -> Vec<(User, AccountRole)> {
        let memberships = self.memberships.read();
        let users = self.users.read();
        memberships
            .iter()
            .filter(|m| m.account_id == account_id)
            .filter_map(|m| users.get(&m.user_id).map(|u| (u.clone(), m.role)))
            .collect()
    }

    // ── Tenant grant operations ───────────────────────────────────────

    pub fn grant_tenant_access(
        &self,
        user_id: u64,
        tenant_id: u64,
        catalog_access: CatalogAccess,
    ) -> Result<()> {
        if !self.users.read().contains_key(&user_id) {
            return Err(Error::NotFound(format!("user {user_id}")));
        }
        if !self.tenants.read().contains_key(&tenant_id) {
            return Err(Error::TenantNotFound(tenant_id));
        }

        let mut grants = self.tenant_grants.write();
        // Replace existing grant for this user+tenant
        grants.retain(|g| !(g.user_id == user_id && g.tenant_id == tenant_id));
        grants.push(TenantGrant {
            user_id,
            tenant_id,
            catalog_access,
        });
        drop(grants);
        info!(user_id, tenant_id, "granted tenant access");
        self.persist();
        Ok(())
    }

    pub fn revoke_tenant_access(&self, user_id: u64, tenant_id: u64) -> Result<()> {
        let mut grants = self.tenant_grants.write();
        let len_before = grants.len();
        grants.retain(|g| !(g.user_id == user_id && g.tenant_id == tenant_id));
        if grants.len() == len_before {
            return Err(Error::NotFound(format!(
                "tenant grant user={user_id} tenant={tenant_id}"
            )));
        }
        drop(grants);
        info!(user_id, tenant_id, "revoked tenant access");
        self.persist();
        Ok(())
    }

    pub fn get_user_tenant_grants(&self, user_id: u64) -> Vec<TenantGrant> {
        self.tenant_grants
            .read()
            .iter()
            .filter(|g| g.user_id == user_id)
            .cloned()
            .collect()
    }

    /// Returns all tenants a user can access (via admin role or explicit grants).
    pub fn get_user_accessible_tenants(&self, user_id: u64) -> Vec<Tenant> {
        let memberships = self.memberships.read();
        let tenants = self.tenants.read();

        // Collect account IDs where user is admin
        let admin_accounts: Vec<u64> = memberships
            .iter()
            .filter(|m| m.user_id == user_id && m.role == AccountRole::Admin)
            .map(|m| m.account_id)
            .collect();

        // All tenants from admin accounts
        let mut result: Vec<Tenant> = tenants
            .values()
            .filter(|t| admin_accounts.contains(&t.account_id))
            .cloned()
            .collect();

        // Plus explicitly granted tenants
        let grants = self.tenant_grants.read();
        for grant in grants.iter().filter(|g| g.user_id == user_id) {
            if !result.iter().any(|t| t.tenant_id == grant.tenant_id) {
                if let Some(t) = tenants.get(&grant.tenant_id) {
                    result.push(t.clone());
                }
            }
        }

        result
    }

    // ── Tenant operations ──────────────────────────────────────────────

    pub fn create_tenant(
        &self,
        account_id: u64,
        name: String,
        limits: TenantLimits,
    ) -> Result<u64> {
        // Validate account exists
        if !self.accounts.read().contains_key(&account_id) {
            return Err(Error::NotFound(format!("account {account_id}")));
        }

        let tenants = self.tenants.read();
        if tenants.values().any(|t| t.name == name) {
            return Err(Error::TenantAlreadyExists(name));
        }
        drop(tenants);

        let mut id_guard = self.next_tenant_id.write();
        let tenant_id = *id_guard;
        *id_guard += 1;
        drop(id_guard);

        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let tenant = Tenant {
            tenant_id,
            account_id,
            name: name.clone(),
            limits,
            api_keys: Vec::new(),
            created_at: now,
        };

        self.tenants.write().insert(tenant_id, tenant);
        info!(tenant_id, account_id, name = %name, "created tenant");
        self.persist();
        Ok(tenant_id)
    }

    pub fn get_tenant(&self, tenant_id: u64) -> Option<Tenant> {
        self.tenants.read().get(&tenant_id).cloned()
    }

    pub fn find_tenant_by_name(&self, name: &str) -> Option<Tenant> {
        self.tenants
            .read()
            .values()
            .find(|t| t.name == name)
            .cloned()
    }

    pub fn update_tenant_limits(&self, tenant_id: u64, limits: TenantLimits) -> Result<()> {
        let mut tenants = self.tenants.write();
        let tenant = tenants
            .get_mut(&tenant_id)
            .ok_or(Error::TenantNotFound(tenant_id))?;
        tenant.limits = limits;
        drop(tenants);
        self.persist();
        Ok(())
    }

    pub fn delete_tenant(&self, tenant_id: u64) -> Result<()> {
        // Check no active catalogs
        let catalogs = self.catalogs.read();
        let active_count = catalogs
            .values()
            .filter(|c| c.tenant_id == tenant_id && c.status != CatalogStatus::Deleted)
            .count();
        if active_count > 0 {
            return Err(Error::InvalidState(format!(
                "tenant {tenant_id} has {active_count} active catalog(s)"
            )));
        }
        drop(catalogs);

        self.tenants
            .write()
            .remove(&tenant_id)
            .ok_or(Error::TenantNotFound(tenant_id))?;
        info!(tenant_id, "deleted tenant");
        self.persist();
        Ok(())
    }

    pub fn list_tenants(&self) -> Vec<Tenant> {
        self.tenants.read().values().cloned().collect()
    }

    // ── Catalog operations ─────────────────────────────────────────────

    pub fn create_catalog(
        &self,
        tenant_id: u64,
        name: String,
        engine: EngineType,
        config: String,
    ) -> Result<(u64, u64)> {
        // Validate tenant exists
        let tenants = self.tenants.read();
        let tenant = tenants
            .get(&tenant_id)
            .ok_or(Error::TenantNotFound(tenant_id))?;
        let max_catalogs = tenant.limits.max_catalogs;
        drop(tenants);

        // Check catalog limit
        let catalogs = self.catalogs.read();
        let current_count = catalogs
            .values()
            .filter(|c| c.tenant_id == tenant_id && c.status != CatalogStatus::Deleted)
            .count() as u32;
        if current_count >= max_catalogs {
            return Err(Error::LimitExceeded(format!(
                "tenant {tenant_id} catalog limit {max_catalogs} reached"
            )));
        }

        // Check name uniqueness within tenant
        if catalogs.values().any(|c| {
            c.tenant_id == tenant_id && c.name == name && c.status != CatalogStatus::Deleted
        }) {
            return Err(Error::CatalogAlreadyExists(name, tenant_id));
        }
        drop(catalogs);

        // Allocate IDs
        let mut cid = self.next_catalog_id.write();
        let catalog_id = *cid;
        *cid += 1;
        drop(cid);

        let mut gid = self.next_raft_group_id.write();
        let raft_group_id = *gid;
        *gid += 1;
        drop(gid);

        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let entry = CatalogEntry {
            catalog_id,
            tenant_id,
            name: name.clone(),
            engine,
            raft_group_id,
            placement: Vec::new(),
            config,
            status: CatalogStatus::Creating,
            created_at: now,
        };

        self.catalogs.write().insert(catalog_id, entry);

        // Create stub routing entry
        self.routing.write().insert(
            raft_group_id,
            RoutingEntry {
                catalog_id,
                raft_group_id,
                leader_node_id: None,
                node_addresses: HashMap::new(),
            },
        );

        info!(
            catalog_id,
            tenant_id,
            raft_group_id,
            name = %name,
            engine = %engine,
            "created catalog"
        );
        self.persist();
        Ok((catalog_id, raft_group_id))
    }

    pub fn get_catalog(&self, catalog_id: u64) -> Option<CatalogEntry> {
        self.catalogs.read().get(&catalog_id).cloned()
    }

    pub fn find_catalog_by_name(&self, tenant_id: u64, name: &str) -> Option<CatalogEntry> {
        self.catalogs
            .read()
            .values()
            .find(|c| {
                c.tenant_id == tenant_id && c.name == name && c.status != CatalogStatus::Deleted
            })
            .cloned()
    }

    pub fn list_catalogs_for_tenant(&self, tenant_id: u64) -> Vec<CatalogEntry> {
        self.catalogs
            .read()
            .values()
            .filter(|c| c.tenant_id == tenant_id && c.status != CatalogStatus::Deleted)
            .cloned()
            .collect()
    }

    pub fn update_catalog_status(&self, catalog_id: u64, status: CatalogStatus) -> Result<()> {
        let mut catalogs = self.catalogs.write();
        let entry = catalogs
            .get_mut(&catalog_id)
            .ok_or(Error::CatalogNotFound(catalog_id))?;
        entry.status = status;
        drop(catalogs);
        self.persist();
        Ok(())
    }

    pub fn update_catalog_placement(&self, catalog_id: u64, placement: Vec<u64>) -> Result<()> {
        let mut catalogs = self.catalogs.write();
        let entry = catalogs
            .get_mut(&catalog_id)
            .ok_or(Error::CatalogNotFound(catalog_id))?;
        let raft_group_id = entry.raft_group_id;
        entry.placement = placement.clone();
        drop(catalogs);

        // Update routing entry node addresses
        let mut routing = self.routing.write();
        if let Some(re) = routing.get_mut(&raft_group_id) {
            re.node_addresses = placement.iter().map(|&nid| (nid, String::new())).collect();
        }
        drop(routing);
        self.persist();
        Ok(())
    }

    pub fn delete_catalog(&self, tenant_id: u64, catalog_id: u64) -> Result<()> {
        let mut catalogs = self.catalogs.write();
        let entry = catalogs
            .get_mut(&catalog_id)
            .ok_or(Error::CatalogNotFound(catalog_id))?;
        if entry.tenant_id != tenant_id {
            return Err(Error::CatalogNotFound(catalog_id));
        }
        entry.status = CatalogStatus::Deleting;
        info!(catalog_id, tenant_id, "marked catalog for deletion");
        drop(catalogs);
        self.persist();
        Ok(())
    }

    // ── Routing ────────────────────────────────────────────────────────

    pub fn update_leader(&self, raft_group_id: u64, leader_node_id: Option<u64>) -> Result<()> {
        let mut routing = self.routing.write();
        let entry = routing
            .get_mut(&raft_group_id)
            .ok_or(Error::InvalidState(format!(
                "no routing entry for group {raft_group_id}"
            )))?;
        entry.leader_node_id = leader_node_id;
        drop(routing);
        self.persist();
        Ok(())
    }

    pub fn get_routing(&self, raft_group_id: u64) -> Option<RoutingEntry> {
        self.routing.read().get(&raft_group_id).cloned()
    }

    pub fn get_routing_table(&self) -> Vec<RoutingEntry> {
        self.routing.read().values().cloned().collect()
    }

    // ── API Keys ───────────────────────────────────────────────────────

    /// Create an API key. The raw key is derived deterministically from
    /// `HMAC(secret, key_id || tenant_id)` so all Raft replicas produce
    /// the same value.
    pub fn create_api_key(&self, tenant_id: u64, scopes: Vec<Scope>) -> Result<(u64, String)> {
        // Validate tenant
        if !self.tenants.read().contains_key(&tenant_id) {
            return Err(Error::TenantNotFound(tenant_id));
        }

        let mut kid = self.next_key_id.write();
        let key_id = *kid;
        *kid += 1;
        drop(kid);

        // Deterministic key derivation: SHA256(secret || key_id || tenant_id)
        let kid_bytes = key_id.to_le_bytes();
        let tid_bytes = tenant_id.to_le_bytes();
        let raw_hash = crate::token::derive_key(&self.config.token_secret, &kid_bytes, &tid_bytes);
        let raw_key = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw_hash);

        // Store the hash of the raw key for verification
        let key_hash = raw_hash.to_vec();

        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let api_key = ApiKey {
            key_id,
            tenant_id,
            key_hash,
            scopes,
            created_at: now,
            expires_at: None,
            revoked: false,
        };

        self.api_keys.write().insert(key_id, api_key);

        // Add key_id to tenant's key list
        if let Some(tenant) = self.tenants.write().get_mut(&tenant_id) {
            tenant.api_keys.push(key_id);
        }

        info!(key_id, tenant_id, "created API key");
        self.persist();
        Ok((key_id, raw_key))
    }

    pub fn revoke_api_key(&self, key_id: u64) -> Result<()> {
        let mut keys = self.api_keys.write();
        let key = keys.get_mut(&key_id).ok_or(Error::ApiKeyNotFound(key_id))?;
        key.revoked = true;
        drop(keys);
        info!(key_id, "revoked API key");
        self.persist();
        Ok(())
    }

    pub fn get_api_key(&self, key_id: u64) -> Option<ApiKey> {
        self.api_keys.read().get(&key_id).cloned()
    }

    // ── Usage ──────────────────────────────────────────────────────────

    pub fn report_usage(
        &self,
        catalog_id: u64,
        disk_bytes: u64,
        deep_storage_bytes: u64,
    ) -> Result<()> {
        if !self.catalogs.read().contains_key(&catalog_id) {
            return Err(Error::CatalogNotFound(catalog_id));
        }

        self.usage.write().insert(
            catalog_id,
            UsageReport {
                catalog_id,
                disk_bytes,
                deep_storage_bytes,
            },
        );
        self.persist();
        Ok(())
    }

    pub fn get_usage(&self, catalog_id: u64) -> Option<UsageReport> {
        self.usage.read().get(&catalog_id).cloned()
    }

    /// Aggregate disk and deep storage usage across all catalogs owned by a tenant.
    pub fn get_tenant_usage(&self, tenant_id: u64) -> (u64, u64) {
        let catalogs = self.catalogs.read();
        let usage = self.usage.read();
        let mut total_disk = 0u64;
        let mut total_deep = 0u64;
        for c in catalogs.values() {
            if c.tenant_id == tenant_id {
                if let Some(u) = usage.get(&c.catalog_id) {
                    total_disk += u.disk_bytes;
                    total_deep += u.deep_storage_bytes;
                }
            }
        }
        (total_disk, total_deep)
    }

    // ── Snapshot ────────────────────────────────────────────────────────

    pub fn snapshot_data(&self) -> MetaSnapshotData {
        MetaSnapshotData {
            accounts: self.accounts.read().clone(),
            users: self.users.read().clone(),
            memberships: self.memberships.read().clone(),
            tenant_grants: self.tenant_grants.read().clone(),
            tenants: self.tenants.read().clone(),
            catalogs: self.catalogs.read().clone(),
            api_keys: self.api_keys.read().clone(),
            routing: self.routing.read().clone(),
            usage: self.usage.read().clone(),
            next_account_id: *self.next_account_id.read(),
            next_user_id: *self.next_user_id.read(),
            next_tenant_id: *self.next_tenant_id.read(),
            next_catalog_id: *self.next_catalog_id.read(),
            next_key_id: *self.next_key_id.read(),
            next_raft_group_id: *self.next_raft_group_id.read(),
        }
    }

    pub fn restore_from_snapshot(&self, data: MetaSnapshotData) {
        *self.accounts.write() = data.accounts;
        *self.users.write() = data.users;
        *self.memberships.write() = data.memberships;
        *self.tenant_grants.write() = data.tenant_grants;
        *self.tenants.write() = data.tenants;
        *self.catalogs.write() = data.catalogs;
        *self.api_keys.write() = data.api_keys;
        *self.routing.write() = data.routing;
        *self.usage.write() = data.usage;
        *self.next_account_id.write() = data.next_account_id;
        *self.next_user_id.write() = data.next_user_id;
        *self.next_tenant_id.write() = data.next_tenant_id;
        *self.next_catalog_id.write() = data.next_catalog_id;
        *self.next_key_id.write() = data.next_key_id;
        *self.next_raft_group_id.write() = data.next_raft_group_id;
        self.persist();
        info!("restored meta engine from snapshot");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Creates a test engine with a default account (id=1) already created.
    fn test_engine() -> MetaEngine {
        let engine = MetaEngine::new(MetaConfig::new(b"test-secret".to_vec()));
        engine.create_account("test-org".into()).unwrap();
        engine
    }

    const TEST_ACCOUNT: u64 = 1;

    #[test]
    fn test_create_tenant() {
        let engine = test_engine();
        let id = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        assert_eq!(id, 1);

        let t = engine.get_tenant(id).unwrap();
        assert_eq!(t.name, "acme");
        assert_eq!(t.tenant_id, 1);
    }

    #[test]
    fn test_create_tenant_duplicate_name_fails() {
        let engine = test_engine();
        engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        let err = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap_err();
        assert!(matches!(err, Error::TenantAlreadyExists(_)));
    }

    #[test]
    fn test_update_tenant_limits() {
        let engine = test_engine();
        let id = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        let new_limits = TenantLimits {
            max_catalogs: 128,
            ..TenantLimits::default()
        };
        engine.update_tenant_limits(id, new_limits.clone()).unwrap();
        assert_eq!(engine.get_tenant(id).unwrap().limits, new_limits);
    }

    #[test]
    fn test_delete_tenant() {
        let engine = test_engine();
        let id = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        engine.delete_tenant(id).unwrap();
        assert!(engine.get_tenant(id).is_none());
    }

    #[test]
    fn test_delete_tenant_with_active_catalogs_fails() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        engine
            .create_catalog(tid, "analytics".into(), EngineType::Lance, "{}".into())
            .unwrap();
        let err = engine.delete_tenant(tid).unwrap_err();
        assert!(matches!(err, Error::InvalidState(_)));
    }

    #[test]
    fn test_create_catalog() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        let (cid, gid) = engine
            .create_catalog(tid, "analytics".into(), EngineType::Lance, "{}".into())
            .unwrap();
        assert_eq!(cid, 1);
        assert_eq!(gid, 1);

        let cat = engine.get_catalog(cid).unwrap();
        assert_eq!(cat.name, "analytics");
        assert_eq!(cat.engine, EngineType::Lance);
        assert_eq!(cat.status, CatalogStatus::Creating);

        // Routing entry should exist
        let re = engine.get_routing(gid).unwrap();
        assert_eq!(re.catalog_id, cid);
        assert!(re.leader_node_id.is_none());
    }

    #[test]
    fn test_create_catalog_exceeds_limit() {
        let engine = test_engine();
        let limits = TenantLimits {
            max_catalogs: 1,
            ..TenantLimits::default()
        };
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), limits)
            .unwrap();
        engine
            .create_catalog(tid, "first".into(), EngineType::Lance, "{}".into())
            .unwrap();
        let err = engine
            .create_catalog(tid, "second".into(), EngineType::Lance, "{}".into())
            .unwrap_err();
        assert!(matches!(err, Error::LimitExceeded(_)));
    }

    #[test]
    fn test_create_catalog_duplicate_name_fails() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        engine
            .create_catalog(tid, "analytics".into(), EngineType::Lance, "{}".into())
            .unwrap();
        let err = engine
            .create_catalog(tid, "analytics".into(), EngineType::Mq, "{}".into())
            .unwrap_err();
        assert!(matches!(err, Error::CatalogAlreadyExists(_, _)));
    }

    #[test]
    fn test_update_catalog_status() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        let (cid, _) = engine
            .create_catalog(tid, "analytics".into(), EngineType::Lance, "{}".into())
            .unwrap();
        engine
            .update_catalog_status(cid, CatalogStatus::Active)
            .unwrap();
        assert_eq!(
            engine.get_catalog(cid).unwrap().status,
            CatalogStatus::Active
        );
    }

    #[test]
    fn test_update_catalog_placement() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        let (cid, gid) = engine
            .create_catalog(tid, "analytics".into(), EngineType::Lance, "{}".into())
            .unwrap();
        engine.update_catalog_placement(cid, vec![1, 2, 3]).unwrap();

        let cat = engine.get_catalog(cid).unwrap();
        assert_eq!(cat.placement, vec![1, 2, 3]);

        let re = engine.get_routing(gid).unwrap();
        assert_eq!(re.node_addresses.len(), 3);
    }

    #[test]
    fn test_delete_catalog() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        let (cid, _) = engine
            .create_catalog(tid, "analytics".into(), EngineType::Lance, "{}".into())
            .unwrap();
        engine.delete_catalog(tid, cid).unwrap();
        assert_eq!(
            engine.get_catalog(cid).unwrap().status,
            CatalogStatus::Deleting
        );
    }

    #[test]
    fn test_update_leader() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        let (_, gid) = engine
            .create_catalog(tid, "analytics".into(), EngineType::Lance, "{}".into())
            .unwrap();
        engine.update_leader(gid, Some(42)).unwrap();
        assert_eq!(engine.get_routing(gid).unwrap().leader_node_id, Some(42));
    }

    #[test]
    fn test_routing_table() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        engine
            .create_catalog(tid, "a".into(), EngineType::Lance, "{}".into())
            .unwrap();
        engine
            .create_catalog(tid, "b".into(), EngineType::Mq, "{}".into())
            .unwrap();
        assert_eq!(engine.get_routing_table().len(), 2);
    }

    #[test]
    fn test_create_api_key() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        let (kid, raw_key) = engine
            .create_api_key(tid, vec![Scope::TenantAdmin])
            .unwrap();
        assert_eq!(kid, 1);
        assert!(!raw_key.is_empty());

        let key = engine.get_api_key(kid).unwrap();
        assert_eq!(key.tenant_id, tid);
        assert!(!key.revoked);

        // Tenant should reference the key
        let t = engine.get_tenant(tid).unwrap();
        assert!(t.api_keys.contains(&kid));
    }

    #[test]
    fn test_revoke_api_key() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        let (kid, _) = engine
            .create_api_key(tid, vec![Scope::TenantAdmin])
            .unwrap();
        engine.revoke_api_key(kid).unwrap();
        assert!(engine.get_api_key(kid).unwrap().revoked);
    }

    #[test]
    fn test_report_usage() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        let (cid, _) = engine
            .create_catalog(tid, "analytics".into(), EngineType::Lance, "{}".into())
            .unwrap();
        engine.report_usage(cid, 1024, 4096).unwrap();

        let u = engine.get_usage(cid).unwrap();
        assert_eq!(u.disk_bytes, 1024);
        assert_eq!(u.deep_storage_bytes, 4096);
    }

    #[test]
    fn test_tenant_aggregate_usage() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        let (c1, _) = engine
            .create_catalog(tid, "a".into(), EngineType::Lance, "{}".into())
            .unwrap();
        let (c2, _) = engine
            .create_catalog(tid, "b".into(), EngineType::Mq, "{}".into())
            .unwrap();
        engine.report_usage(c1, 100, 200).unwrap();
        engine.report_usage(c2, 300, 400).unwrap();

        let (disk, deep) = engine.get_tenant_usage(tid);
        assert_eq!(disk, 400);
        assert_eq!(deep, 600);
    }

    #[test]
    fn test_snapshot_roundtrip() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        engine
            .create_catalog(tid, "analytics".into(), EngineType::Lance, "{}".into())
            .unwrap();
        engine
            .create_api_key(tid, vec![Scope::TenantAdmin])
            .unwrap();

        let snap = engine.snapshot_data();
        assert_eq!(snap.tenants.len(), 1);
        assert_eq!(snap.catalogs.len(), 1);
        assert_eq!(snap.api_keys.len(), 1);

        // Restore into fresh engine
        let engine2 = test_engine();
        engine2.restore_from_snapshot(snap);
        assert_eq!(engine2.list_tenants().len(), 1);
        assert_eq!(engine2.list_catalogs_for_tenant(tid).len(), 1);
    }

    #[test]
    fn test_restore_from_snapshot_clears_existing() {
        let engine = test_engine();
        engine
            .create_tenant(TEST_ACCOUNT, "old".into(), TenantLimits::default())
            .unwrap();

        let empty_snap = MetaSnapshotData {
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
        engine.restore_from_snapshot(empty_snap);
        assert!(engine.list_tenants().is_empty());
    }

    // ── Error path tests ─────────────────────────────────────────────

    #[test]
    fn test_update_tenant_limits_not_found() {
        let engine = test_engine();
        let err = engine
            .update_tenant_limits(999, TenantLimits::default())
            .unwrap_err();
        assert!(matches!(err, Error::TenantNotFound(999)));
    }

    #[test]
    fn test_delete_tenant_not_found() {
        let engine = test_engine();
        let err = engine.delete_tenant(999).unwrap_err();
        assert!(matches!(err, Error::TenantNotFound(999)));
    }

    #[test]
    fn test_create_catalog_tenant_not_found() {
        let engine = test_engine();
        let err = engine
            .create_catalog(999, "x".into(), EngineType::Lance, "{}".into())
            .unwrap_err();
        assert!(matches!(err, Error::TenantNotFound(999)));
    }

    #[test]
    fn test_update_catalog_status_not_found() {
        let engine = test_engine();
        let err = engine
            .update_catalog_status(999, CatalogStatus::Active)
            .unwrap_err();
        assert!(matches!(err, Error::CatalogNotFound(999)));
    }

    #[test]
    fn test_update_catalog_placement_not_found() {
        let engine = test_engine();
        let err = engine.update_catalog_placement(999, vec![1]).unwrap_err();
        assert!(matches!(err, Error::CatalogNotFound(999)));
    }

    #[test]
    fn test_delete_catalog_not_found() {
        let engine = test_engine();
        let err = engine.delete_catalog(1, 999).unwrap_err();
        assert!(matches!(err, Error::CatalogNotFound(999)));
    }

    #[test]
    fn test_delete_catalog_wrong_tenant() {
        let engine = test_engine();
        let t1 = engine
            .create_tenant(TEST_ACCOUNT, "t1".into(), TenantLimits::default())
            .unwrap();
        let t2 = engine
            .create_tenant(TEST_ACCOUNT, "t2".into(), TenantLimits::default())
            .unwrap();
        let (cid, _) = engine
            .create_catalog(t1, "cat".into(), EngineType::Lance, "{}".into())
            .unwrap();
        // Try to delete t1's catalog using t2's tenant_id
        let err = engine.delete_catalog(t2, cid).unwrap_err();
        assert!(matches!(err, Error::CatalogNotFound(_)));
    }

    #[test]
    fn test_update_leader_no_routing_entry() {
        let engine = test_engine();
        let err = engine.update_leader(999, Some(1)).unwrap_err();
        assert!(matches!(err, Error::InvalidState(_)));
    }

    #[test]
    fn test_create_api_key_tenant_not_found() {
        let engine = test_engine();
        let err = engine
            .create_api_key(999, vec![Scope::TenantAdmin])
            .unwrap_err();
        assert!(matches!(err, Error::TenantNotFound(999)));
    }

    #[test]
    fn test_revoke_api_key_not_found() {
        let engine = test_engine();
        let err = engine.revoke_api_key(999).unwrap_err();
        assert!(matches!(err, Error::ApiKeyNotFound(999)));
    }

    #[test]
    fn test_report_usage_catalog_not_found() {
        let engine = test_engine();
        let err = engine.report_usage(999, 100, 200).unwrap_err();
        assert!(matches!(err, Error::CatalogNotFound(999)));
    }

    // ── Lookup / query tests ─────────────────────────────────────────

    #[test]
    fn test_find_tenant_by_name() {
        let engine = test_engine();
        engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        engine
            .create_tenant(TEST_ACCOUNT, "beta".into(), TenantLimits::default())
            .unwrap();

        let t = engine.find_tenant_by_name("acme").unwrap();
        assert_eq!(t.name, "acme");
        assert_eq!(t.tenant_id, 1);

        let t2 = engine.find_tenant_by_name("beta").unwrap();
        assert_eq!(t2.tenant_id, 2);

        assert!(engine.find_tenant_by_name("nonexistent").is_none());
    }

    #[test]
    fn test_find_catalog_by_name() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        engine
            .create_catalog(tid, "analytics".into(), EngineType::Lance, "{}".into())
            .unwrap();
        engine
            .create_catalog(tid, "events".into(), EngineType::Mq, "{}".into())
            .unwrap();

        let c = engine.find_catalog_by_name(tid, "analytics").unwrap();
        assert_eq!(c.name, "analytics");
        assert_eq!(c.engine, EngineType::Lance);

        let c2 = engine.find_catalog_by_name(tid, "events").unwrap();
        assert_eq!(c2.engine, EngineType::Mq);

        assert!(engine.find_catalog_by_name(tid, "nope").is_none());
        assert!(engine.find_catalog_by_name(999, "analytics").is_none());
    }

    #[test]
    fn test_find_catalog_by_name_excludes_deleted() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        let (cid, _) = engine
            .create_catalog(tid, "analytics".into(), EngineType::Lance, "{}".into())
            .unwrap();

        // Mark as deleted status
        engine
            .update_catalog_status(cid, CatalogStatus::Deleted)
            .unwrap();

        // Should not be findable by name
        assert!(engine.find_catalog_by_name(tid, "analytics").is_none());
    }

    #[test]
    fn test_list_catalogs_excludes_deleted() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        let (c1, _) = engine
            .create_catalog(tid, "a".into(), EngineType::Lance, "{}".into())
            .unwrap();
        engine
            .create_catalog(tid, "b".into(), EngineType::Mq, "{}".into())
            .unwrap();

        assert_eq!(engine.list_catalogs_for_tenant(tid).len(), 2);

        engine
            .update_catalog_status(c1, CatalogStatus::Deleted)
            .unwrap();
        assert_eq!(engine.list_catalogs_for_tenant(tid).len(), 1);
    }

    #[test]
    fn test_get_nonexistent_returns_none() {
        let engine = test_engine();
        assert!(engine.get_tenant(999).is_none());
        assert!(engine.get_catalog(999).is_none());
        assert!(engine.get_api_key(999).is_none());
        assert!(engine.get_routing(999).is_none());
        assert!(engine.get_usage(999).is_none());
    }

    // ── Multi-tenant isolation tests ─────────────────────────────────

    #[test]
    fn test_multi_tenant_catalog_isolation() {
        let engine = test_engine();
        let t1 = engine
            .create_tenant(TEST_ACCOUNT, "t1".into(), TenantLimits::default())
            .unwrap();
        let t2 = engine
            .create_tenant(TEST_ACCOUNT, "t2".into(), TenantLimits::default())
            .unwrap();

        engine
            .create_catalog(t1, "shared_name".into(), EngineType::Lance, "{}".into())
            .unwrap();
        engine
            .create_catalog(t2, "shared_name".into(), EngineType::Lance, "{}".into())
            .unwrap();

        // Same name across tenants should work
        assert_eq!(engine.list_catalogs_for_tenant(t1).len(), 1);
        assert_eq!(engine.list_catalogs_for_tenant(t2).len(), 1);

        let c1 = engine.find_catalog_by_name(t1, "shared_name").unwrap();
        let c2 = engine.find_catalog_by_name(t2, "shared_name").unwrap();
        assert_ne!(c1.catalog_id, c2.catalog_id);
        assert_ne!(c1.raft_group_id, c2.raft_group_id);
    }

    #[test]
    fn test_multi_tenant_usage_isolation() {
        let engine = test_engine();
        let t1 = engine
            .create_tenant(TEST_ACCOUNT, "t1".into(), TenantLimits::default())
            .unwrap();
        let t2 = engine
            .create_tenant(TEST_ACCOUNT, "t2".into(), TenantLimits::default())
            .unwrap();

        let (c1, _) = engine
            .create_catalog(t1, "a".into(), EngineType::Lance, "{}".into())
            .unwrap();
        let (c2, _) = engine
            .create_catalog(t2, "b".into(), EngineType::Lance, "{}".into())
            .unwrap();

        engine.report_usage(c1, 100, 200).unwrap();
        engine.report_usage(c2, 300, 400).unwrap();

        let (d1, s1) = engine.get_tenant_usage(t1);
        assert_eq!(d1, 100);
        assert_eq!(s1, 200);

        let (d2, s2) = engine.get_tenant_usage(t2);
        assert_eq!(d2, 300);
        assert_eq!(s2, 400);
    }

    #[test]
    fn test_tenant_usage_no_reports() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        engine
            .create_catalog(tid, "a".into(), EngineType::Lance, "{}".into())
            .unwrap();

        let (disk, deep) = engine.get_tenant_usage(tid);
        assert_eq!(disk, 0);
        assert_eq!(deep, 0);
    }

    // ── ID monotonicity tests ────────────────────────────────────────

    #[test]
    fn test_ids_are_monotonically_increasing() {
        let engine = test_engine();
        let t1 = engine
            .create_tenant(TEST_ACCOUNT, "a".into(), TenantLimits::default())
            .unwrap();
        let t2 = engine
            .create_tenant(TEST_ACCOUNT, "b".into(), TenantLimits::default())
            .unwrap();
        assert_eq!(t1, 1);
        assert_eq!(t2, 2);

        let (c1, g1) = engine
            .create_catalog(t1, "x".into(), EngineType::Lance, "{}".into())
            .unwrap();
        let (c2, g2) = engine
            .create_catalog(t1, "y".into(), EngineType::Mq, "{}".into())
            .unwrap();
        assert_eq!(c1, 1);
        assert_eq!(c2, 2);
        assert_eq!(g1, 1);
        assert_eq!(g2, 2);

        let (k1, _) = engine.create_api_key(t1, vec![]).unwrap();
        let (k2, _) = engine.create_api_key(t1, vec![]).unwrap();
        assert_eq!(k1, 1);
        assert_eq!(k2, 2);
    }

    // ── Snapshot ID preservation ─────────────────────────────────────

    #[test]
    fn test_snapshot_preserves_id_counters() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        engine
            .create_catalog(tid, "a".into(), EngineType::Lance, "{}".into())
            .unwrap();
        engine
            .create_catalog(tid, "b".into(), EngineType::Mq, "{}".into())
            .unwrap();
        engine.create_api_key(tid, vec![]).unwrap();

        let snap = engine.snapshot_data();
        assert_eq!(snap.next_tenant_id, 2);
        assert_eq!(snap.next_catalog_id, 3);
        assert_eq!(snap.next_key_id, 2);
        assert_eq!(snap.next_raft_group_id, 3);

        // Restore and verify new IDs continue from where we left off
        let engine2 = test_engine();
        engine2.restore_from_snapshot(snap);

        let t2 = engine2
            .create_tenant(TEST_ACCOUNT, "beta".into(), TenantLimits::default())
            .unwrap();
        assert_eq!(t2, 2);

        let (c3, g3) = engine2
            .create_catalog(t2, "c".into(), EngineType::LibSql, "{}".into())
            .unwrap();
        assert_eq!(c3, 3);
        assert_eq!(g3, 3);
    }

    // ── Deterministic key generation ─────────────────────────────────

    #[test]
    fn test_api_key_deterministic() {
        let engine1 = test_engine();
        let engine2 = test_engine();

        let t1 = engine1
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        let t2 = engine2
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        assert_eq!(t1, t2);

        let (k1, raw1) = engine1
            .create_api_key(t1, vec![Scope::TenantAdmin])
            .unwrap();
        let (k2, raw2) = engine2
            .create_api_key(t2, vec![Scope::TenantAdmin])
            .unwrap();

        // Same key_id + tenant_id + secret = same raw key
        assert_eq!(k1, k2);
        assert_eq!(raw1, raw2);
    }

    #[test]
    fn test_api_key_multiple_scopes() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        let scopes = vec![
            Scope::TenantAdmin,
            Scope::Catalog("analytics".into()),
            Scope::CatalogRead("events".into()),
        ];
        let (kid, _) = engine.create_api_key(tid, scopes.clone()).unwrap();
        let key = engine.get_api_key(kid).unwrap();
        assert_eq!(key.scopes, scopes);
    }

    // ── Usage update (overwrite) ─────────────────────────────────────

    #[test]
    fn test_report_usage_overwrites_previous() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        let (cid, _) = engine
            .create_catalog(tid, "a".into(), EngineType::Lance, "{}".into())
            .unwrap();

        engine.report_usage(cid, 100, 200).unwrap();
        engine.report_usage(cid, 500, 600).unwrap();

        let u = engine.get_usage(cid).unwrap();
        assert_eq!(u.disk_bytes, 500);
        assert_eq!(u.deep_storage_bytes, 600);
    }

    // ── Catalog reuse after deletion ─────────────────────────────────

    #[test]
    fn test_can_create_catalog_with_deleted_name() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        let (cid, _) = engine
            .create_catalog(tid, "analytics".into(), EngineType::Lance, "{}".into())
            .unwrap();

        // Mark as Deleted
        engine
            .update_catalog_status(cid, CatalogStatus::Deleted)
            .unwrap();

        // Should be able to create a new catalog with the same name
        let (cid2, _) = engine
            .create_catalog(tid, "analytics".into(), EngineType::Lance, "{}".into())
            .unwrap();
        assert_ne!(cid, cid2);
    }

    // ── Delete tenant after catalogs deleted ─────────────────────────

    #[test]
    fn test_delete_tenant_succeeds_after_catalogs_deleted() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        let (cid, _) = engine
            .create_catalog(tid, "a".into(), EngineType::Lance, "{}".into())
            .unwrap();

        // Can't delete with active catalog
        assert!(engine.delete_tenant(tid).is_err());

        // Mark catalog as Deleted
        engine
            .update_catalog_status(cid, CatalogStatus::Deleted)
            .unwrap();

        // Now deletion should succeed
        engine.delete_tenant(tid).unwrap();
        assert!(engine.get_tenant(tid).is_none());
    }

    // ── Multiple engine types ────────────────────────────────────────

    #[test]
    fn test_all_engine_types() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();

        let (c1, _) = engine
            .create_catalog(tid, "lance_cat".into(), EngineType::Lance, "{}".into())
            .unwrap();
        let (c2, _) = engine
            .create_catalog(tid, "mq_cat".into(), EngineType::Mq, "{}".into())
            .unwrap();
        let (c3, _) = engine
            .create_catalog(tid, "sql_cat".into(), EngineType::LibSql, "{}".into())
            .unwrap();

        assert_eq!(engine.get_catalog(c1).unwrap().engine, EngineType::Lance);
        assert_eq!(engine.get_catalog(c2).unwrap().engine, EngineType::Mq);
        assert_eq!(engine.get_catalog(c3).unwrap().engine, EngineType::LibSql);
    }

    // ── Leader update to None ────────────────────────────────────────

    #[test]
    fn test_update_leader_to_none() {
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        let (_, gid) = engine
            .create_catalog(tid, "a".into(), EngineType::Lance, "{}".into())
            .unwrap();

        engine.update_leader(gid, Some(1)).unwrap();
        assert_eq!(engine.get_routing(gid).unwrap().leader_node_id, Some(1));

        engine.update_leader(gid, None).unwrap();
        assert_eq!(engine.get_routing(gid).unwrap().leader_node_id, None);
    }

    // ── List tenants ─────────────────────────────────────────────────

    #[test]
    fn test_list_tenants_multiple() {
        let engine = test_engine();
        assert!(engine.list_tenants().is_empty());

        engine
            .create_tenant(TEST_ACCOUNT, "a".into(), TenantLimits::default())
            .unwrap();
        engine
            .create_tenant(TEST_ACCOUNT, "b".into(), TenantLimits::default())
            .unwrap();
        engine
            .create_tenant(TEST_ACCOUNT, "c".into(), TenantLimits::default())
            .unwrap();

        let tenants = engine.list_tenants();
        assert_eq!(tenants.len(), 3);
    }

    // ── Catalog limit with deleted catalogs ──────────────────────────

    #[test]
    fn test_catalog_limit_excludes_deleted() {
        let engine = test_engine();
        let limits = TenantLimits {
            max_catalogs: 1,
            ..TenantLimits::default()
        };
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), limits)
            .unwrap();
        let (cid, _) = engine
            .create_catalog(tid, "first".into(), EngineType::Lance, "{}".into())
            .unwrap();

        // At limit
        assert!(
            engine
                .create_catalog(tid, "second".into(), EngineType::Lance, "{}".into())
                .is_err()
        );

        // Delete first, should free up the slot
        engine
            .update_catalog_status(cid, CatalogStatus::Deleted)
            .unwrap();

        engine
            .create_catalog(tid, "second".into(), EngineType::Lance, "{}".into())
            .unwrap();
    }

    // ── MDBX persistence tests ──────────────────────────────────────

    /// Creates a persistent test engine at the given path with a default account.
    fn persistent_engine(dir: &Path) -> MetaEngine {
        let engine =
            MetaEngine::open(MetaConfig::new(b"test-secret".to_vec()), dir).unwrap();
        // Only create account on first open (persistence means it may already exist).
        if engine.list_accounts().is_empty() {
            engine.create_account("test-org".into()).unwrap();
        }
        engine
    }

    #[test]
    fn test_persistent_open_empty() {
        let dir = tempfile::tempdir().unwrap();
        let engine = MetaEngine::open(
            MetaConfig::new(b"test-secret".to_vec()),
            dir.path(),
        )
        .unwrap();
        assert!(engine.list_accounts().is_empty());
        assert!(engine.list_tenants().is_empty());
    }

    #[test]
    fn test_persistent_accounts_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();

        {
            let engine = persistent_engine(dir.path());
            assert_eq!(engine.list_accounts().len(), 1);
            assert_eq!(engine.list_accounts()[0].name, "test-org");
        }

        // Reopen — account should still be there.
        {
            let engine = persistent_engine(dir.path());
            assert_eq!(engine.list_accounts().len(), 1);
            assert_eq!(engine.list_accounts()[0].name, "test-org");
        }
    }

    #[test]
    fn test_persistent_tenant_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();

        let tenant_id;
        {
            let engine = persistent_engine(dir.path());
            tenant_id = engine
                .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
                .unwrap();
            assert_eq!(tenant_id, 1);
        }

        {
            let engine = persistent_engine(dir.path());
            let t = engine.get_tenant(tenant_id).unwrap();
            assert_eq!(t.name, "acme");
            assert_eq!(t.account_id, TEST_ACCOUNT);

            // ID counter should continue from 2.
            let t2 = engine
                .create_tenant(TEST_ACCOUNT, "beta".into(), TenantLimits::default())
                .unwrap();
            assert_eq!(t2, 2);
        }
    }

    #[test]
    fn test_persistent_catalog_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();

        let (catalog_id, raft_group_id);
        {
            let engine = persistent_engine(dir.path());
            let tid = engine
                .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
                .unwrap();
            let result = engine
                .create_catalog(tid, "analytics".into(), EngineType::Lance, "{}".into())
                .unwrap();
            catalog_id = result.0;
            raft_group_id = result.1;
            engine
                .update_catalog_status(catalog_id, CatalogStatus::Active)
                .unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            let cat = engine.get_catalog(catalog_id).unwrap();
            assert_eq!(cat.name, "analytics");
            assert_eq!(cat.engine, EngineType::Lance);
            assert_eq!(cat.status, CatalogStatus::Active);
            assert_eq!(cat.raft_group_id, raft_group_id);

            // Routing entry should also persist.
            let re = engine.get_routing(raft_group_id).unwrap();
            assert_eq!(re.catalog_id, catalog_id);
        }
    }

    #[test]
    fn test_persistent_api_keys_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();

        let (key_id, raw_key);
        {
            let engine = persistent_engine(dir.path());
            let tid = engine
                .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
                .unwrap();
            let result = engine
                .create_api_key(tid, vec![Scope::TenantAdmin, Scope::Catalog("db".into())])
                .unwrap();
            key_id = result.0;
            raw_key = result.1;
        }

        {
            let engine = persistent_engine(dir.path());
            let key = engine.get_api_key(key_id).unwrap();
            assert_eq!(key.tenant_id, 1);
            assert!(!key.revoked);
            assert_eq!(
                key.scopes,
                vec![Scope::TenantAdmin, Scope::Catalog("db".into())]
            );

            // Tenant should still reference the key.
            let tenant = engine.get_tenant(1).unwrap();
            assert!(tenant.api_keys.contains(&key_id));

            // Deterministic key derivation should produce the same raw key.
            let (k2, raw2) = engine
                .create_api_key(1, vec![Scope::TenantAdmin])
                .unwrap();
            // Different key_id, so different raw key.
            assert_ne!(k2, key_id);
            assert_ne!(raw2, raw_key);
        }
    }

    #[test]
    fn test_persistent_users_and_memberships_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();

        let user_id;
        {
            let engine = persistent_engine(dir.path());
            user_id = engine
                .create_user("alice".into(), b"hash123".to_vec())
                .unwrap();
            engine
                .add_membership(user_id, TEST_ACCOUNT, AccountRole::Admin)
                .unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            let user = engine.get_user(user_id).unwrap();
            assert_eq!(user.username, "alice");
            assert!(!user.disabled);

            let memberships = engine.get_user_memberships(user_id);
            assert_eq!(memberships.len(), 1);
            assert_eq!(memberships[0].account_id, TEST_ACCOUNT);
            assert_eq!(memberships[0].role, AccountRole::Admin);
        }
    }

    #[test]
    fn test_persistent_tenant_grants_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();

        let (user_id, tenant_id);
        {
            let engine = persistent_engine(dir.path());
            user_id = engine
                .create_user("bob".into(), b"hash".to_vec())
                .unwrap();
            engine
                .add_membership(user_id, TEST_ACCOUNT, AccountRole::Member)
                .unwrap();
            tenant_id = engine
                .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
                .unwrap();
            engine
                .grant_tenant_access(user_id, tenant_id, CatalogAccess::All)
                .unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            let grants = engine.get_user_tenant_grants(user_id);
            assert_eq!(grants.len(), 1);
            assert_eq!(grants[0].tenant_id, tenant_id);
            assert!(matches!(grants[0].catalog_access, CatalogAccess::All));
        }
    }

    #[test]
    fn test_persistent_usage_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();

        let catalog_id;
        {
            let engine = persistent_engine(dir.path());
            let tid = engine
                .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
                .unwrap();
            catalog_id = engine
                .create_catalog(tid, "db".into(), EngineType::Lance, "{}".into())
                .unwrap()
                .0;
            engine.report_usage(catalog_id, 1024, 4096).unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            let u = engine.get_usage(catalog_id).unwrap();
            assert_eq!(u.disk_bytes, 1024);
            assert_eq!(u.deep_storage_bytes, 4096);
        }
    }

    #[test]
    fn test_persistent_id_counters_continue_after_reopen() {
        let dir = tempfile::tempdir().unwrap();

        {
            let engine = persistent_engine(dir.path());
            let t1 = engine
                .create_tenant(TEST_ACCOUNT, "a".into(), TenantLimits::default())
                .unwrap();
            let t2 = engine
                .create_tenant(TEST_ACCOUNT, "b".into(), TenantLimits::default())
                .unwrap();
            assert_eq!(t1, 1);
            assert_eq!(t2, 2);

            engine
                .create_catalog(t1, "x".into(), EngineType::Lance, "{}".into())
                .unwrap();
            engine
                .create_catalog(t1, "y".into(), EngineType::Mq, "{}".into())
                .unwrap();
            engine.create_api_key(t1, vec![]).unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            // Tenant IDs should continue from 3.
            let t3 = engine
                .create_tenant(TEST_ACCOUNT, "c".into(), TenantLimits::default())
                .unwrap();
            assert_eq!(t3, 3);

            // Catalog IDs from 3, raft group IDs from 3.
            let (c3, g3) = engine
                .create_catalog(t3, "z".into(), EngineType::LibSql, "{}".into())
                .unwrap();
            assert_eq!(c3, 3);
            assert_eq!(g3, 3);

            // API key IDs from 2.
            let (k2, _) = engine.create_api_key(1, vec![]).unwrap();
            assert_eq!(k2, 2);
        }
    }

    #[test]
    fn test_persistent_delete_operations_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();

        {
            let engine = persistent_engine(dir.path());
            let tid = engine
                .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
                .unwrap();
            let (cid, _) = engine
                .create_catalog(tid, "db".into(), EngineType::Lance, "{}".into())
                .unwrap();
            let (kid, _) = engine
                .create_api_key(tid, vec![Scope::TenantAdmin])
                .unwrap();

            // Delete catalog, revoke key, then delete tenant.
            engine
                .update_catalog_status(cid, CatalogStatus::Deleted)
                .unwrap();
            engine.revoke_api_key(kid).unwrap();
            engine.delete_tenant(tid).unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            // Tenant should be gone.
            assert!(engine.get_tenant(1).is_none());
            assert!(engine.list_tenants().is_empty());

            // Catalog should be marked Deleted (not removed from map).
            let cat = engine.get_catalog(1).unwrap();
            assert_eq!(cat.status, CatalogStatus::Deleted);

            // API key should be revoked.
            let key = engine.get_api_key(1).unwrap();
            assert!(key.revoked);
        }
    }

    #[test]
    fn test_persistent_disable_user_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();

        let user_id;
        {
            let engine = persistent_engine(dir.path());
            user_id = engine
                .create_user("carol".into(), b"hash".to_vec())
                .unwrap();
            engine.disable_user(user_id).unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            let user = engine.get_user(user_id).unwrap();
            assert!(user.disabled);
        }
    }

    #[test]
    fn test_persistent_update_password_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();

        let user_id;
        {
            let engine = persistent_engine(dir.path());
            user_id = engine
                .create_user("dave".into(), b"old-hash".to_vec())
                .unwrap();
            engine
                .update_password(user_id, b"new-hash".to_vec())
                .unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            let user = engine.get_user(user_id).unwrap();
            assert_eq!(user.password_hash, b"new-hash");
        }
    }

    #[test]
    fn test_persistent_update_tenant_limits_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();

        let tid;
        {
            let engine = persistent_engine(dir.path());
            tid = engine
                .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
                .unwrap();
            let new_limits = TenantLimits {
                max_catalogs: 256,
                max_concurrent_queries: 128,
                ..TenantLimits::default()
            };
            engine.update_tenant_limits(tid, new_limits).unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            let t = engine.get_tenant(tid).unwrap();
            assert_eq!(t.limits.max_catalogs, 256);
            assert_eq!(t.limits.max_concurrent_queries, 128);
        }
    }

    #[test]
    fn test_persistent_catalog_placement_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();

        let (cid, gid);
        {
            let engine = persistent_engine(dir.path());
            let tid = engine
                .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
                .unwrap();
            let result = engine
                .create_catalog(tid, "db".into(), EngineType::Lance, "{}".into())
                .unwrap();
            cid = result.0;
            gid = result.1;
            engine
                .update_catalog_placement(cid, vec![1, 2, 3])
                .unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            let cat = engine.get_catalog(cid).unwrap();
            assert_eq!(cat.placement, vec![1, 2, 3]);

            let re = engine.get_routing(gid).unwrap();
            assert_eq!(re.node_addresses.len(), 3);
        }
    }

    #[test]
    fn test_persistent_leader_update_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();

        let gid;
        {
            let engine = persistent_engine(dir.path());
            let tid = engine
                .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
                .unwrap();
            gid = engine
                .create_catalog(tid, "db".into(), EngineType::Lance, "{}".into())
                .unwrap()
                .1;
            engine.update_leader(gid, Some(42)).unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            assert_eq!(engine.get_routing(gid).unwrap().leader_node_id, Some(42));
        }
    }

    #[test]
    fn test_persistent_remove_membership_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();

        let user_id;
        {
            let engine = persistent_engine(dir.path());
            user_id = engine
                .create_user("eve".into(), b"hash".to_vec())
                .unwrap();
            engine
                .add_membership(user_id, TEST_ACCOUNT, AccountRole::Admin)
                .unwrap();
            engine.remove_membership(user_id, TEST_ACCOUNT).unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            assert!(engine.get_user_memberships(user_id).is_empty());
        }
    }

    #[test]
    fn test_persistent_set_account_role_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();

        let user_id;
        {
            let engine = persistent_engine(dir.path());
            user_id = engine
                .create_user("frank".into(), b"hash".to_vec())
                .unwrap();
            engine
                .add_membership(user_id, TEST_ACCOUNT, AccountRole::Member)
                .unwrap();
            engine
                .set_account_role(user_id, TEST_ACCOUNT, AccountRole::Admin)
                .unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            let memberships = engine.get_user_memberships(user_id);
            assert_eq!(memberships.len(), 1);
            assert_eq!(memberships[0].role, AccountRole::Admin);
        }
    }

    #[test]
    fn test_persistent_revoke_tenant_access_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();

        let (user_id, tenant_id);
        {
            let engine = persistent_engine(dir.path());
            user_id = engine
                .create_user("grace".into(), b"hash".to_vec())
                .unwrap();
            engine
                .add_membership(user_id, TEST_ACCOUNT, AccountRole::Member)
                .unwrap();
            tenant_id = engine
                .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
                .unwrap();
            engine
                .grant_tenant_access(user_id, tenant_id, CatalogAccess::All)
                .unwrap();
            engine
                .revoke_tenant_access(user_id, tenant_id)
                .unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            assert!(engine.get_user_tenant_grants(user_id).is_empty());
        }
    }

    #[test]
    fn test_persistent_delete_account_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();

        let account_id;
        {
            let engine =
                MetaEngine::open(MetaConfig::new(b"test-secret".to_vec()), dir.path()).unwrap();
            account_id = engine.create_account("ephemeral".into()).unwrap();
            engine.delete_account(account_id).unwrap();
        }

        {
            let engine =
                MetaEngine::open(MetaConfig::new(b"test-secret".to_vec()), dir.path()).unwrap();
            assert!(engine.get_account(account_id).is_none());
            assert!(engine.list_accounts().is_empty());
        }
    }

    #[test]
    fn test_persistent_restore_from_snapshot_persists() {
        let dir = tempfile::tempdir().unwrap();

        {
            let engine = persistent_engine(dir.path());
            let tid = engine
                .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
                .unwrap();
            engine
                .create_catalog(tid, "db".into(), EngineType::Lance, "{}".into())
                .unwrap();
        }

        // Take a snapshot, open a new engine, restore, verify persistence.
        let snapshot;
        {
            let engine = persistent_engine(dir.path());
            snapshot = engine.snapshot_data();
        }

        let dir2 = tempfile::tempdir().unwrap();
        {
            let engine =
                MetaEngine::open(MetaConfig::new(b"test-secret".to_vec()), dir2.path()).unwrap();
            engine.restore_from_snapshot(snapshot);
        }

        {
            let engine =
                MetaEngine::open(MetaConfig::new(b"test-secret".to_vec()), dir2.path()).unwrap();
            assert_eq!(engine.list_accounts().len(), 1);
            assert_eq!(engine.list_tenants().len(), 1);
            let cats = engine.list_catalogs_for_tenant(1);
            assert_eq!(cats.len(), 1);
            assert_eq!(cats[0].name, "db");
        }
    }

    #[test]
    fn test_persistent_bootstrap_only_runs_once() {
        let dir = tempfile::tempdir().unwrap();

        // Simulate bootstrap: create account + tenant + key.
        {
            let engine = persistent_engine(dir.path());
            let tid = engine
                .create_tenant(TEST_ACCOUNT, "default".into(), TenantLimits::default())
                .unwrap();
            engine
                .create_api_key(tid, vec![Scope::SuperAdmin])
                .unwrap();
        }

        // On second open, accounts are not empty so bootstrap should not run.
        {
            let engine = persistent_engine(dir.path());
            // Should still have exactly 1 account, 1 tenant, 1 key.
            assert_eq!(engine.list_accounts().len(), 1);
            assert_eq!(engine.list_tenants().len(), 1);
            let keys: Vec<_> = (1..=10)
                .filter_map(|id| engine.get_api_key(id))
                .collect();
            assert_eq!(keys.len(), 1);
        }
    }

    #[test]
    fn test_persistent_multiple_mutations_between_reopens() {
        let dir = tempfile::tempdir().unwrap();

        {
            let engine = persistent_engine(dir.path());
            // Create 5 tenants with catalogs.
            for i in 1..=5 {
                let tid = engine
                    .create_tenant(
                        TEST_ACCOUNT,
                        format!("tenant-{i}"),
                        TenantLimits::default(),
                    )
                    .unwrap();
                engine
                    .create_catalog(tid, format!("cat-{i}"), EngineType::Lance, "{}".into())
                    .unwrap();
                engine
                    .create_api_key(tid, vec![Scope::TenantAdmin])
                    .unwrap();
            }
        }

        {
            let engine = persistent_engine(dir.path());
            assert_eq!(engine.list_tenants().len(), 5);
            for i in 1..=5u64 {
                let t = engine.get_tenant(i).unwrap();
                assert_eq!(t.name, format!("tenant-{i}"));
                let cats = engine.list_catalogs_for_tenant(i);
                assert_eq!(cats.len(), 1);
                assert_eq!(cats[0].name, format!("cat-{i}"));
            }

            // Delete tenant 3's catalog and tenant.
            engine
                .update_catalog_status(3, CatalogStatus::Deleted)
                .unwrap();
            engine.delete_tenant(3).unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            assert_eq!(engine.list_tenants().len(), 4);
            assert!(engine.get_tenant(3).is_none());
            // Catalog is still in the map but marked deleted.
            assert_eq!(
                engine.get_catalog(3).unwrap().status,
                CatalogStatus::Deleted
            );

            // ID counters should continue from 6.
            let t6 = engine
                .create_tenant(TEST_ACCOUNT, "tenant-6".into(), TenantLimits::default())
                .unwrap();
            assert_eq!(t6, 6);
        }
    }

    #[test]
    fn test_persistent_specific_catalog_grants() {
        let dir = tempfile::tempdir().unwrap();

        let (user_id, tenant_id);
        {
            let engine = persistent_engine(dir.path());
            user_id = engine
                .create_user("henry".into(), b"hash".to_vec())
                .unwrap();
            engine
                .add_membership(user_id, TEST_ACCOUNT, AccountRole::Member)
                .unwrap();
            tenant_id = engine
                .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
                .unwrap();
            let access = CatalogAccess::Specific(vec![
                CatalogGrant {
                    catalog_name: "analytics".into(),
                    read_only: false,
                },
                CatalogGrant {
                    catalog_name: "events".into(),
                    read_only: true,
                },
            ]);
            engine
                .grant_tenant_access(user_id, tenant_id, access)
                .unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            let grants = engine.get_user_tenant_grants(user_id);
            assert_eq!(grants.len(), 1);
            match &grants[0].catalog_access {
                CatalogAccess::Specific(list) => {
                    assert_eq!(list.len(), 2);
                    assert_eq!(list[0].catalog_name, "analytics");
                    assert!(!list[0].read_only);
                    assert_eq!(list[1].catalog_name, "events");
                    assert!(list[1].read_only);
                }
                CatalogAccess::All => panic!("expected Specific access"),
            }
        }
    }

    #[test]
    fn test_persistent_user_accessible_tenants() {
        let dir = tempfile::tempdir().unwrap();

        let (admin_id, member_id, t1);
        {
            let engine = persistent_engine(dir.path());
            admin_id = engine
                .create_user("admin".into(), b"hash".to_vec())
                .unwrap();
            engine
                .add_membership(admin_id, TEST_ACCOUNT, AccountRole::Admin)
                .unwrap();

            member_id = engine
                .create_user("member".into(), b"hash".to_vec())
                .unwrap();
            engine
                .add_membership(member_id, TEST_ACCOUNT, AccountRole::Member)
                .unwrap();

            t1 = engine
                .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
                .unwrap();
            engine
                .create_tenant(TEST_ACCOUNT, "beta".into(), TenantLimits::default())
                .unwrap();

            // Grant member access only to t1.
            engine
                .grant_tenant_access(member_id, t1, CatalogAccess::All)
                .unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            // Admin sees all tenants in the account.
            let admin_tenants = engine.get_user_accessible_tenants(admin_id);
            assert_eq!(admin_tenants.len(), 2);

            // Member sees only granted tenant.
            let member_tenants = engine.get_user_accessible_tenants(member_id);
            assert_eq!(member_tenants.len(), 1);
            assert_eq!(member_tenants[0].tenant_id, t1);
        }
    }

    #[test]
    fn test_persistent_multi_engine_type_catalogs() {
        let dir = tempfile::tempdir().unwrap();

        {
            let engine = persistent_engine(dir.path());
            let tid = engine
                .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
                .unwrap();
            engine
                .create_catalog(tid, "lance_cat".into(), EngineType::Lance, "{}".into())
                .unwrap();
            engine
                .create_catalog(tid, "mq_cat".into(), EngineType::Mq, "{}".into())
                .unwrap();
            engine
                .create_catalog(
                    tid,
                    "sql_cat".into(),
                    EngineType::LibSql,
                    r#"{"path":"test.db"}"#.into(),
                )
                .unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            let cats = engine.list_catalogs_for_tenant(1);
            assert_eq!(cats.len(), 3);
            let lance = engine.find_catalog_by_name(1, "lance_cat").unwrap();
            assert_eq!(lance.engine, EngineType::Lance);
            let mq = engine.find_catalog_by_name(1, "mq_cat").unwrap();
            assert_eq!(mq.engine, EngineType::Mq);
            let sql = engine.find_catalog_by_name(1, "sql_cat").unwrap();
            assert_eq!(sql.engine, EngineType::LibSql);
            assert_eq!(sql.config, r#"{"path":"test.db"}"#);
        }
    }

    #[test]
    fn test_persistent_triple_reopen_with_mutations() {
        let dir = tempfile::tempdir().unwrap();

        // Phase 1: Create account + tenant + catalog.
        let (tid, cid);
        {
            let engine = persistent_engine(dir.path());
            tid = engine
                .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
                .unwrap();
            cid = engine
                .create_catalog(tid, "db".into(), EngineType::Lance, "{}".into())
                .unwrap()
                .0;
        }

        // Phase 2: Reopen, mutate further (add key, update status, create second catalog).
        let (kid, cid2);
        {
            let engine = persistent_engine(dir.path());
            // Verify phase 1 data survived.
            assert_eq!(engine.get_tenant(tid).unwrap().name, "acme");
            assert_eq!(engine.get_catalog(cid).unwrap().name, "db");

            engine
                .update_catalog_status(cid, CatalogStatus::Active)
                .unwrap();
            kid = engine
                .create_api_key(tid, vec![Scope::TenantAdmin])
                .unwrap()
                .0;
            cid2 = engine
                .create_catalog(tid, "events".into(), EngineType::Mq, "{}".into())
                .unwrap()
                .0;
            engine.report_usage(cid, 1024, 2048).unwrap();
        }

        // Phase 3: Reopen again, verify all mutations from both phases.
        {
            let engine = persistent_engine(dir.path());
            assert_eq!(engine.get_tenant(tid).unwrap().name, "acme");
            assert_eq!(engine.get_catalog(cid).unwrap().status, CatalogStatus::Active);
            assert_eq!(engine.get_catalog(cid2).unwrap().name, "events");
            assert!(!engine.get_api_key(kid).unwrap().revoked);
            let u = engine.get_usage(cid).unwrap();
            assert_eq!(u.disk_bytes, 1024);
            assert_eq!(u.deep_storage_bytes, 2048);

            // Mutate in phase 3.
            engine.revoke_api_key(kid).unwrap();
            engine
                .update_catalog_status(cid2, CatalogStatus::Deleted)
                .unwrap();
        }

        // Phase 4: Final reopen — verify phase 3 mutations.
        {
            let engine = persistent_engine(dir.path());
            assert!(engine.get_api_key(kid).unwrap().revoked);
            assert_eq!(
                engine.get_catalog(cid2).unwrap().status,
                CatalogStatus::Deleted
            );
            // ID counters should continue correctly.
            let t2 = engine
                .create_tenant(TEST_ACCOUNT, "beta".into(), TenantLimits::default())
                .unwrap();
            assert_eq!(t2, 2);
            let (c3, g3) = engine
                .create_catalog(t2, "logs".into(), EngineType::LibSql, "{}".into())
                .unwrap();
            assert_eq!(c3, 3);
            assert_eq!(g3, 3);
        }
    }

    #[test]
    fn test_persistent_verify_user_password_after_reopen() {
        let dir = tempfile::tempdir().unwrap();

        {
            let engine = persistent_engine(dir.path());
            let pw_hash = crate::token::hash_password(b"my-secret-pw");
            engine
                .create_user("alice".into(), pw_hash)
                .unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            // Correct password should verify.
            let user = engine.verify_user_password("alice", b"my-secret-pw");
            assert!(user.is_some());
            assert_eq!(user.unwrap().username, "alice");

            // Wrong password should fail.
            assert!(engine.verify_user_password("alice", b"wrong").is_none());

            // Unknown user should fail.
            assert!(engine.verify_user_password("bob", b"my-secret-pw").is_none());
        }
    }

    #[test]
    fn test_persistent_disabled_user_password_rejected_after_reopen() {
        let dir = tempfile::tempdir().unwrap();

        {
            let engine = persistent_engine(dir.path());
            let pw_hash = crate::token::hash_password(b"secret");
            let uid = engine.create_user("carol".into(), pw_hash).unwrap();
            engine.disable_user(uid).unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            // Disabled user should not verify even with correct password.
            assert!(engine.verify_user_password("carol", b"secret").is_none());
        }
    }

    #[test]
    fn test_persistent_timestamps_preserved() {
        let dir = tempfile::tempdir().unwrap();

        let (account_created_at, tenant_created_at, catalog_created_at, key_created_at, user_created_at);
        {
            let engine = persistent_engine(dir.path());
            account_created_at = engine.get_account(TEST_ACCOUNT).unwrap().created_at;

            let tid = engine
                .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
                .unwrap();
            tenant_created_at = engine.get_tenant(tid).unwrap().created_at;

            let (cid, _) = engine
                .create_catalog(tid, "db".into(), EngineType::Lance, "{}".into())
                .unwrap();
            catalog_created_at = engine.get_catalog(cid).unwrap().created_at;

            let (kid, _) = engine
                .create_api_key(tid, vec![Scope::TenantAdmin])
                .unwrap();
            key_created_at = engine.get_api_key(kid).unwrap().created_at;

            let uid = engine
                .create_user("alice".into(), b"hash".to_vec())
                .unwrap();
            user_created_at = engine.get_user(uid).unwrap().created_at;
        }

        {
            let engine = persistent_engine(dir.path());
            assert_eq!(engine.get_account(TEST_ACCOUNT).unwrap().created_at, account_created_at);
            assert_ne!(account_created_at, 0);

            assert_eq!(engine.get_tenant(1).unwrap().created_at, tenant_created_at);
            assert_ne!(tenant_created_at, 0);

            assert_eq!(engine.get_catalog(1).unwrap().created_at, catalog_created_at);
            assert_ne!(catalog_created_at, 0);

            assert_eq!(engine.get_api_key(1).unwrap().created_at, key_created_at);
            assert_ne!(key_created_at, 0);

            assert_eq!(engine.get_user(1).unwrap().created_at, user_created_at);
            assert_ne!(user_created_at, 0);
        }
    }

    #[test]
    fn test_in_memory_engine_persist_is_noop() {
        // Ensure `new()` engine doesn't panic on any mutation.
        let engine = test_engine();
        let tid = engine
            .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
            .unwrap();
        let (cid, gid) = engine
            .create_catalog(tid, "db".into(), EngineType::Lance, "{}".into())
            .unwrap();
        engine
            .update_catalog_status(cid, CatalogStatus::Active)
            .unwrap();
        engine
            .update_catalog_placement(cid, vec![1, 2])
            .unwrap();
        engine.update_leader(gid, Some(1)).unwrap();

        let uid = engine
            .create_user("bob".into(), b"hash".to_vec())
            .unwrap();
        engine
            .add_membership(uid, TEST_ACCOUNT, AccountRole::Admin)
            .unwrap();
        engine
            .set_account_role(uid, TEST_ACCOUNT, AccountRole::Member)
            .unwrap();
        engine.remove_membership(uid, TEST_ACCOUNT).unwrap();

        engine
            .grant_tenant_access(uid, tid, CatalogAccess::All)
            .unwrap();
        engine.revoke_tenant_access(uid, tid).unwrap();

        let (kid, _) = engine
            .create_api_key(tid, vec![Scope::TenantAdmin])
            .unwrap();
        engine.revoke_api_key(kid).unwrap();

        engine.report_usage(cid, 100, 200).unwrap();
        engine.update_tenant_limits(tid, TenantLimits::default()).unwrap();
        engine.update_password(uid, b"new-hash".to_vec()).unwrap();
        engine.disable_user(uid).unwrap();

        // delete_catalog marks as Deleting; need Deleted status to allow tenant deletion.
        engine.delete_catalog(tid, cid).unwrap();
        engine
            .update_catalog_status(cid, CatalogStatus::Deleted)
            .unwrap();
        engine.delete_tenant(tid).unwrap();
        engine.delete_account(TEST_ACCOUNT).unwrap();

        // Restore from snapshot should also be fine.
        let snap = engine.snapshot_data();
        engine.restore_from_snapshot(snap);
    }

    #[test]
    fn test_persistent_multi_account_isolation() {
        let dir = tempfile::tempdir().unwrap();

        let (acct1, acct2);
        {
            let engine =
                MetaEngine::open(MetaConfig::new(b"test-secret".to_vec()), dir.path()).unwrap();
            acct1 = engine.create_account("org-alpha".into()).unwrap();
            acct2 = engine.create_account("org-beta".into()).unwrap();

            let t1 = engine
                .create_tenant(acct1, "alpha-tenant".into(), TenantLimits::default())
                .unwrap();
            let t2 = engine
                .create_tenant(acct2, "beta-tenant".into(), TenantLimits::default())
                .unwrap();

            engine
                .create_catalog(t1, "shared_name".into(), EngineType::Lance, "{}".into())
                .unwrap();
            engine
                .create_catalog(t2, "shared_name".into(), EngineType::Mq, "{}".into())
                .unwrap();

            engine
                .create_api_key(t1, vec![Scope::Catalog("shared_name".into())])
                .unwrap();
            engine
                .create_api_key(t2, vec![Scope::TenantAdmin])
                .unwrap();
        }

        {
            let engine =
                MetaEngine::open(MetaConfig::new(b"test-secret".to_vec()), dir.path()).unwrap();
            assert_eq!(engine.list_accounts().len(), 2);

            // Each account has one tenant.
            let a1_tenants: Vec<_> = engine
                .list_tenants()
                .into_iter()
                .filter(|t| t.account_id == acct1)
                .collect();
            let a2_tenants: Vec<_> = engine
                .list_tenants()
                .into_iter()
                .filter(|t| t.account_id == acct2)
                .collect();
            assert_eq!(a1_tenants.len(), 1);
            assert_eq!(a2_tenants.len(), 1);
            assert_eq!(a1_tenants[0].name, "alpha-tenant");
            assert_eq!(a2_tenants[0].name, "beta-tenant");

            // Same catalog name, different tenants, different engines.
            let c1 = engine
                .find_catalog_by_name(a1_tenants[0].tenant_id, "shared_name")
                .unwrap();
            let c2 = engine
                .find_catalog_by_name(a2_tenants[0].tenant_id, "shared_name")
                .unwrap();
            assert_eq!(c1.engine, EngineType::Lance);
            assert_eq!(c2.engine, EngineType::Mq);
            assert_ne!(c1.catalog_id, c2.catalog_id);

            // API keys belong to correct tenants.
            let k1 = engine.get_api_key(1).unwrap();
            let k2 = engine.get_api_key(2).unwrap();
            assert_eq!(k1.tenant_id, a1_tenants[0].tenant_id);
            assert_eq!(k2.tenant_id, a2_tenants[0].tenant_id);
            assert_eq!(k1.scopes, vec![Scope::Catalog("shared_name".into())]);
            assert_eq!(k2.scopes, vec![Scope::TenantAdmin]);
        }
    }

    #[test]
    fn test_persistent_aggregate_usage_after_reopen() {
        let dir = tempfile::tempdir().unwrap();

        let tid;
        {
            let engine = persistent_engine(dir.path());
            tid = engine
                .create_tenant(TEST_ACCOUNT, "acme".into(), TenantLimits::default())
                .unwrap();
            let (c1, _) = engine
                .create_catalog(tid, "a".into(), EngineType::Lance, "{}".into())
                .unwrap();
            let (c2, _) = engine
                .create_catalog(tid, "b".into(), EngineType::Mq, "{}".into())
                .unwrap();
            let (c3, _) = engine
                .create_catalog(tid, "c".into(), EngineType::LibSql, "{}".into())
                .unwrap();
            engine.report_usage(c1, 100, 200).unwrap();
            engine.report_usage(c2, 300, 400).unwrap();
            engine.report_usage(c3, 500, 600).unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            let (disk, deep) = engine.get_tenant_usage(tid);
            assert_eq!(disk, 900);
            assert_eq!(deep, 1200);
        }
    }

    #[test]
    fn test_persistent_account_members_after_reopen() {
        let dir = tempfile::tempdir().unwrap();

        {
            let engine = persistent_engine(dir.path());
            let u1 = engine
                .create_user("admin".into(), b"h1".to_vec())
                .unwrap();
            let u2 = engine
                .create_user("member".into(), b"h2".to_vec())
                .unwrap();
            engine
                .add_membership(u1, TEST_ACCOUNT, AccountRole::Admin)
                .unwrap();
            engine
                .add_membership(u2, TEST_ACCOUNT, AccountRole::Member)
                .unwrap();
        }

        {
            let engine = persistent_engine(dir.path());
            let members = engine.get_account_members(TEST_ACCOUNT);
            assert_eq!(members.len(), 2);

            let admin = members.iter().find(|(u, _)| u.username == "admin").unwrap();
            assert_eq!(admin.1, AccountRole::Admin);

            let member = members.iter().find(|(u, _)| u.username == "member").unwrap();
            assert_eq!(member.1, AccountRole::Member);
        }
    }
}
