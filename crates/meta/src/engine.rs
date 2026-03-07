//! In-memory control plane state.
//!
//! `MetaEngine` holds the registries of tenants, catalogs, API keys, and routing
//! entries that the state machine reads and mutates. Analogous to `BisqueLance`
//! in the lance crate.

use std::collections::HashMap;

use base64::Engine as _;
use parking_lot::RwLock;
use tracing::info;

use crate::config::MetaConfig;
use crate::error::{Error, Result};
use crate::token;
use crate::types::*;

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
}

impl MetaEngine {
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
        info!(user_id, "disabled user");
        Ok(())
    }

    pub fn update_password(&self, user_id: u64, password_hash: Vec<u8>) -> Result<()> {
        let mut users = self.users.write();
        let user = users
            .get_mut(&user_id)
            .ok_or(Error::NotFound(format!("user {user_id}")))?;
        user.password_hash = password_hash;
        info!(user_id, "updated password");
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
        info!(user_id, account_id, %role, "added account membership");
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
        info!(user_id, account_id, "removed account membership");
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
        info!(user_id, account_id, %role, "set account role");
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
        info!(user_id, tenant_id, "granted tenant access");
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
        info!(user_id, tenant_id, "revoked tenant access");
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
        Ok((key_id, raw_key))
    }

    pub fn revoke_api_key(&self, key_id: u64) -> Result<()> {
        let mut keys = self.api_keys.write();
        let key = keys.get_mut(&key_id).ok_or(Error::ApiKeyNotFound(key_id))?;
        key.revoked = true;
        info!(key_id, "revoked API key");
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
}
