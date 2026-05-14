use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub id: String,
    pub addr: String,
    pub db: u8,
}

pub struct ClientState {
    pub client_info: ClientInfo,
    pub watched_keys: Vec<String>,
    pub watched_keys_versions: Vec<(String, u64)>,
    pub queued_commands: Vec<String>,
    pub in_transaction: bool,
}

impl ClientState {
    pub fn new(id: String, addr: String) -> Self {
        ClientState {
            client_info: ClientInfo { id, addr, db: 0 },
            watched_keys: Vec::new(),
            watched_keys_versions: Vec::new(),
            queued_commands: Vec::new(),
            in_transaction: false,
        }
    }

    pub fn watch(&mut self, keys: Vec<String>, versions: Vec<(String, u64)>) {
        self.watched_keys = keys;
        self.watched_keys_versions = versions;
    }

    pub fn unwatch(&mut self) {
        self.watched_keys.clear();
        self.watched_keys_versions.clear();
    }

    pub fn multi(&mut self) {
        self.in_transaction = true;
    }

    pub fn discard(&mut self) {
        self.in_transaction = false;
        self.queued_commands.clear();
        self.watched_keys.clear();
        self.watched_keys_versions.clear();
    }

    pub fn exec(&mut self) -> Vec<String> {
        self.in_transaction = false;
        let commands = self.queued_commands.clone();
        self.queued_commands.clear();
        self.watched_keys.clear();
        self.watched_keys_versions.clear();
        commands
    }

    pub fn queue_command(&mut self, cmd: String) {
        if self.in_transaction {
            self.queued_commands.push(cmd);
        }
    }

    pub fn watched_keys_versions(&self) -> &[(String, u64)] {
        &self.watched_keys_versions
    }

    pub fn has_watched_keys(&self) -> bool {
        !self.watched_keys_versions.is_empty()
    }
}

pub type SharedClientState = Arc<Mutex<ClientState>>;

pub fn new_client_state(id: String, addr: String) -> SharedClientState {
    Arc::new(Mutex::new(ClientState::new(id, addr)))
}
