use std::collections::HashMap;

use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use solana_signer::Signer;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NamedAccount {
    SimpleA,
    SimpleB,
    SimpleC,
    SimpleD,
    SharedA,
    SharedB,
    RestartA,
    RestartB,
    OwnerData,
    Hot00,
    Hot01,
    Hot02,
    Hot03,
    Hot04,
    Hot05,
    Hot06,
    Hot07,
    Hot08,
    Hot09,
}

const ALL_NAMED_ACCOUNTS: &[NamedAccount] = &[
    NamedAccount::SimpleA,
    NamedAccount::SimpleB,
    NamedAccount::SimpleC,
    NamedAccount::SimpleD,
    NamedAccount::SharedA,
    NamedAccount::SharedB,
    NamedAccount::RestartA,
    NamedAccount::RestartB,
    NamedAccount::OwnerData,
    NamedAccount::Hot00,
    NamedAccount::Hot01,
    NamedAccount::Hot02,
    NamedAccount::Hot03,
    NamedAccount::Hot04,
    NamedAccount::Hot05,
    NamedAccount::Hot06,
    NamedAccount::Hot07,
    NamedAccount::Hot08,
    NamedAccount::Hot09,
];

pub struct ScenarioAccounts {
    keypairs: HashMap<NamedAccount, Keypair>,
}

#[allow(dead_code)]
impl ScenarioAccounts {
    pub fn new() -> Self {
        let keypairs = ALL_NAMED_ACCOUNTS
            .iter()
            .map(|account| (*account, Keypair::new()))
            .collect();
        Self { keypairs }
    }

    pub fn keypair(&self, account: NamedAccount) -> &Keypair {
        self.keypairs
            .get(&account)
            .expect("ScenarioAccounts missing keypair for account")
    }

    pub fn pubkey(&self, account: NamedAccount) -> Pubkey {
        self.keypair(account).pubkey()
    }

    pub fn pubkey_b58(&self, account: NamedAccount) -> String {
        self.pubkey(account).to_string()
    }
}
