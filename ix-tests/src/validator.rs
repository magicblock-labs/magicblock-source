use std::time::Duration;

use anyhow::Context;
use futures::future::try_join_all;
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use solana_rpc_client::{
    api::config::CommitmentConfig, nonblocking::rpc_client::RpcClient,
};
use solana_signer::Signer;
use solana_system_interface::instruction as system_instruction;
use solana_transaction::Transaction;
use tracing::info;

use crate::config::SuiteConfig;

pub struct ValidatorDriver {
    rpc: RpcClient,
    payer: Keypair,
    transaction_timeout: Duration,
}

impl ValidatorDriver {
    pub fn new(config: &SuiteConfig) -> Self {
        let rpc = RpcClient::new_with_commitment(
            config.validator_rpc_url.clone(),
            CommitmentConfig::confirmed(),
        );
        let payer = Keypair::new();
        let transaction_timeout =
            Duration::from_millis(config.transaction_timeout_ms);
        Self {
            rpc,
            payer,
            transaction_timeout,
        }
    }

    pub async fn fund_payer(&self) -> anyhow::Result<()> {
        let lamports = 10_000_000_000; // 10 SOL
        self.rpc
            .request_airdrop(&self.payer.pubkey(), lamports)
            .await
            .context("fund_payer: request_airdrop failed")?;
        info!(
            payer = %self.payer.pubkey(),
            lamports,
            "funded payer"
        );
        Ok(())
    }

    pub async fn airdrop(
        &self,
        pubkey: &Pubkey,
        lamports: u64,
    ) -> anyhow::Result<String> {
        let sig = self
            .rpc
            .request_airdrop(pubkey, lamports)
            .await
            .with_context(|| {
                format!("airdrop to {pubkey} of {lamports} failed")
            })?;
        self.confirm_signature(&sig).await?;
        info!(%pubkey, lamports, %sig, "airdrop confirmed");
        Ok(sig.to_string())
    }

    pub async fn airdrops(
        &self,
        requests: Vec<(Pubkey, u64)>,
    ) -> anyhow::Result<Vec<String>> {
        try_join_all(
            requests
                .iter()
                .map(|(pubkey, lamports)| self.airdrop(pubkey, *lamports)),
        )
        .await
    }

    pub async fn transfer(
        &self,
        from: &Keypair,
        to: &Pubkey,
        lamports: u64,
    ) -> anyhow::Result<String> {
        let ix = system_instruction::transfer(&from.pubkey(), to, lamports);
        let blockhash = self
            .rpc
            .get_latest_blockhash()
            .await
            .context("transfer: get_latest_blockhash failed")?;
        let mut tx = Transaction::new_with_payer(&[ix], Some(&from.pubkey()));
        tx.sign(&[from], blockhash);
        let sig = self
            .rpc
            .send_and_confirm_transaction(&tx)
            .await
            .with_context(|| {
                format!(
                    "transfer {} lamports from {} to {} failed",
                    lamports,
                    from.pubkey(),
                    to
                )
            })?;
        info!(from = %from.pubkey(), %to, lamports, %sig, "transfer confirmed");
        Ok(sig.to_string())
    }

    pub async fn allocate_and_assign(
        &self,
        target: &Keypair,
        space: u64,
        new_owner: Pubkey,
    ) -> anyhow::Result<String> {
        let alloc_ix = system_instruction::allocate(&target.pubkey(), space);
        let assign_ix =
            system_instruction::assign(&target.pubkey(), &new_owner);
        let blockhash = self
            .rpc
            .get_latest_blockhash()
            .await
            .context("allocate_and_assign: get_latest_blockhash failed")?;
        let mut tx = Transaction::new_with_payer(
            &[alloc_ix, assign_ix],
            Some(&self.payer.pubkey()),
        );
        tx.sign(&[&self.payer, target], blockhash);
        let sig = self
            .rpc
            .send_and_confirm_transaction(&tx)
            .await
            .with_context(|| {
                format!(
                    "allocate_and_assign for {} (space={}, owner={}) failed",
                    target.pubkey(),
                    space,
                    new_owner
                )
            })?;
        info!(
            target = %target.pubkey(),
            space,
            new_owner = %new_owner,
            %sig,
            "allocate_and_assign confirmed"
        );
        Ok(sig.to_string())
    }

    pub async fn rent_exempt_balance(&self, space: u64) -> anyhow::Result<u64> {
        self.rpc
            .get_minimum_balance_for_rent_exemption(space as usize)
            .await
            .with_context(|| {
                format!("failed to fetch rent-exempt balance for {space} bytes")
            })
    }

    async fn confirm_signature(
        &self,
        sig: &solana_signature::Signature,
    ) -> anyhow::Result<()> {
        let deadline = tokio::time::Instant::now() + self.transaction_timeout;
        let mut last_status_error = None;
        loop {
            match self
                .rpc
                .get_signature_status_with_commitment(
                    sig,
                    CommitmentConfig::confirmed(),
                )
                .await
            {
                Ok(Some(Ok(()))) => return Ok(()),
                Ok(Some(Err(err))) => {
                    anyhow::bail!("transaction {sig} failed: {err:?}");
                }
                Ok(None) => {}
                Err(err) => {
                    last_status_error = Some(err.to_string());
                }
            }

            if tokio::time::Instant::now() >= deadline {
                if let Some(error) = last_status_error {
                    anyhow::bail!(
                        "transaction {sig} not confirmed within {:?}; last status check error: {error}",
                        self.transaction_timeout
                    );
                }
                anyhow::bail!(
                    "transaction {sig} not confirmed within {:?}",
                    self.transaction_timeout
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}
