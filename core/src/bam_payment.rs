use {
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::blockstore::Blockstore,
    solana_poh::poh_recorder::PohRecorder,
    solana_pubkey::Pubkey,
    solana_sdk::{signer::Signer, transaction::VersionedTransaction},
    std::sync::{Arc, RwLock},
};

const COMMISSION_PERCENTAGE: u64 = 1; // 1% commission

pub fn calculate_payment_amount(blockstore: &Blockstore, slot: u64) -> Option<u64> {
    let Ok(block) = blockstore.get_rooted_block(slot, false) else {
        return None;
    };

    const BASE_FEE_LAMPORT_PER_SIGNATURE: u64 = 5_000;
    Some(
        block
            .transactions
            .iter()
            .map(|tx| {
                let fee = tx.meta.fee;
                let base_fee = BASE_FEE_LAMPORT_PER_SIGNATURE
                    .saturating_mul(tx.transaction.signatures.len() as u64);
                fee.saturating_sub(base_fee)
            })
            .sum::<u64>()
            .saturating_mul(COMMISSION_PERCENTAGE)
            .saturating_div(100),
    )
}

pub fn create_transfer_transaction(
    cluster_info: &ClusterInfo,
    poh_recorder: &Arc<RwLock<PohRecorder>>,
    destination_pubkey: Pubkey,
    lamports: u64,
    slot: u64,
) -> VersionedTransaction {
    // Create transfer instruction
    let transfer_instruction = solana_sdk::system_instruction::transfer(
        &cluster_info.keypair().pubkey(),
        &destination_pubkey,
        lamports,
    );

    // Create memo instruction
    let memo = format!("bam_slot={}", slot);
    let memo_instruction =
        spl_memo::build_memo(memo.as_bytes(), &[&cluster_info.keypair().pubkey()]);

    let payer = cluster_info.keypair();
    let blockhash = poh_recorder
        .read()
        .unwrap()
        .get_poh_recorder_bank()
        .bank()
        .last_blockhash();

    let tx = solana_sdk::transaction::Transaction::new_signed_with_payer(
        &[transfer_instruction, memo_instruction],
        Some(&payer.pubkey()),
        &[payer.as_ref()],
        blockhash,
    );
    VersionedTransaction::from(tx)
}
