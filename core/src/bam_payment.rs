/// Simple payment sender for BAM. Will send payments to the BAM node
/// for the slots it was connected to as a leader.
/// It will calculate the payment amount based on the fees collected in that slot.
/// The payment is sent as a transfer transaction with a memo indicating the slot.
/// The payment is sent with a 1% commission.
use {
    crate::bam_dependencies::BamDependencies,
    solana_client::rpc_client::RpcClient,
    solana_ledger::blockstore::{Blockstore, BlockstoreError},
    solana_poh::poh_recorder::PohRecorder,
    solana_pubkey::Pubkey,
    solana_sdk::{
        clock::Slot, commitment_config::CommitmentConfig, compute_budget::ComputeBudgetInstruction,
        signature::Keypair, signer::Signer, transaction::VersionedTransaction,
    },
    std::{
        collections::BTreeSet,
        sync::{Arc, RwLock},
        time::Instant,
    },
};

pub const COMMISSION_PERCENTAGE: u64 = 1; // 1% commission
const LOCALHOST: &str = "http://localhost:8899";

pub struct BamPaymentSender {
    thread: std::thread::JoinHandle<()>,
    slot_sender: crossbeam_channel::Sender<u64>,
    previous_slot: u64,
}

impl BamPaymentSender {
    pub fn new(
        exit: Arc<std::sync::atomic::AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        dependencies: BamDependencies,
    ) -> Self {
        let (slot_sender, slot_receiver) = crossbeam_channel::bounded(10_000);
        Self {
            thread: std::thread::spawn(move || {
                Self::run(exit, slot_receiver, poh_recorder, dependencies);
            }),
            slot_sender,
            previous_slot: 0,
        }
    }

    fn run(
        exit: Arc<std::sync::atomic::AtomicBool>,
        slot_receiver: crossbeam_channel::Receiver<u64>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        dependencies: BamDependencies,
    ) {
        let mut leader_slots_for_payment = BTreeSet::new();
        let blockstore = poh_recorder.read().unwrap().get_blockstore();
        const DURATION_BETWEEN_PAYMENTS: std::time::Duration = std::time::Duration::from_secs(30);
        let mut last_payment_time = Instant::now();

        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            // Receive new potentially new slots
            while let Ok(slot) = slot_receiver.try_recv() {
                leader_slots_for_payment.insert(slot); // Will dedup
            }

            let now = Instant::now();
            if now.duration_since(last_payment_time).as_secs() < DURATION_BETWEEN_PAYMENTS.as_secs()
            {
                std::thread::sleep(std::time::Duration::from_millis(100));
                continue;
            }
            last_payment_time = now;

            // Create batch
            let current_slot = poh_recorder.read().unwrap().get_current_slot();
            let batch = Self::create_batch(&blockstore, &leader_slots_for_payment, current_slot);
            if batch.is_empty() {
                continue;
            }

            // Try to send
            if Self::send_batch(&batch, &dependencies) {
                for (slot, _) in batch.iter() {
                    leader_slots_for_payment.remove(slot);
                }
                info!("Payment sent successfully for slots: {:?}", batch);
            }
            info!("slots_unpaid={:?}", leader_slots_for_payment);
        }

        let final_batch = Self::create_batch(
            &blockstore,
            &leader_slots_for_payment,
            poh_recorder.read().unwrap().get_current_slot(),
        );
        Self::send_batch(&final_batch, &dependencies);
        warn!(
            "BamPaymentSender thread exiting, final batch: {:?} remaining slots: {:?}",
            final_batch, leader_slots_for_payment
        );
    }

    pub fn send_batch(batch: &[(u64, u64)], dependencies: &BamDependencies) -> bool {
        let total_payment = batch.iter().map(|(_, amount)| *amount).sum::<u64>();
        if total_payment == 0 {
            return true;
        }

        let payment_pubkey = dependencies.bam_node_pubkey.lock().unwrap().clone();
        let rpc_url = dependencies
            .cluster_info
            .my_contact_info()
            .rpc()
            .map_or_else(|| LOCALHOST.to_string(), |rpc| rpc.to_string());

        let ((lowest_slot, _), (highest_slot, _)) = (batch.first().unwrap(), batch.last().unwrap());

        info!(
            "Sending payment for {} slots, range: ({}, {}), total payment: {}",
            batch.len(),
            lowest_slot,
            highest_slot,
            total_payment
        );
        let Some(blockhash) = Self::get_latest_blockhash(&rpc_url) else {
            error!("Failed to get latest blockhash, skipping payment");
            return false;
        };
        let batch_txn = Self::create_transfer_transaction(
            dependencies.cluster_info.keypair().as_ref(),
            blockhash,
            payment_pubkey,
            total_payment,
            *lowest_slot,
            *highest_slot,
        );

        Self::payment_successful(&rpc_url, &batch_txn, *lowest_slot, *highest_slot)
    }

    pub fn send_slot(&mut self, slot: Slot) -> bool {
        if slot <= self.previous_slot {
            return false;
        }
        self.previous_slot = slot;
        self.slot_sender.try_send(slot).is_ok()
    }

    pub fn join(self) -> std::thread::Result<()> {
        self.thread.join()
    }

    fn create_batch(
        blockstore: &Blockstore,
        leader_slots_for_payment: &BTreeSet<u64>,
        current_slot: u64,
    ) -> Vec<(u64, u64)> {
        let mut batch = vec![];
        for slot in leader_slots_for_payment.iter() {
            if current_slot.saturating_sub(*slot) < 32 {
                continue;
            }
            let Some(payment_amount) = Self::calculate_payment_amount(&blockstore, *slot) else {
                break;
            };

            batch.push((*slot, payment_amount));
        }
        batch
    }

    fn get_latest_blockhash(rpc_url: &str) -> Option<solana_sdk::hash::Hash> {
        let rpc_client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());
        rpc_client.get_latest_blockhash().ok()
    }

    fn payment_successful(
        rpc_url: &str,
        txn: &VersionedTransaction,
        lowest_slot: Slot,
        highest_slot: Slot,
    ) -> bool {
        // Send it via RpcClient (loopback to the same node)
        let rpc_client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());
        if let Err(err) = rpc_client.send_and_confirm_transaction(txn) {
            error!(
                "Failed to send payment transaction for slot range ({}, {}): {}",
                lowest_slot, highest_slot, err
            );
            false
        } else {
            info!(
                "Payment for slot range ({}, {}) sent successfully; signature: {:?}",
                lowest_slot,
                highest_slot,
                txn.signatures.first()
            );
            true
        }
    }

    pub fn calculate_payment_amount(blockstore: &Blockstore, slot: u64) -> Option<u64> {
        let result = blockstore.get_rooted_block(slot, false);
        if result.is_err() && !matches!(result, Err(BlockstoreError::SlotNotRooted)) {
            error!("Failed to get block for slot {}: {:?}", slot, result);
            return Some(0);
        }

        let Ok(block) = result else {
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
        keypair: &Keypair,
        blockhash: solana_sdk::hash::Hash,
        destination_pubkey: Pubkey,
        lamports: u64,
        lowest_slot: u64,
        highest_slot: u64,
    ) -> VersionedTransaction {
        // Create transfer instruction
        let transfer_ix = solana_sdk::system_instruction::transfer(
            &keypair.pubkey(),
            &destination_pubkey,
            lamports,
        );

        // Create memo instruction
        let memo = format!("bam_pay=({}, {})", lowest_slot, highest_slot);
        let memo_ix = spl_memo::build_memo(memo.as_bytes(), &[&keypair.pubkey()]);

        // Set compute unit price
        let compute_unit_price_ix = ComputeBudgetInstruction::set_compute_unit_price(10_000);
        let compute_unit_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(50_000);

        let payer = keypair;

        let tx = solana_sdk::transaction::Transaction::new_signed_with_payer(
            &[
                compute_unit_price_ix,
                compute_unit_limit_ix,
                transfer_ix,
                memo_ix,
            ],
            Some(&payer.pubkey()),
            &[payer],
            blockhash,
        );
        VersionedTransaction::from(tx)
    }
}
