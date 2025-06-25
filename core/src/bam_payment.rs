/// Simple payment sender for BAM. Will send payments to the BAM node
/// for the slots it was connected to as a leader.
/// It will calculate the payment amount based on the fees collected in that slot.
/// The payment is sent as a transfer transaction with a memo indicating the slot.
/// The payment is sent with a 1% commission.
use {
    crate::bam_dependencies::BamDependencies,
    solana_client::rpc_client::RpcClient,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::blockstore::Blockstore,
    solana_poh::poh_recorder::PohRecorder,
    solana_pubkey::Pubkey,
    solana_sdk::{
        commitment_config::CommitmentConfig, signer::Signer, transaction::VersionedTransaction,
    },
    std::{
        collections::HashSet,
        sync::{Arc, RwLock},
    },
};

const COMMISSION_PERCENTAGE: u64 = 1; // 1% commission

pub struct BamPaymentSender {
    thread: std::thread::JoinHandle<()>,
    slot_sender: crossbeam_channel::Sender<u64>,
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
        }
    }

    fn run(
        exit: Arc<std::sync::atomic::AtomicBool>,
        slot_receiver: crossbeam_channel::Receiver<u64>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        dependencies: BamDependencies,
    ) {
        let mut leader_slots_for_payment = HashSet::new();

        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            // Receive new potentially new slots
            while let Ok(slot) = slot_receiver.try_recv() {
                leader_slots_for_payment.insert(slot); // Will dedup
            }

            // Review the leader slots that need payment
            let blockstore = poh_recorder.read().unwrap().get_blockstore();
            let payment_pubkey = dependencies.bam_node_pubkey.lock().unwrap().clone();
            let current_slot = poh_recorder.read().unwrap().get_current_slot();
            leader_slots_for_payment.retain(|slot| {
                // Will only be finalized  after 32 slots
                if current_slot.saturating_sub(*slot) < 32 {
                    return true;
                }
                !Self::payment_successful(
                    *slot,
                    &blockstore,
                    &dependencies.cluster_info,
                    &poh_recorder,
                    &payment_pubkey,
                )
            });

            info!("slots_unpaid={:?}", leader_slots_for_payment);

            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    pub fn send_slot(&self, slot: u64) -> bool {
        self.slot_sender.try_send(slot).is_ok()
    }

    pub fn join(self) -> std::thread::Result<()> {
        self.thread.join()
    }

    fn payment_successful(
        slot: u64,
        blockstore: &Blockstore,
        cluster_info: &ClusterInfo,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        bam_node_pubkey: &Pubkey,
    ) -> bool {
        let Some(payment_amount) = Self::calculate_payment_amount(&blockstore, slot) else {
            return false;
        };

        if payment_amount == 0 {
            info!("No payment amount for slot {}. Skipping payment.", slot);
            return true;
        }

        let transfer_txn = Self::create_transfer_transaction(
            cluster_info,
            poh_recorder,
            *bam_node_pubkey,
            payment_amount,
            slot,
        );

        info!(
            "Sending payment of {} lamports for slot {} to BAM node {}",
            payment_amount, slot, bam_node_pubkey
        );

        // Send it via RpcClient (loopback to the same node)
        let rpc_client =
            RpcClient::new_with_commitment("http://localhost:8899", CommitmentConfig::confirmed());
        rpc_client
            .send_and_confirm_transaction(&transfer_txn)
            .is_ok()
    }

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
}
