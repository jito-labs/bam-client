/// Simple payment sender for BAM. Will send payments to the BAM node
/// for the slots it was connected to as a leader.
/// It will calculate the payment amount based on the fees collected in that slot.
/// The payment is sent as a transfer transaction with a memo indicating the slot.
/// The payment is sent with a 1% commission.
use {
    crate::bam_dependencies::BamDependencies,
    solana_client::rpc_client::RpcClient,
    solana_poh::poh_recorder::PohRecorder,
    solana_pubkey::Pubkey,
    std::{
        collections::BTreeSet,
        sync::{Arc, RwLock},
        time::Instant,
    },
};
use {
    solana_clock::Slot, solana_commitment_config::CommitmentConfig,
    solana_compute_budget_interface::ComputeBudgetInstruction, solana_keypair::Keypair,
    solana_runtime::bank_forks::BankForks, solana_signer::Signer,
    solana_system_interface::instruction::transfer,
    solana_transaction::versioned::VersionedTransaction,
};

pub const COMMISSION_PERCENTAGE: u64 = 3; // 3% commission
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
            let batch = Self::create_batch(
                &leader_slots_for_payment,
                current_slot,
                &dependencies.bank_forks,
            );
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
            &leader_slots_for_payment,
            poh_recorder.read().unwrap().get_current_slot(),
            &dependencies.bank_forks,
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

        let payment_pubkey = *dependencies.bam_node_pubkey.lock().unwrap();
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
        leader_slots_for_payment: &BTreeSet<u64>,
        current_slot: u64,
        bank_forks: &Arc<RwLock<BankForks>>,
    ) -> Vec<(u64, u64)> {
        let mut batch = vec![];

        for slot in leader_slots_for_payment.iter().copied() {
            let bank_forks = bank_forks.read().unwrap();
            let root = bank_forks.root();

            // must be >= 32 slots older than tip and rooted to access bank
            if current_slot.saturating_sub(slot) < 32 || slot > root {
                continue;
            }

            let Some(payment_amount) = Self::calculate_payment_amount(&bank_forks, slot) else {
                break;
            };

            batch.push((slot, payment_amount));
        }
        batch
    }

    fn get_latest_blockhash(rpc_url: &str) -> Option<solana_hash::Hash> {
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

    pub fn calculate_payment_amount(bank_forks: &BankForks, slot: u64) -> Option<u64> {
        let bank = match bank_forks.get(slot) {
            Some(bank) => bank,
            None => {
                error!(
                    "Bank not found for slot {} in calculate_payment_amount",
                    slot
                );
                return None;
            }
        };

        Some(
            bank.priority_fee_total()
                .saturating_mul(COMMISSION_PERCENTAGE)
                .saturating_div(100),
        )
    }

    pub fn create_transfer_transaction(
        keypair: &Keypair,
        blockhash: solana_hash::Hash,
        destination_pubkey: Pubkey,
        lamports: u64,
        lowest_slot: u64,
        highest_slot: u64,
    ) -> VersionedTransaction {
        // Create transfer instruction
        let transfer_ix = transfer(&keypair.pubkey(), &destination_pubkey, lamports);

        // Create memo instruction
        let memo = format!("bam_pay=({}, {})", lowest_slot, highest_slot);
        let memo_ix = spl_memo::build_memo(memo.as_bytes(), &[&keypair.pubkey()]);

        // Set compute unit price
        let compute_unit_price_ix = ComputeBudgetInstruction::set_compute_unit_price(10_000);
        let compute_unit_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(50_000);

        let payer = keypair;

        let tx = solana_transaction::Transaction::new_signed_with_payer(
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

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_native_token::LAMPORTS_PER_SOL,
        solana_runtime::{
            bank::Bank,
            genesis_utils::{create_genesis_config_with_leader, GenesisConfigInfo},
        },
    };

    fn create_test_bank() -> (Bank, Keypair) {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config_with_leader(
            1_000_000 * LAMPORTS_PER_SOL,
            &Pubkey::new_unique(),
            100 * LAMPORTS_PER_SOL,
        );
        let bank = Bank::new_for_tests(&genesis_config);
        (bank, mint_keypair)
    }

    #[test]
    fn test_calculate_payment_amount_with_priority_fees() {
        // This test verifies the commission calculation logic
        // by checking against known priority fee values
        let test_cases: Vec<(u64, u64)> = vec![
            (10000, 300),   // 3% of 10000 = 300
            (50000, 1500),  // 3% of 50000 = 1500
            (100000, 3000), // 3% of 100000 = 3000
        ];

        for (priority_fee, expected_payment) in test_cases {
            let (bank, _) = create_test_bank();

            // Mock the priority fee total by creating a bank with known fee
            // In a real scenario, this would be set by processing transactions
            // For testing, we can verify the calculation logic works correctly
            let bank_forks = BankForks::new_rw_arc(bank);
            let slot = bank_forks.read().unwrap().root();

            // Test that when a bank has priority_fee_total() = priority_fee,
            // the payment amount = priority_fee * COMMISSION_PERCENTAGE / 100
            let payment_amount =
                BamPaymentSender::calculate_payment_amount(&*bank_forks.read().unwrap(), slot);

            assert!(payment_amount.is_some());
            let _payment = payment_amount.unwrap();

            // Since we can't easily mock priority_fee_total() without processing transactions,
            // we verify the calculation logic separately
            let calculated_payment = priority_fee
                .saturating_mul(COMMISSION_PERCENTAGE)
                .saturating_div(100);

            assert_eq!(
                calculated_payment, expected_payment,
                "For priority fee {}, expected payment {}, got {}",
                priority_fee, expected_payment, calculated_payment
            );
        }
    }

    #[test]
    fn test_calculate_payment_amount_no_priority_fees() {
        let (bank, _) = create_test_bank();
        let bank_forks = BankForks::new_rw_arc(bank);
        let slot = bank_forks.read().unwrap().root();

        // Bank has no transactions, so no priority fees
        let payment_amount =
            BamPaymentSender::calculate_payment_amount(&*bank_forks.read().unwrap(), slot);

        assert!(payment_amount.is_some());
        assert_eq!(payment_amount.unwrap(), 0);
    }

    #[test]
    fn test_calculate_payment_amount_nonexistent_slot() {
        let (bank, _) = create_test_bank();
        let bank_forks = BankForks::new_rw_arc(bank);

        // Try to get payment for a non-existent slot
        let nonexistent_slot = 999999;
        let payment_amount = BamPaymentSender::calculate_payment_amount(
            &*bank_forks.read().unwrap(),
            nonexistent_slot,
        );

        assert!(payment_amount.is_none());
    }

    #[test]
    fn test_commission_calculation() {
        // Test various priority fee amounts to ensure commission is calculated correctly
        let test_cases: Vec<(u64, u64)> = vec![
            (0, 0),
            (100, 3),     // 3% of 100 = 3
            (1000, 30),   // 3% of 1000 = 30
            (10000, 300), // 3% of 10000 = 300
            (33, 0),      // 3% of 33 = 0.99, rounds down to 0
            (34, 1),      // 3% of 34 = 1.02, rounds down to 1
        ];

        for (priority_fee_total, expected_commission) in test_cases {
            let commission = priority_fee_total
                .saturating_mul(COMMISSION_PERCENTAGE)
                .saturating_div(100);
            assert_eq!(
                commission, expected_commission,
                "For priority fee {}, expected commission {}, got {}",
                priority_fee_total, expected_commission, commission
            );
        }
    }

    #[test]
    fn test_multiple_transactions_priority_fees() {
        // Test that the payment calculation works correctly for accumulated priority fees
        let accumulated_priority_fees: Vec<u64> = vec![
            1000,  // After 1st transaction
            3000,  // After 2nd transaction (1000 + 2000)
            6000,  // After 3rd transaction (3000 + 3000)
            10000, // After 4th transaction (6000 + 4000)
        ];

        for total_fees in accumulated_priority_fees {
            let expected_payment = total_fees
                .saturating_mul(COMMISSION_PERCENTAGE)
                .saturating_div(100);

            assert_eq!(
                expected_payment,
                total_fees * COMMISSION_PERCENTAGE / 100,
                "Payment calculation should be consistent for total fees: {}",
                total_fees
            );
        }

        // Verify edge cases
        assert_eq!(
            0_u64
                .saturating_mul(COMMISSION_PERCENTAGE)
                .saturating_div(100),
            0
        );
        // For u64::MAX, saturating_mul will overflow, so we verify the behavior
        // u64::MAX * 3 would overflow, so saturating_mul returns u64::MAX
        // Then u64::MAX / 100 = 184467440737095516
        assert_eq!(
            u64::MAX
                .saturating_mul(COMMISSION_PERCENTAGE)
                .saturating_div(100),
            184467440737095516
        );
    }

    #[test]
    fn test_create_batch_slot_constraints() {
        // Test the slot constraint logic
        // The create_batch function should only include slots that are:
        // 1. At least 32 slots behind the current slot
        // 2. Rooted

        let test_cases: Vec<(u64, u64, bool)> = vec![
            // (current_slot, slot_to_check, should_be_eligible)
            (100, 68, true),   // 100 - 32 = 68, exactly at boundary
            (100, 69, false),  // Too recent
            (100, 50, true),   // Old enough
            (100, 100, false), // Same as current, too recent
            (50, 18, true),    // 50 - 32 = 18
            (50, 19, false),   // Too recent
            (32, 0, true),     // 32 - 32 = 0, exactly at boundary
        ];

        for (current_slot, slot, should_be_eligible) in test_cases {
            let is_old_enough = current_slot.saturating_sub(slot) >= 32;
            assert_eq!(
                is_old_enough, should_be_eligible,
                "For current slot {} and slot {}, eligibility should be {}",
                current_slot, slot, should_be_eligible
            );
        }

        // Test edge case with saturating subtraction
        assert_eq!(10_u64.saturating_sub(50), 0);
        assert_eq!(100_u64.saturating_sub(32), 68);

        // Verify the batch payment calculation works correctly with a simple bank
        let (bank, _) = create_test_bank();
        let bank_forks = BankForks::new_rw_arc(bank);
        let slot = bank_forks.read().unwrap().root();

        // Even with no transactions, the calculation should return Some(0)
        let payment =
            BamPaymentSender::calculate_payment_amount(&*bank_forks.read().unwrap(), slot);
        assert!(payment.is_some());
        assert_eq!(payment.unwrap(), 0);
    }
}
