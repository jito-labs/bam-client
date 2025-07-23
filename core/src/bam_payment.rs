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
    std::{
        collections::BTreeSet,
        sync::{Arc, RwLock},
        time::Instant,
    },
};
use {
    solana_clock::Slot, solana_commitment_config::CommitmentConfig,
    solana_compute_budget_interface::ComputeBudgetInstruction, solana_keypair::Keypair,
    solana_message::AccountKeys,
    solana_runtime_transaction::signature_details::get_precompile_signature_details,
    solana_signer::Signer, solana_system_interface::instruction::transfer,
    solana_transaction::versioned::sanitized::SanitizedVersionedTransaction,
    solana_transaction::versioned::VersionedTransaction,
    solana_transaction_status::VersionedTransactionWithStatusMeta,
};

pub const COMMISSION_PERCENTAGE: u64 = 3; // 3% commission

fn count_precompile_sigs(tx: &VersionedTransactionWithStatusMeta) -> u64 {
    let keys: AccountKeys = tx.account_keys();

    let details =
        get_precompile_signature_details(tx.transaction.message.instructions().iter().map(|ix| {
            let program_id = &keys[ix.program_id_index as usize];
            (program_id, (&(*ix)).into())
        }));
    details.num_secp256k1_instruction_signatures
        + details.num_ed25519_instruction_signatures
        + details.num_secp256r1_instruction_signatures
}

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
        blockstore: &Blockstore,
        leader_slots_for_payment: &BTreeSet<u64>,
        current_slot: u64,
    ) -> Vec<(u64, u64)> {
        let mut batch = vec![];
        for slot in leader_slots_for_payment.iter() {
            if current_slot.saturating_sub(*slot) < 32 {
                continue;
            }
            let Some(payment_amount) = Self::calculate_payment_amount(blockstore, *slot) else {
                break;
            };

            batch.push((*slot, payment_amount));
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
                    let txn_signature_fee = BASE_FEE_LAMPORT_PER_SIGNATURE
                        .saturating_mul(tx.transaction.signatures.len() as u64);

                    let precompile_signature_fee = if let Ok(_sanitized_tx) =
                        SanitizedVersionedTransaction::try_from(tx.transaction.clone())
                    {
                        let total_precompile_sigs = count_precompile_sigs(tx);
                        BASE_FEE_LAMPORT_PER_SIGNATURE.saturating_mul(total_precompile_sigs)
                    } else {
                        0
                    };

                    let total_base_fee = txn_signature_fee.saturating_add(precompile_signature_fee);
                    fee.saturating_sub(total_base_fee)
                })
                .sum::<u64>()
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
        solana_sdk::{
            hash::Hash,
            instruction::CompiledInstruction,
            message::{
                v0::{LoadedAddresses, Message as V0Message},
                MessageHeader, VersionedMessage,
            },
            pubkey::Pubkey,
            signature::Signature,
            transaction::VersionedTransaction,
        },
        solana_transaction_status::{TransactionStatusMeta, VersionedTransactionWithStatusMeta},
    };

    fn create_mock_versioned_transaction_with_precompiles() -> VersionedTransactionWithStatusMeta {
        // Create account keys
        let secp256k1_program = solana_sdk::secp256k1_program::id();
        let ed25519_program = solana_sdk::ed25519_program::id();
        let system_program = solana_sdk::system_program::id();
        let payer = Pubkey::new_unique();

        let account_keys = vec![payer, secp256k1_program, ed25519_program, system_program];

        // Create instructions that would invoke precompile programs
        // First byte of data indicates number of signatures for precompile instructions
        // See https://github.com/jito-labs/jito-solana-jds/blob/1ea00f80166788f35d563219c3ce400a92b8c925/runtime-transaction/src/signature_details.rs#L53
        let secp256k1_ix = CompiledInstruction {
            program_id_index: 1, // secp256k1_program index
            accounts: vec![0],   // payer account
            data: vec![1, 2, 3], // 1 signature (first byte = 1)
        };

        let ed25519_ix = CompiledInstruction {
            program_id_index: 2, // ed25519_program index
            accounts: vec![0],   // payer account
            data: vec![2, 5, 6], // 2 signatures (first byte = 2)
        };

        // Create a versioned message
        let message = VersionedMessage::V0(V0Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 2,
            },
            account_keys,
            recent_blockhash: Hash::default(),
            instructions: vec![secp256k1_ix, ed25519_ix],
            address_table_lookups: vec![],
        });

        // Create versioned transaction
        let versioned_tx = VersionedTransaction {
            signatures: vec![Signature::default()],
            message,
        };

        // Create transaction status meta
        let meta = TransactionStatusMeta {
            status: Ok(()),
            fee: 5000,
            pre_balances: vec![1000000, 0, 0, 0],
            post_balances: vec![995000, 0, 0, 0],
            inner_instructions: None,
            log_messages: None,
            pre_token_balances: None,
            post_token_balances: None,
            rewards: None,
            loaded_addresses: LoadedAddresses::default(),
            return_data: None,
            compute_units_consumed: Some(1000),
        };

        VersionedTransactionWithStatusMeta {
            transaction: versioned_tx,
            meta,
        }
    }

    fn create_mock_versioned_transaction_without_precompiles() -> VersionedTransactionWithStatusMeta
    {
        // Create account keys without precompile programs
        let system_program = solana_sdk::system_program::id();
        let payer = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();

        let account_keys = vec![payer, recipient, system_program];

        // Create a system transfer instruction (no precompiles)
        let transfer_ix = CompiledInstruction {
            program_id_index: 2,                              // system_program index
            accounts: vec![0, 1],                             // payer and recipient
            data: vec![2, 0, 0, 0, 232, 3, 0, 0, 0, 0, 0, 0], // transfer instruction data
        };

        // Create a versioned message
        let message = VersionedMessage::V0(V0Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys,
            recent_blockhash: Hash::default(),
            instructions: vec![transfer_ix],
            address_table_lookups: vec![],
        });

        // Create versioned transaction
        let versioned_tx = VersionedTransaction {
            signatures: vec![Signature::default()],
            message,
        };

        // Create transaction status meta
        let meta = TransactionStatusMeta {
            status: Ok(()),
            fee: 5000,
            pre_balances: vec![1000000, 0, 0],
            post_balances: vec![995000, 1000, 0],
            inner_instructions: None,
            log_messages: None,
            pre_token_balances: None,
            post_token_balances: None,
            rewards: None,
            loaded_addresses: LoadedAddresses::default(),
            return_data: None,
            compute_units_consumed: Some(500),
        };

        VersionedTransactionWithStatusMeta {
            transaction: versioned_tx,
            meta,
        }
    }

    #[test]
    fn test_count_precompile_sigs_with_precompiles() {
        let tx_with_precompiles = create_mock_versioned_transaction_with_precompiles();
        let precompile_count = count_precompile_sigs(&tx_with_precompiles);

        // The mock transaction has secp256k1 instruction with 1 signature (first byte = 1)
        // and ed25519 instruction with 2 signatures (first byte = 2)
        // Total expected: 1 + 2 = 3 signatures
        assert_eq!(precompile_count, 3);
    }

    #[test]
    fn test_count_precompile_sigs_without_precompiles() {
        let tx_without_precompiles = create_mock_versioned_transaction_without_precompiles();
        let precompile_count = count_precompile_sigs(&tx_without_precompiles);

        // Transaction with only system program should have 0 precompile signatures
        assert_eq!(
            precompile_count, 0,
            "Transaction without precompiles should have 0 precompile signatures"
        );
    }

    #[test]
    fn test_count_precompile_sigs_empty_instructions() {
        let payer = Pubkey::new_unique();
        let account_keys = vec![payer];

        // Create a transaction with no instructions
        let message = VersionedMessage::V0(V0Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys,
            recent_blockhash: Hash::default(),
            instructions: vec![], // No instructions
            address_table_lookups: vec![],
        });

        let versioned_tx = VersionedTransaction {
            signatures: vec![Signature::default()],
            message,
        };

        let meta = TransactionStatusMeta {
            status: Ok(()),
            fee: 5000,
            pre_balances: vec![1000000],
            post_balances: vec![995000],
            inner_instructions: None,
            log_messages: None,
            pre_token_balances: None,
            post_token_balances: None,
            rewards: None,
            loaded_addresses: LoadedAddresses::default(),
            return_data: None,
            compute_units_consumed: Some(0),
        };

        let tx_empty = VersionedTransactionWithStatusMeta {
            transaction: versioned_tx,
            meta,
        };

        let precompile_count = count_precompile_sigs(&tx_empty);
        assert_eq!(
            precompile_count, 0,
            "Transaction with no instructions should have 0 precompile signatures"
        );
    }
}
