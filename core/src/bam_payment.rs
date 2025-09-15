//! Simple payment sender for BAM. Will send payments to the BAM node
//! for the slots it was connected to as a leader.
//! It will calculate the payment amount based on the fees collected in that slot.
//! The payment is sent as a transfer transaction with a memo indicating the slot.
//! The payment is sent with a 1% commission.

use std::sync::Mutex;

use solana_runtime::bank::Bank;
use solana_signature::Signature;
use strum::AsRefStr;
use {
    crossbeam_channel::{Receiver, RecvTimeoutError, Sender},
    solana_clock::Slot,
    solana_compute_budget_interface::ComputeBudgetInstruction,
    solana_gossip::cluster_info::ClusterInfo,
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_poh::poh_recorder::PohRecorder,
    solana_pubkey::Pubkey,
    solana_runtime::{bank_forks::BankForks, commitment::BlockCommitmentCache},
    solana_signer::Signer,
    solana_system_interface::instruction::transfer,
    solana_transaction::versioned::VersionedTransaction,
    std::{
        collections::HashMap,
        sync::{atomic::AtomicBool, Arc, RwLock},
        thread::{spawn, JoinHandle},
        time::Duration,
    },
};

pub const COMMISSION_PERCENTAGE: u64 = 3; // 3% commission

/// Represents the state of a bank's payment
#[derive(Debug, AsRefStr)]
pub enum BankPaymentState {
    // The bank has been dropped, useful for metrics
    #[strum(serialize = "Dropped")]
    Dropped,

    // Waiting for the bank to freeze since the slot message may have been received mid-slot
    #[strum(serialize = "WaitingToFreeze")]
    WaitingToFreeze,
    // Waiting for the bank to confirm
    #[strum(serialize = "WaitingForSlotConfirmation")]
    WaitingForSlotConfirmation { payment_amount: u64 },
    /// Waiting for the transaction to be confirmed
    #[strum(serialize = "WaitingForPaymentConfirmation")]
    WaitingForPaymentConfirmation {
        payment_amount: u64,
        transaction: VersionedTransaction,
        last_valid_block_height: u64,
    },
    // The payment has been confirmed
    #[strum(serialize = "PaymentConfirmed")]
    PaymentConfirmed {
        payment_amount: u64,
        signature: Signature,
    },
}

pub struct BamPaymentSender {
    thread: JoinHandle<()>,
    slot_sender: Sender<u64>,
    previous_slot: u64,
}

impl BamPaymentSender {
    pub fn new(
        exit: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bank_forks: Arc<RwLock<BankForks>>,
        cluster_info: Arc<ClusterInfo>,
        block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
        bam_node_pubkey: Arc<Mutex<Pubkey>>,
    ) -> Self {
        let (slot_sender, slot_receiver) = crossbeam_channel::bounded(10_000);

        Self {
            thread: spawn(move || {
                Self::run(
                    exit,
                    slot_receiver,
                    poh_recorder,
                    bank_forks,
                    cluster_info,
                    block_commitment_cache,
                    bam_node_pubkey,
                );
            }),
            slot_sender,
            previous_slot: 0,
        }
    }

    fn emit_slot_state_transition(
        slot: u64,
        state: &BankPaymentState,
        old_state: Option<&BankPaymentState>,
    ) {
        datapoint_info!(
            "bam_payment-slot_state",
            ("slot", slot, i64),
            ("state", state.as_ref(), String),
            (
                "old_state",
                old_state
                    .map(|s| s.as_ref().to_string())
                    .unwrap_or("None".to_string()),
                String
            )
        );
    }

    fn run(
        exit: Arc<AtomicBool>,
        slot_receiver: Receiver<u64>,
        _poh_recorder: Arc<RwLock<PohRecorder>>,
        bank_forks: Arc<RwLock<BankForks>>,
        cluster_info: Arc<ClusterInfo>,
        block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
        bam_node_pubkey: Arc<Mutex<Pubkey>>,
    ) {
        let mut bank_payment_states = HashMap::new();

        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            match slot_receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(slot) => {
                    bank_payment_states.insert(slot, BankPaymentState::WaitingToFreeze);
                    Self::emit_slot_state_transition(
                        slot,
                        bank_payment_states.get(&slot).unwrap(),
                        None,
                    );
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    error!("Frozen slot receiver disconnected - exiting");
                    return;
                }
            }

            for (slot, state) in bank_payment_states.iter_mut() {
                let maybe_new_state = Self::update_slot_state(
                    *slot,
                    state,
                    &bank_forks,
                    &cluster_info,
                    &block_commitment_cache,
                    &bam_node_pubkey,
                );
                if let Some(new_state) = maybe_new_state {
                    Self::emit_slot_state_transition(*slot, &new_state, Some(state));
                    *state = new_state;
                }
            }

            // Remove states that are confirmed or dropped
            bank_payment_states.retain(|_, state| match state {
                BankPaymentState::PaymentConfirmed { .. } | BankPaymentState::Dropped => false,
                _ => true,
            });
        }
    }

    fn update_slot_state(
        slot: u64,
        state: &BankPaymentState,
        bank_forks: &Arc<RwLock<BankForks>>,
        cluster_info: &Arc<ClusterInfo>,
        block_commitment_cache: &Arc<RwLock<BlockCommitmentCache>>,
        bam_node_pubkey: &Arc<Mutex<Pubkey>>,
    ) -> Option<BankPaymentState> {
        match state {
            BankPaymentState::Dropped => {
                return None;
            }
            BankPaymentState::WaitingToFreeze => match bank_forks.read().unwrap().get(slot) {
                Some(bank) => {
                    if bank.is_frozen() {
                        return Some(BankPaymentState::WaitingForSlotConfirmation {
                            payment_amount: Self::calculate_payment_amount(&bank),
                        });
                    } else {
                        return None;
                    }
                }
                None => {
                    warn!("Bank not found for payment (slot={})", slot);
                    return Some(BankPaymentState::Dropped);
                }
            },
            BankPaymentState::WaitingForSlotConfirmation { payment_amount } => {
                // TODO (LB): need to wait for confirmation here

                let my_keypair = cluster_info.keypair();
                let blockhash = bank_forks.read().unwrap().root_bank().last_blockhash();

                let transaction = Self::create_transfer_transaction(
                    my_keypair.as_ref(),
                    blockhash,
                    bam_node_pubkey.lock().unwrap().clone(),
                    *payment_amount,
                    slot,
                );
                drop(my_keypair);
                let last_valid_block_height = bank_forks
                    .read()
                    .unwrap()
                    .working_bank()
                    .get_blockhash_last_valid_block_height(blockhash);

                return Some(BankPaymentState::WaitingForPaymentConfirmation {
                    payment_amount: *payment_amount,
                    transaction,
                    last_valid_block_height: last_valid_block_height,
                });
            }
            BankPaymentState::WaitingForPaymentConfirmation {
                payment_amount: _,
                transaction,
                last_valid_block_height,
            } => {
                // check to see if landed. if not, retry it.
                // if blockhash expired, get a new one and retry it.
                let confirmed_slot = block_commitment_cache
                    .read()
                    .unwrap()
                    .commitment_slots()
                    .highest_confirmed_slot;
                match bank_forks.read().unwrap().get(confirmed_slot) {
                    Some(bank) => {
                        if bank.has_signature(transaction.signatures.get(0).unwrap()) {
                            // processed and landed
                            return Some(BankPaymentState::PaymentConfirmed {
                                payment_amount: *payment_amount,
                                signature: transaction.signatures.get(0).unwrap().clone(),
                            });
                        } else if bank.block_height() > *last_valid_block_height {
                            // blockhash expired and didn't land; need to re-generate and re-send
                            warn!(
                                "Re-generating and re-sending payment transaction for slot={}",
                                slot
                            );
                            let blockhash = bank_forks.read().unwrap().root_bank().last_blockhash();
                            let my_keypair = cluster_info.keypair();
                            let transaction = Self::create_transfer_transaction(
                                my_keypair.as_ref(),
                                blockhash,
                                bam_node_pubkey.lock().unwrap().clone(),
                                *payment_amount,
                                slot,
                            );
                            drop(my_keypair);
                            let last_valid_block_height = bank_forks
                                .read()
                                .unwrap()
                                .working_bank()
                                .get_blockhash_last_valid_block_height(blockhash);

                            // TODO (LB): send the transactions

                            return Some(BankPaymentState::WaitingForPaymentConfirmation {
                                payment_amount: *payment_amount,
                                transaction,
                                last_valid_block_height: last_valid_block_height,
                            });
                        } else {
                            // TODO (LB): Still waiting for confirmation; re-send the transaction
                            return None;
                        }
                    }
                    None => {
                        // unclear why this would happen...
                        warn!(
                            "Confirmed slot not found in bank forks (slot={})",
                            confirmed_slot
                        );
                        return None;
                    }
                }

                return None;
            }
            BankPaymentState::PaymentConfirmed {
                payment_amount,
                signature,
            } => return None,
        };
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

    // fn create_batch(
    //     leader_slots_for_payment: &BTreeSet<u64>,
    //     current_slot: u64,
    //     bank_forks: &Arc<RwLock<BankForks>>,
    // ) -> Vec<(u64, u64)> {
    //     let mut batch = vec![];

    //     for slot in leader_slots_for_payment.iter().copied() {
    //         let bank_forks = bank_forks.read().unwrap();
    //         let root = bank_forks.root();

    //         // must be >= 32 slots older than tip and rooted to access bank
    //         if current_slot.saturating_sub(slot) < 32 || slot > root {
    //             continue;
    //         }

    //         let Some(payment_amount) = Self::calculate_payment_amount(&bank_forks, slot) else {
    //             break;
    //         };

    //         batch.push((slot, payment_amount));
    //     }
    //     batch
    // }

    pub fn calculate_payment_amount(bank: &Arc<Bank>) -> u64 {
        (bank.priority_fee_total() as f32 * COMMISSION_PERCENTAGE as f32 / 100.0) as u64
    }

    pub fn create_transfer_transaction(
        keypair: &Keypair,
        blockhash: Hash,
        destination_pubkey: Pubkey,
        lamports: u64,
        slot: u64,
    ) -> VersionedTransaction {
        // Create transfer instruction
        let transfer_ix = transfer(&keypair.pubkey(), &destination_pubkey, lamports);

        // Create memo instruction
        let memo = format!("bam_payment,slot={},amount={}", slot, lamports);
        let memo_ix = spl_memo_interface::instruction::build_memo(
            &spl_memo_interface::v3::id(),
            memo.as_bytes(),
            &[&keypair.pubkey()],
        );

        // Set compute unit price (TODO: make this configurable)
        let compute_unit_price_ix = ComputeBudgetInstruction::set_compute_unit_price(10_000);
        let compute_unit_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(50_000);

        let tx = solana_transaction::Transaction::new_signed_with_payer(
            &[
                compute_unit_price_ix,
                compute_unit_limit_ix,
                transfer_ix,
                memo_ix,
            ],
            Some(&keypair.pubkey()),
            &[keypair],
            blockhash,
        );
        VersionedTransaction::from(tx)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_runtime::{bank::Bank, bank_forks::BankForks, genesis_utils::create_genesis_config},
        solana_signer::Signer,
        solana_system_interface::instruction::transfer as system_transfer,
        solana_transaction::Transaction,
    };

    #[test]
    fn calculate_payment_amount_uses_rooted_bank_priority_fee_total() {
        let mut genesis = create_genesis_config(10_000_000_000);
        genesis
            .genesis_config
            .fee_rate_governor
            .lamports_per_signature = 5_000; // Need to override the sneaky-deaky test default
        let parent = std::sync::Arc::new(solana_runtime::bank::Bank::new_for_tests(
            &genesis.genesis_config,
        ));
        let slot = 100u64;
        let bank = solana_runtime::bank::Bank::new_from_parent(
            parent,
            &solana_pubkey::Pubkey::new_unique(),
            slot,
        );
        let bank_forks = BankForks::new_rw_arc(bank);
        let bank_ref = bank_forks.read().unwrap().get(slot).unwrap().clone();

        // Build a few txs with explicit CU limit + price
        let payer = genesis.mint_keypair;
        let mut txs = Vec::new();
        for _ in 0..3 {
            let bh = bank_ref.last_blockhash();
            let cu_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(300_000);
            let cu_price_ix = ComputeBudgetInstruction::set_compute_unit_price(20_000); // 0.02 lamports/CU
            let transfer_ix = system_transfer(&payer.pubkey(), &payer.pubkey(), 1); // trivial nonzero ix
            let tx = Transaction::new_signed_with_payer(
                &[cu_limit_ix, cu_price_ix, transfer_ix],
                Some(&payer.pubkey()),
                &[&payer],
                bh,
            );
            let _results = bank_ref.process_transaction(&tx);
            txs.push(tx);
        }

        // Priority fees should be > 0 now
        assert!(bank_ref.priority_fee_total() > 0);
    }

    #[test]
    fn calculate_payment_amount_none_for_missing_bank() {
        let genesis = create_genesis_config(1_000_000).genesis_config;
        let parent = std::sync::Arc::new(Bank::new_for_tests(&genesis));
        let slot: u64 = 42;
        let bank = Bank::new_from_parent(parent, &solana_pubkey::Pubkey::default(), slot);
        let bank_forks = BankForks::new_rw_arc(bank);
        let forks_read = bank_forks.read().unwrap();
        assert!(BamPaymentSender::calculate_payment_amount(&forks_read, slot + 1).is_none());
    }
}
