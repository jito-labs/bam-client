use {
    crossbeam_channel::{Receiver, Sender},
    jito_protos::proto::bam_types::AtomicTxnBatchResult,
    log::info,
    solana_compute_budget_interface::ComputeBudgetInstruction,
    solana_core::{
        bam_dependencies::BamOutboundMessage,
        verified_bam_packet_batch::{BamPacketBatchMeta, VerifiedBamPacketBatch},
    },
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_message::Message,
    solana_perf::packet::{BytesPacket, PacketBatch},
    solana_poh::poh_recorder::SharedWorkingBank,
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    solana_signature::Signature,
    solana_signer::Signer,
    solana_system_interface::instruction::transfer,
    solana_transaction::Transaction,
    std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::{self, JoinHandle},
        time::Instant,
    },
};

// transfer transaction cost = 1 * SIGNATURE_COST +
//                             2 * WRITE_LOCK_UNITS +
//                             1 * system_program
//                           = 1470 CU
const TRANSFER_TRANSACTION_COST: u32 = 1470;

fn make_transfer_transaction_with_compute_unit_price(
    from_keypair: &Keypair,
    to: &Pubkey,
    lamports: u64,
    recent_blockhash: Hash,
    compute_unit_price: u64,
) -> Transaction {
    let mut message = Message::new(
        &[
            transfer(&from_keypair.pubkey(), to, lamports),
            ComputeBudgetInstruction::set_compute_unit_price(compute_unit_price),
            ComputeBudgetInstruction::set_compute_unit_limit(TRANSFER_TRANSACTION_COST),
        ],
        Some(&from_keypair.pubkey()),
    );
    message.recent_blockhash = recent_blockhash;

    // skip signing because we are not sending the transaction to the network and it slows down tx sending
    let mut tx = Transaction::new_unsigned(message);
    tx.signatures = vec![Signature::new_unique(); 1];
    tx
}

struct BamOutboundMessageResult {
    time_received: Instant,
    result: AtomicTxnBatchResult,
}

struct BamTransactionInfo {
    time_sent: Instant,
    #[allow(dead_code)]
    transaction: Transaction,
}

struct BamTransactionAndResult {
    transaction: BamTransactionInfo,
    result: Option<BamOutboundMessageResult>,
}

struct BankStats {
    bank_slot: u64,
    start_time: Instant,

    sent_transactions_and_results: HashMap<u32, BamTransactionAndResult>,
}

impl BankStats {
    fn new(bank_slot: u64) -> Self {
        Self {
            bank_slot,
            start_time: Instant::now(),
            sent_transactions_and_results: HashMap::with_capacity(50_000),
        }
    }

    fn print_stats(&self) {
        let mut time_diffs = self
            .sent_transactions_and_results
            .values()
            .map(|tx_and_result| {
                tx_and_result
                    .result
                    .as_ref()
                    .unwrap()
                    .time_received
                    .duration_since(tx_and_result.transaction.time_sent)
                    .as_millis()
            })
            .collect::<Vec<_>>();
        time_diffs.sort();

        let num_outside_leader_slot = self
            .sent_transactions_and_results
            .values()
            .filter(|result| {
                if let Some(res) = &result.result {
                    if let Some(jito_protos::proto::bam_types::atomic_txn_batch_result::Result::NotCommitted(not_committed)) = &res.result.result {
                        if let Some(jito_protos::proto::bam_types::not_committed::Reason::SchedulingError(error)) = &not_committed.reason {
                            return *error == jito_protos::proto::bam_types::SchedulingError::OutsideLeaderSlot as i32;
                        }
                    }
                }
                false
            })
            .count();

        let median_time_diff = time_diffs[time_diffs.len() / 2];
        let average_time_diff = time_diffs.iter().sum::<u128>() / time_diffs.len() as u128;
        let max_time_diff = time_diffs.iter().max().unwrap();
        let min_time_diff = time_diffs.iter().min().unwrap();
        let num_committed = self
            .sent_transactions_and_results
            .values()
            .filter(|result| {
                matches!(
                    result.result.as_ref().unwrap().result.result,
                    Some(
                        jito_protos::proto::bam_types::atomic_txn_batch_result::Result::Committed(
                            _
                        )
                    )
                )
            })
            .count();

        println!(
            "==> bank slot: {}, elapsed: {}ms, transactions sent: {} transactions landed: {} outside leader slot: {}",
            self.bank_slot,
            self.start_time.elapsed().as_millis(),
            self.sent_transactions_and_results.len(),
            num_committed,
            num_outside_leader_slot
        );
        println!(
            "==> rtt: median time diff: {}ms, average time diff: {}ms, max time diff: {}ms, min time diff: {}ms",
            median_time_diff, average_time_diff, max_time_diff, min_time_diff
        );
    }
}

pub(crate) struct MockBamServer;

impl MockBamServer {
    pub(crate) fn run(
        batch_sender: Sender<VerifiedBamPacketBatch>,
        outbound_receiver: Receiver<BamOutboundMessage>,
        shared_working_bank: SharedWorkingBank,
        exit: Arc<AtomicBool>,
        keypairs: Vec<Keypair>,
    ) -> JoinHandle<()> {
        let mut bank_stats = BankStats::new(shared_working_bank.load().unwrap().slot());

        let mut nonce = 0;
        let mut seq_id = 0;

        thread::spawn(move || loop {
            while !exit.load(Ordering::Relaxed) {
                let Some(bank) = shared_working_bank.load() else {
                    continue;
                };

                if bank.slot() != bank_stats.bank_slot {
                    Self::wait_for_all_results(&outbound_receiver, &mut bank_stats);

                    bank_stats.print_stats();
                    bank_stats = BankStats::new(bank.slot());
                    seq_id = 0;
                    nonce = 0;
                }

                Self::send_transactions(
                    &keypairs,
                    &mut bank_stats,
                    &batch_sender,
                    &bank,
                    &mut nonce,
                    &mut seq_id,
                );

                Self::handle_outbound_messages(&outbound_receiver, &mut bank_stats);
            }
        })
    }

    fn handle_outbound_messages(
        outbound_receiver: &Receiver<BamOutboundMessage>,
        bank_stats: &mut BankStats,
    ) {
        while let Ok(msg) = outbound_receiver.try_recv() {
            match msg {
                BamOutboundMessage::AtomicTxnBatchResult(result) => {
                    let transaction_info = bank_stats
                        .sent_transactions_and_results
                        .get_mut(&result.seq_id)
                        .unwrap();
                    transaction_info.result = Some(BamOutboundMessageResult {
                        time_received: Instant::now(),
                        result,
                    });
                }
                _msg => {
                    panic!("unexpected message");
                }
            }
        }
    }

    fn wait_for_all_results(
        outbound_receiver: &Receiver<BamOutboundMessage>,
        bank_stats: &mut BankStats,
    ) {
        loop {
            let num_results_received = bank_stats
                .sent_transactions_and_results
                .iter()
                .filter(|(_, result)| result.result.is_some())
                .count();
            if num_results_received == bank_stats.sent_transactions_and_results.len() {
                break;
            }
            info!(
                "waiting for {} results",
                bank_stats.sent_transactions_and_results.len() - num_results_received
            );
            Self::handle_outbound_messages(outbound_receiver, bank_stats);
        }
    }

    fn send_transactions(
        keypairs: &[Keypair],
        bank_stats: &mut BankStats,
        batch_sender: &Sender<VerifiedBamPacketBatch>,
        bank: &Arc<Bank>,
        nonce: &mut u64,
        seq_id: &mut u32,
    ) {
        for keypair in keypairs.iter() {
            let tx = make_transfer_transaction_with_compute_unit_price(
                keypair,
                &keypair.pubkey(),
                *nonce % 1_000_000,
                bank.last_blockhash(),
                1,
            );

            let packet = BytesPacket::from_data(None, &tx).unwrap();
            let verfied_packet_batch = VerifiedBamPacketBatch::new(
                PacketBatch::from(vec![packet]),
                BamPacketBatchMeta {
                    discard: false,
                    seq_id: *seq_id,
                    max_schedule_slot: bank.slot(),
                    revert_on_error: false,
                },
            );

            batch_sender.send(verfied_packet_batch).unwrap();

            bank_stats.sent_transactions_and_results.insert(
                *seq_id,
                BamTransactionAndResult {
                    transaction: BamTransactionInfo {
                        time_sent: Instant::now(),
                        transaction: tx,
                    },
                    result: None,
                },
            );
            *nonce += 1;
            *seq_id += 1;
        }
    }
}
