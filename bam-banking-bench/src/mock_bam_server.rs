use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::Instant,
};

use crossbeam_channel::{Receiver, Sender};
use jito_protos::proto::bam_types::{AtomicTxnBatch, AtomicTxnBatchResult, Packet};
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_core::bam_dependencies::BamOutboundMessage;
use solana_hash::Hash;
use solana_keypair::Keypair;
use solana_perf::packet::solana_packet;
use solana_poh::poh_recorder::SharedWorkingBank;
use solana_pubkey::Pubkey;
use solana_signer::Signer;
use solana_system_interface::instruction::transfer;
use solana_transaction::Transaction;

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
    Transaction::new_signed_with_payer(
        &[
            transfer(&from_keypair.pubkey(), to, lamports),
            ComputeBudgetInstruction::set_compute_unit_price(compute_unit_price),
            ComputeBudgetInstruction::set_compute_unit_limit(TRANSFER_TRANSACTION_COST),
        ],
        Some(&from_keypair.pubkey()),
        &[from_keypair],
        recent_blockhash,
    )
}

struct BamOutboundMessageResult {
    time_received: Instant,
    result: AtomicTxnBatchResult,
}

struct BamTransactionInfo {
    time_sent: Instant,
    transaction: Transaction,
}

struct BamTransactionAndResult {
    transaction: BamTransactionInfo,
    result: Option<BamOutboundMessageResult>,
}

struct BankStats {
    bank_slot: u64,
    start_time: Instant,
    transactions_sent: u64,

    sent_transactions_and_results: HashMap<u32, BamTransactionAndResult>,
}

impl BankStats {
    fn new(bank_slot: u64) -> Self {
        Self {
            bank_slot,
            start_time: Instant::now(),
            transactions_sent: 0,
            sent_transactions_and_results: HashMap::with_capacity(50_000),
        }
    }

    fn print_stats(&self) {
        println!(
            "bank slot: {}, elapsed: {}ms, transactions sent: {}",
            self.bank_slot,
            self.start_time.elapsed().as_millis(),
            self.transactions_sent
        );
    }
}

pub(crate) struct MockBamServer;

impl MockBamServer {
    pub(crate) fn run(
        batch_sender: Sender<AtomicTxnBatch>,
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
                if let Some(bank) = shared_working_bank.load() {
                    // print stats, reset variables, and let it rip
                    if bank.slot() != bank_stats.bank_slot {
                        // TODO (LB): wait for all the results to return + time how long that takes
                        println!(
                            "bank slot changed from {} to {}",
                            bank_stats.bank_slot,
                            bank.slot()
                        );

                        print!("waiting for results...");
                        loop {
                            if let Ok(msg) = outbound_receiver.recv() {
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
                                    msg => {
                                        panic!("unexpected message");
                                    }
                                }

                                if bank_stats
                                    .sent_transactions_and_results
                                    .iter()
                                    .all(|(_, result)| result.result.is_some())
                                {
                                    println!("got all results");
                                    break;
                                }
                            }
                        }

                        bank_stats.print_stats();
                        bank_stats = BankStats::new(bank.slot());
                        seq_id = 0;
                        nonce = 0;
                    }

                    for keypair in keypairs.iter() {
                        let tx = make_transfer_transaction_with_compute_unit_price(
                            keypair,
                            &keypair.pubkey(),
                            nonce % 100_000,
                            bank.last_blockhash(),
                            1,
                        );

                        bank_stats.sent_transactions_and_results.insert(
                            seq_id,
                            BamTransactionAndResult {
                                transaction: BamTransactionInfo {
                                    time_sent: Instant::now(),
                                    transaction: tx.clone(),
                                },
                                result: None,
                            },
                        );

                        let packet = solana_packet::Packet::from_data(None, tx).unwrap();
                        let data = packet.data(..).unwrap_or_default().to_vec();
                        let atomic_txn_batch = AtomicTxnBatch {
                            seq_id: seq_id,
                            max_schedule_slot: bank.slot(),
                            packets: vec![Packet {
                                data: data.to_vec(),
                                meta: Some(jito_protos::proto::bam_types::Meta {
                                    size: data.len() as u64,
                                    flags: None,
                                }),
                            }],
                        };
                        batch_sender.send(atomic_txn_batch).unwrap();
                        bank_stats.transactions_sent += 1;
                        nonce += 1;
                        seq_id += 1;
                    }
                }

                // for current_iteration_index in 0..iterations {
                //     trace!("RUNNING ITERATION {}", current_iteration_index);
                //     let now = Instant::now();
                //     let mut sent = 0;

                //     let packets_for_this_iteration = &all_packets[current_iteration_index % num_chunks];
                //     for (packet_batch_index, packet_batch) in
                //         packets_for_this_iteration.packet_batches.iter().enumerate()
                //     {
                //         sent += packet_batch.len();
                //         trace!(
                //             "Sending PacketBatch index {}, {}",
                //             packet_batch_index,
                //             timestamp(),
                //         );

                //         for p in packet_batch {
                //             // bam non-multi transaction bundles are a single packet
                //             let bam_batch = AtomicTxnBatch {
                //                 seq_id: next_seq_id,
                //                 max_schedule_slot: bank.slot(),
                //                 packets: vec![Packet {
                //                     data: p.data(..).unwrap_or_default().to_vec(),
                //                     meta: Some(jito_protos::proto::bam_types::Meta {
                //                         size: p.meta().size as u64,
                //                         flags: None,
                //                     }),
                //                 }],
                //             };

                //             next_seq_id += 1;
                //             batch_sender.send(bam_batch).unwrap();
                //         }
                //     }
            }
        })
    }
}
