/// Manages majority of the JDS related functionality:
/// - Connecting to the JDS block engine via GRPC service
/// - Sending signed slot ticks + Receive microblocks
/// - Actuating the received microblocks
/// - Disabling JDS and re-enabling standard txn processing when health check fails

use std::{sync::{atomic::AtomicBool, Arc, RwLock}, thread::Builder};

use jito_protos::proto::{jds_api::validator_api_client::ValidatorApiClient, jds_types::{MicroBlock, SignedSlotTick, SlotTick}};
use solana_entry::poh;
use solana_poh::poh_recorder::PohRecorder;
use solana_runtime::bank_forks::BankForks;
use solana_sdk::clock::Slot;
use tokio::{task::spawn_blocking, time::timeout};

use crate::jds_actuator::JdsActuator;

pub(crate) struct JdsManager {
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl JdsManager {
    pub fn new(
        jds_url: String,
        jds_enabled: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bank_forks: Arc<RwLock<BankForks>>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let api_connection_thread = Builder::new()
            .name("block-engine-stage".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(Self::start_manager(
                    jds_url,
                    jds_enabled,
                    exit,
                    poh_recorder,
                    bank_forks,
                ));
            })
            .unwrap();

        Self {
            threads: vec![
                api_connection_thread
            ],
        }
    }

    async fn start_manager(
        jds_url: String,
        jds_enabled: Arc<AtomicBool>,
        exit: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bank_forks: Arc<RwLock<BankForks>>,
    ) {
        let mut jds_connection = None;
        let mut jds_actuator = JdsActuator::new();

        // Run until the world ends
        while !exit.load(std::sync::atomic::Ordering::Relaxed) {

            // If no connection exists; create one
            let Some(current_jds_connection) = jds_connection.as_mut() else {
                jds_connection = JdsConnection::try_init(jds_url.clone());
                if jds_connection.is_none() {
                    jds_enabled.store(false, std::sync::atomic::Ordering::Relaxed);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
                continue;
            };

            // If jds connection is not ready (hasn't received first heartbeat); wait and loop again
            if !current_jds_connection.is_ready() {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                continue;
            }

            // If the jds_connection is not healthy; disable jds
            if !current_jds_connection.is_healthy() {
                jds_enabled.store(false, std::sync::atomic::Ordering::Relaxed);
                jds_connection = None;
                continue;
            }

            // If not time for auction; wait and loop again
            if !Self::time_for_auction(&poh_recorder.read().unwrap()) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            }

            // Send slot tick
            let Some(signed_slot_tick) = Self::get_signed_slot_tick(&poh_recorder.read().unwrap()) else {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            };
            current_jds_connection.send_signed_tick(signed_slot_tick);

            // Wait til in leader slot (TODO: breakout for error)
            while !Self::inside_leader_slot(&poh_recorder.read().unwrap()) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }

            // Keep receiving microblocks and actuating them as long as within the existing slot
            while Self::inside_leader_slot(&poh_recorder.read().unwrap()) {
                let micro_block = current_jds_connection.recv_next();
                if micro_block.is_none() {
                    tokio::time::sleep(std::time::Duration::from_micros(100)).await;
                    continue;
                }
                jds_actuator.execute_and_commit_micro_block(micro_block.unwrap());
            }
        }
    }

    pub fn join(self) -> std::thread::Result<()> {
        for thread in self.threads {
            thread.join()?;
        }
        Ok(())
    }

    pub fn time_for_auction(poh_recorder: &PohRecorder) -> bool {
        const TICK_LOOKAHEAD: u64 = 8;
        poh_recorder.would_be_leader(TICK_LOOKAHEAD)
    }

    pub fn inside_leader_slot(poh_recorder: &PohRecorder) -> bool {
        poh_recorder.would_be_leader(0)
    }

    pub fn get_signed_slot_tick(poh_recorder: &PohRecorder) -> Option<SignedSlotTick> {
        let Some(current_slot) = poh_recorder.bank().and_then(|bank| Some(bank.slot())) else {
            return None;
        };
        let Some(parent_slot) = poh_recorder.bank().and_then(|bank| Some(bank.parent_slot())) else {
            return None;
        };
        let slot_tick = SlotTick{ current_slot, parent_slot };
        let signature = todo!();
        Some(SignedSlotTick { slot_tick: Some(slot_tick), signature })
    }
}

struct JdsConnection {
}

impl JdsConnection {
    fn try_init(url: String) -> Option<Self> {
        // let backend_endpoint = tonic::transport::Endpoint::from_shared(jds_url).unwrap();
        // let connection_timeout = std::time::Duration::from_secs(5);
        // let block_engine_channel = timeout(connection_timeout, backend_endpoint.connect()).await.unwrap().unwrap();
        // let validator_client = ValidatorApiClient::new(block_engine_channel);
        // let sent_tick_for_slot: Option<Slot> = None;

        //let init_request = jito_protos::proto::jds_types::SignedSlotTick{ slot_tick: todo!(), signature: todo!() };
        //let stream = validator_client.start_scheduler_stream(init_request).await.unwrap();


        None
    }

    fn recv_next(&mut self) -> Option<MicroBlock> {
        todo!()
    }

    fn send_signed_tick(&mut self, _signed_slot_tick: SignedSlotTick) {
        todo!()
    }

    fn is_healthy(&self) -> bool {
        todo!()
    }

    fn is_ready(&self) -> bool {
        todo!()
    }
}