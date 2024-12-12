/// Manages majority of the JDS related functionality:
/// - Connecting to the JDS block engine via GRPC service
/// - Sending signed slot ticks + Receive microblocks
/// - Actuating the received microblocks
/// - Disabling JDS and re-enabling standard txn processing when health check fails

use std::{sync::{atomic::AtomicBool, Arc, RwLock}, thread::Builder};

use jito_protos::proto::{jds_api::validator_api_client::ValidatorApiClient, jds_types::MicroBlock};
use solana_poh::poh_recorder::PohRecorder;
use solana_runtime::bank_forks::BankForks;
use solana_sdk::clock::Slot;
use tokio::time::timeout;

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
                rt.block_on(Self::start_service(
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

    async fn start_service(
        jds_url: String,
        jds_enabled: Arc<AtomicBool>,
        exit: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bank_forks: Arc<RwLock<BankForks>>,
    ) {
        let backend_endpoint = tonic::transport::Endpoint::from_shared(jds_url).unwrap();
        let connection_timeout = std::time::Duration::from_secs(5);
        let block_engine_channel = timeout(connection_timeout, backend_endpoint.connect()).await.unwrap().unwrap();
        let validator_client = ValidatorApiClient::new(block_engine_channel);
        let sent_tick_for_slot: Option<Slot> = None;
        let jds_actuator = JdsActuator::new();

        let init_request = jito_protos::proto::jds_types::SignedSlotTick{ slot_tick: todo!(), signature: todo!() };
        //let stream = validator_client.start_scheduler_stream(init_request).await.unwrap();

        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            if !jds_enabled.load(std::sync::atomic::Ordering::Relaxed) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }

            const TICK_LOOKAHEAD: u64 = 8;
            // Check if signed slot tick needs to be sent; if yes, send it
            // Then; wait with time out; if nothing received; disable jds
            // For each microblock received; send it to the 'Actuator'
            loop {
                let micro_block = MicroBlock::default();
                jds_actuator.execute_and_commit_micro_block(micro_block);
            }
        }
    }

    pub fn join(self) -> std::thread::Result<()> {
        for thread in self.threads {
            thread.join()?;
        }
        Ok(())
    }
}