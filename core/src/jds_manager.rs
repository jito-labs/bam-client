use std::{sync::{atomic::AtomicBool, Arc, RwLock}, thread::Builder};

use crossbeam_channel::Receiver;
use jito_protos::proto::jds_api::validator_api_client::ValidatorApiClient;
use solana_perf::packet::PacketBatch;
use solana_poh::poh_recorder::PohRecorder;
use solana_runtime::bank_forks::BankForks;
use tokio::time::timeout;

use crate::sigverify::SigverifyTracerPacketStats;

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
                ));
            })
            .unwrap();


        JdsStage {
            threads: vec![
                api_connection_thread
            ],
        }
    }

    async fn start_service(
        jds_url: String,
        jds_enabled: Arc<AtomicBool>,
        exit: Arc<AtomicBool>,
    ) {
        let backend_endpoint = tonic::transport::Endpoint::from_shared(jds_url).unwrap();
        let connection_timeout = std::time::Duration::from_secs(5);
        let block_engine_channel = timeout(connection_timeout, backend_endpoint.connect()).await.unwrap().unwrap();
        let validator_client = ValidatorApiClient::new(block_engine_channel);
    }

    pub fn join(self) -> std::thread::Result<()> {
        for thread in self.threads {
            thread.join()?;
        }
        Ok(())
    }
}