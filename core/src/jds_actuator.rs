/// Receives pre-scheduled microblocks and attempts to 'actuate' them by applying the transactions to the state.

use jito_protos::proto::jds_types::{micro_block_packet::Data, Bundle, MicroBlock, Packet};

pub struct JdsActuator {
}

impl JdsActuator {
    pub fn new() -> Self {
        Self {}
    }

    fn parse_validate_execute_and_commit_bundle(_bundle: Bundle) {
        todo!();
    }

    fn parse_validate_execute_and_commit_transaction(_packet: Packet) {
        todo!();
    }

    pub fn execute_and_commit_micro_block(&mut self, micro_block: MicroBlock) {
        for packet in micro_block.packets {
            let Some(packet) = packet.data else {
                continue;
            };

            match packet {
                Data::Bundle(bundle) => {
                    Self::parse_validate_execute_and_commit_bundle(bundle);
                }
                Data::Packet(packet) => {
                    Self::parse_validate_execute_and_commit_transaction(packet);
                }
            }
        }
    }
}