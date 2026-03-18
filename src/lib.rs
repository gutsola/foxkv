pub mod app_context;
mod command;
pub mod config;
pub mod persistence;
mod resp;

pub mod server;
pub mod storage;

pub fn warm_up_command_registry() {
    command::init_command_registry();
}
