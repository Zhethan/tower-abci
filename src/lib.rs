#![doc = include_str!("../README.md")]
/// A fork of tower_abci @ `d18d3e6`
mod buffer4;

pub mod v037 {
    mod codec;
    mod server;
    pub mod split;
    pub use server::Server;
    pub use server::ServerBuilder;
}

/// A convenient error type alias.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
