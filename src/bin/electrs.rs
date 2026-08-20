/// Standalone electrs binary backed by Bitcoin Core (not nakamoto).
///
/// This preserves the original upstream electrs behaviour: connect to a local
/// Bitcoin Core node via RPC/ZMQ and serve the Electrum protocol.
///
/// For the nakamoto-backed bridge use the default `nakamoto-electrs` binary.

fn main() {
    if let Err(e) = electrs::run() {
        eprintln!("electrs failed: {e:#}");
        std::process::exit(1);
    }
}
