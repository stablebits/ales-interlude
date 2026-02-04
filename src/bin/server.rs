#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use bytes::Bytes;
use quinn::{Connection, Endpoint, IdleTimeout, ServerConfig, TransportConfig, VarInt};
use rustls::pki_types::PrivateKeyDer;
use std::{
    array,
    env,
    net::{IpAddr, Ipv6Addr, SocketAddr},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Old,
    New,
}

#[tokio::main]
async fn main() {
    let mode = if env::args().any(|arg| arg == "--new") {
        eprintln!("Using new accept_any_complete_uni_with_data mode");
        Mode::New
    } else {
        eprintln!("Using old accept_uni + read_chunks mode");
        Mode::Old
    };
    let mut transport_config = TransportConfig::default();

    let timeout = IdleTimeout::try_from(Duration::from_secs(10)).unwrap();
    transport_config.max_idle_timeout(Some(timeout));

    transport_config.max_concurrent_uni_streams(VarInt::from_u32(100000));

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();

    // Save certificate for client to use
    std::fs::write("server_cert.der", cert.cert.der()).unwrap();

    let key = PrivateKeyDer::Pkcs8(cert.signing_key.serialize_der().into());

    let transport_config = Arc::new(transport_config);

    let mut server_config =
        ServerConfig::with_single_cert(vec![cert.cert.der().clone()], key).unwrap();

    server_config.transport_config(transport_config.clone());

    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();

    let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 8080);

    let server = Endpoint::server(server_config, addr).unwrap();

    let stream_count = Arc::new(AtomicU64::new(0));

    // Spawn global stats printer
    {
        let stream_count = stream_count.clone();
        let mode_str = if mode == Mode::New { "NEW" } else { "OLD" };
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            let mut last_count = 0u64;
            loop {
                interval.tick().await;
                let count = stream_count.load(Ordering::Relaxed);
                let delta = count - last_count;
                eprintln!("[{mode_str}] Streams/10s: {delta}");
                last_count = count;
            }
        });
    }

    loop {
        eprintln!("waiting for connection");
        let Some(connecting) = server.accept().await else {
            break;
        };
        eprintln!("accepted connection");
        let conn = connecting.await.unwrap();

        match mode {
            Mode::Old => handle_connection_old(conn, stream_count.clone()).await,
            Mode::New => handle_connection_new(conn, stream_count.clone()).await,
        }
    }
}

async fn handle_connection_old(conn: Connection, stream_count: Arc<AtomicU64>) {
    let mut chunks: [Bytes; 4] = array::from_fn(|_| Bytes::new());
    tokio::task::spawn(async move {
        loop {
            match conn.accept_uni().await {
                Ok(mut recv_stream) => {
                    loop {
                        match recv_stream.read_chunks(&mut chunks).await {
                            Ok(n) => {
                                if n.is_none() {
                                    break;
                                }
                            }
                            Err(e) => {
                                eprintln!("error reading stream {e:?}");
                                break;
                            }
                        }
                    }
                    stream_count.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => break,
            }
        }
    });
}

async fn handle_connection_new(conn: Connection, stream_count: Arc<AtomicU64>) {
    tokio::task::spawn(async move {
        let mut data = Vec::with_capacity(4);
        loop {
            match conn.accept_any_complete_uni_with_data(&mut data).await {
                Ok(_) => {
                    stream_count.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => break,
            }
        }
    });
}
