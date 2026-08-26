// P1-1/P2 PoC: noq 0.17.0 멀티패스 실측 (이슈 #38)
//
// 모드 (환경변수 POC_MODE)
//   single(기본): 단일 와일드카드 소켓 — [협상]/[T1 원격상이]/[T2 원격동일] 판정
//   dual        : DualUdpSocket(어댑터 프로토타입) — [T3] 양단 소켓 2개 + 원격 포트로
//                 경로를 구분(4443/4444)했을 때 경로 검증·양경로 송신이 성립하는지 판정
//
// 참고: noq 0.17 루트는 PathStatus 를 재노출하지 않는다(공백 API) → noq-proto 직접 사용.
//       PathId 는 공개 생성자가 없어 PathId::ZERO + Path::id() 로만 얻는다.

mod dual;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use noq::PathId;
use noq_proto::PathStatus;

const SERVER_A: &str = "127.0.0.1:4443";
const SERVER_B_SINGLE: &str = "127.0.0.2:4443"; // single 모드: 다른 원격 IP
const SERVER_B_DUAL: &str = "127.0.0.1:4444"; // dual 모드: 다른 원격 포트

fn transport() -> noq::TransportConfig {
    let mut t = noq::TransportConfig::default();
    t.max_concurrent_multipath_paths(4); // nonzero → 멀티패스 확장 협상
    t.keep_alive_interval(Some(Duration::from_secs(2)));
    t.max_idle_timeout(Some(Duration::from_secs(20).try_into().unwrap()));
    t
}

fn certs() -> anyhow::Result<(
    rustls::pki_types::CertificateDer<'static>,
    rustls::pki_types::PrivateKeyDer<'static>,
)> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = rustls::pki_types::CertificateDer::from(cert.cert);
    let key_der = rustls::pki_types::PrivateKeyDer::try_from(cert.signing_key.serialize_der())
        .map_err(|e| anyhow::anyhow!("key: {e}"))?;
    Ok((cert_der, key_der))
}

fn client_config(cert_der: rustls::pki_types::CertificateDer<'static>) -> anyhow::Result<noq::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert_der)?;
    let tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let tls: noq::crypto::rustls::QuicClientConfig = tls.try_into()?;
    let mut cfg = noq::ClientConfig::new(Arc::new(tls));
    cfg.transport_config(Arc::new(transport()));
    Ok(cfg)
}

fn spawn_server_loop(server: noq::Endpoint) {
    tokio::spawn(async move {
        while let Some(incoming) = server.accept().await {
            tokio::spawn(async move {
                match incoming.await {
                    Ok(conn) => {
                        println!(
                            "[server] conn from {} multipath={}",
                            conn.remote_address(),
                            conn.is_multipath_enabled()
                        );
                        let mut events = conn.path_events();
                        tokio::spawn(async move {
                            while let Ok(ev) = events.recv().await {
                                println!("[server] path event: {ev:?}");
                            }
                        });
                        loop {
                            match conn.accept_uni().await {
                                Ok(mut recv) => {
                                    tokio::spawn(async move {
                                        let mut buf = vec![0u8; 65536];
                                        while let Ok(Some(_)) = recv.read(&mut buf).await {}
                                    });
                                }
                                Err(e) => {
                                    println!("[server] closed: {e}");
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => println!("[server] handshake fail: {e}"),
                }
            });
        }
    });
}

async fn drive_and_report(
    conn: &noq::Connection,
    second_remote: SocketAddr,
    also_dup_test: bool,
) -> anyhow::Result<Arc<Mutex<Vec<PathId>>>> {
    anyhow::ensure!(conn.is_multipath_enabled(), "multipath NOT negotiated");

    let known_paths: Arc<Mutex<Vec<PathId>>> = Arc::new(Mutex::new(vec![PathId::ZERO]));

    let mut events = conn.path_events();
    tokio::spawn(async move {
        while let Ok(ev) = events.recv().await {
            println!("[client] path event: {ev:?}");
        }
    });

    // 데이터 배경 송신 (양 경로 사용 여부 관찰용)
    let conn_tx = conn.clone();
    tokio::spawn(async move {
        loop {
            if let Ok(mut s) = conn_tx.open_uni().await {
                let _ = s.write_all(&[0u8; 32768]).await;
                let _ = s.finish();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;

    println!("[T-second] open_path({second_remote}) ...");
    match tokio::time::timeout(
        Duration::from_secs(5),
        conn.open_path(second_remote, PathStatus::Available),
    )
    .await
    {
        Ok(Ok(path)) => {
            println!("[T-second] OK — path opened: id={:?}", path.id());
            known_paths.lock().unwrap().push(path.id());
        }
        Ok(Err(e)) => println!("[T-second] ERR — open_path failed: {e}"),
        Err(_) => println!("[T-second] TIMEOUT — path validation did not complete (5s)"),
    }

    if also_dup_test {
        let dup: SocketAddr = SERVER_A.parse()?;
        println!("[T-dup] open_path({dup}) (동일 원격) ...");
        match tokio::time::timeout(Duration::from_secs(5), conn.open_path(dup, PathStatus::Available)).await {
            Ok(Ok(path)) => {
                println!("[T-dup] OK — duplicate-remote path opened: id={:?}", path.id());
                known_paths.lock().unwrap().push(path.id());
            }
            Ok(Err(e)) => println!("[T-dup] ERR — {e}"),
            Err(_) => println!("[T-dup] TIMEOUT (5s)"),
        }
    }

    for _ in 0..5 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let ids = known_paths.lock().unwrap().clone();
        let mut seen = Vec::new();
        for id in ids {
            if let Some(st) = conn.path_stats(id) {
                seen.push(format!("{id:?} tx_bytes={} rx_bytes={}", st.udp_tx.bytes, st.udp_rx.bytes));
            }
        }
        println!("[client] paths: {}", seen.join(" | "));
    }
    Ok(known_paths)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))
        .init();
    rustls::crypto::ring::default_provider().install_default().ok();

    let mode = std::env::var("POC_MODE").unwrap_or_else(|_| "single".into());
    println!("[poc] mode={mode}");

    let (cert_der, key_der) = certs()?;
    let mut server_cfg = noq::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der)?;
    server_cfg.transport_config(Arc::new(transport()));
    let runtime = noq::default_runtime().context("runtime")?;

    let addr_a: SocketAddr = SERVER_A.parse()?;

    if mode == "dual" {
        // ── T3: 양단 DualUdpSocket, 경로=원격 포트 구분 ──────────────
        let addr_b: SocketAddr = SERVER_B_DUAL.parse()?;

        let s1 = runtime.wrap_udp_socket(std::net::UdpSocket::bind("0.0.0.0:4443")?)?;
        let s2 = runtime.wrap_udp_socket(std::net::UdpSocket::bind("0.0.0.0:4444")?)?;
        let server_dual = dual::DualUdpSocket::new(s1, s2, &[]);
        let server = noq::Endpoint::new_with_abstract_socket(
            noq::EndpointConfig::default(),
            Some(server_cfg),
            Box::new(server_dual),
            runtime.clone(),
        )?;
        spawn_server_loop(server);
        tokio::time::sleep(Duration::from_millis(300)).await;

        let c1 = runtime.wrap_udp_socket(std::net::UdpSocket::bind("0.0.0.0:0")?)?;
        let c2 = runtime.wrap_udp_socket(std::net::UdpSocket::bind("0.0.0.0:0")?)?;
        // 시드: 주포트(4443)→소켓A, 부포트(4444)→소켓B  (프로덕션에선 A=Wi-Fi, B=LTE)
        let client_dual = dual::DualUdpSocket::new(c1, c2, &[(addr_a, 0), (addr_b, 1)]);
        let kill_a = client_dual.blackhole_switch();
        let mut client = noq::Endpoint::new_with_abstract_socket(
            noq::EndpointConfig::default(),
            None,
            Box::new(client_dual),
            runtime,
        )?;
        client.set_default_client_config(client_config(cert_der)?);

        let conn = client.connect(addr_a, "localhost")?.await?;
        println!(
            "[client] connected via {} multipath={}",
            conn.remote_address(),
            conn.is_multipath_enabled()
        );
        let known_paths = drive_and_report(&conn, addr_b, false).await?;

        // ── T4: 주 경로(소켓 A) 블랙홀 → 무중단 페일오버 관찰 ────────
        println!("[T4] blackhole socket A (path0, Wi-Fi 급사 모사) ...");
        kill_a.store(true, std::sync::atomic::Ordering::Relaxed);
        for sec in 1..=8 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if sec == 3 {
                // T4c: 정책 계층 개입 모사 — 죽은 경로를 폐기(abandon).
                //      (T4b에서 Backup 강등은 신규 배정만 막고 in-flight 백로그를 회수하지
                //       못해 윈도우 고갈로 write 가 멈추는 것을 확인함 → 사망 경로는 close 가 정답)
                match conn.path(PathId::ZERO) {
                    Some(p) => {
                        let r = p.close();
                        println!("[T4c+{sec}s] close path0 (abandon): {r:?}");
                    }
                    None => println!("[T4c+{sec}s] path0 handle unavailable"),
                }
            }
            let write_ok = tokio::time::timeout(Duration::from_secs(2), async {
                let mut s = conn.open_uni().await?;
                s.write_all(&[0u8; 32768]).await?;
                s.finish()?;
                anyhow::Ok(())
            })
            .await;
            let alive = conn.close_reason().is_none();
            let ids = known_paths.lock().unwrap().clone();
            let mut seen = Vec::new();
            for id in ids {
                if let Some(st) = conn.path_stats(id) {
                    seen.push(format!("{id:?} tx={} lost={}", st.udp_tx.bytes, st.lost_packets));
                }
            }
            println!(
                "[T4+{sec}s] alive={alive} write={} | {}",
                match write_ok {
                    Ok(Ok(())) => "OK",
                    Ok(Err(_)) => "ERR",
                    Err(_) => "TIMEOUT",
                },
                seen.join(" | ")
            );
        }
        conn.close(0u32.into(), b"done");
    } else {
        // ── single: 기존 T1/T2 ───────────────────────────────────────
        let addr_b: SocketAddr = SERVER_B_SINGLE.parse()?;

        let server_sock = std::net::UdpSocket::bind("0.0.0.0:4443")?;
        let server = noq::Endpoint::new(
            noq::EndpointConfig::default(),
            Some(server_cfg),
            server_sock,
            runtime.clone(),
        )?;
        spawn_server_loop(server);
        tokio::time::sleep(Duration::from_millis(300)).await;

        let client_sock = std::net::UdpSocket::bind("0.0.0.0:0")?;
        let mut client = noq::Endpoint::new(noq::EndpointConfig::default(), None, client_sock, runtime)?;
        client.set_default_client_config(client_config(cert_der)?);

        let conn = client.connect(addr_a, "localhost")?.await?;
        println!(
            "[client] connected via {} multipath={}",
            conn.remote_address(),
            conn.is_multipath_enabled()
        );
        let _ = drive_and_report(&conn, addr_b, true).await?;
        conn.close(0u32.into(), b"done");
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    println!("[poc] end");
    Ok(())
}
