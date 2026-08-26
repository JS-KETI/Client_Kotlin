// DualUdpSocket — P2 어댑터 프로토타입 (T3 실험용)
//
// 설계: noq Runtime::wrap_udp_socket 이 만들어 주는 내장 AsyncUdpSocket 두 개를 감싸
//       "받은 소켓으로 되돌려 보낸다(learn-map)" 규칙으로 송신을 라우팅한다.
//       - 수신: 두 내장 소켓을 순서대로 poll (둘 다 waker 등록 → 어느 쪽이 와도 깨어남)
//       - 송신: transmit.destination 을 learn-map 에서 조회 → 해당 소켓의 sender 로 위임
//       - 시드: 클라이언트처럼 "먼저 보내는" 쪽은 원격주소→소켓 매핑을 미리 시드한다
//
// 프로덕션(P2)에서는 소켓 A=Wi-Fi bindSocket, B=LTE bindSocket 이 되고
// 시드 규칙이 "relay 주포트→A, 부포트→B" 가 된다.

use std::collections::HashMap;
use std::fmt;
use std::io::{self, IoSliceMut};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use std::sync::atomic::{AtomicBool, Ordering};

use noq::udp::{RecvMeta, Transmit};
use noq::{AsyncUdpSocket, UdpSender};

pub struct DualUdpSocket {
    a: Box<dyn AsyncUdpSocket>,
    b: Box<dyn AsyncUdpSocket>,
    route: Arc<Mutex<HashMap<SocketAddr, u8>>>,
    // T4 페일오버 실험용: true 면 소켓 A 를 블랙홀 처리(송신 무시·수신 폐기) — Wi-Fi 급사 모사
    blackhole_a: Arc<AtomicBool>,
}

impl fmt::Debug for DualUdpSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DualUdpSocket")
            .field("a", &self.a.local_addr().ok())
            .field("b", &self.b.local_addr().ok())
            .finish()
    }
}

impl DualUdpSocket {
    pub fn new(
        a: Box<dyn AsyncUdpSocket>,
        b: Box<dyn AsyncUdpSocket>,
        seed: &[(SocketAddr, u8)],
    ) -> Self {
        let route = Arc::new(Mutex::new(seed.iter().copied().collect::<HashMap<_, _>>()));
        Self {
            a,
            b,
            route,
            blackhole_a: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn blackhole_switch(&self) -> Arc<AtomicBool> {
        self.blackhole_a.clone()
    }
}

impl AsyncUdpSocket for DualUdpSocket {
    fn create_sender(&self) -> Pin<Box<dyn UdpSender>> {
        Box::pin(DualSender {
            a: self.a.create_sender(),
            b: self.b.create_sender(),
            route: self.route.clone(),
            blackhole_a: self.blackhole_a.clone(),
        })
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        match self.a.poll_recv(cx, bufs, meta) {
            Poll::Ready(Ok(n)) => {
                if self.blackhole_a.load(Ordering::Relaxed) {
                    // 수신분 폐기 (Wi-Fi 급사 모사) — B 폴링으로 계속 진행
                } else {
                    let mut r = self.route.lock().unwrap();
                    for m in meta.iter().take(n) {
                        r.insert(m.addr, 0);
                    }
                    return Poll::Ready(Ok(n));
                }
            }
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => {}
        }
        match self.b.poll_recv(cx, bufs, meta) {
            Poll::Ready(Ok(n)) => {
                let mut r = self.route.lock().unwrap();
                for m in meta.iter().take(n) {
                    r.insert(m.addr, 1);
                }
                Poll::Ready(Ok(n))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.a.local_addr()
    }

    fn max_receive_segments(&self) -> NonZeroUsize {
        self.a.max_receive_segments().min(self.b.max_receive_segments())
    }

    fn may_fragment(&self) -> bool {
        self.a.may_fragment() || self.b.may_fragment()
    }
}

struct DualSender {
    a: Pin<Box<dyn UdpSender>>,
    b: Pin<Box<dyn UdpSender>>,
    route: Arc<Mutex<HashMap<SocketAddr, u8>>>,
    blackhole_a: Arc<AtomicBool>,
}

impl fmt::Debug for DualSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DualSender")
    }
}

impl UdpSender for DualSender {
    fn poll_send(
        self: Pin<&mut Self>,
        transmit: &Transmit<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let idx = this
            .route
            .lock()
            .unwrap()
            .get(&transmit.destination)
            .copied()
            .unwrap_or(0);
        if idx == 0 {
            if this.blackhole_a.load(Ordering::Relaxed) {
                // 송신 무시 (성공한 척 폐기) — Wi-Fi 급사 모사
                return Poll::Ready(Ok(()));
            }
            this.a.as_mut().poll_send(transmit, cx)
        } else {
            this.b.as_mut().poll_send(transmit, cx)
        }
    }

    fn max_transmit_segments(&self) -> NonZeroUsize {
        self.a.max_transmit_segments().min(self.b.max_transmit_segments())
    }
}

impl Unpin for DualSender {}
