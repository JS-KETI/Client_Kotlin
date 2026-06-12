package dev.jsketi.moqclient

import android.app.Application
import dev.jsketi.moqclient.service.ServiceLocator

/**
 * 앱 전역 Application. 필드 로그 캡처([dev.jsketi.moqclient.util.log.FieldLogCapture])의
 * 부트스트랩만 담당한다 — 나머지 싱글톤 배선은 기존대로 [ServiceLocator] 가 lazy 하게 처리.
 * (주의: 패키지 루트의 MoqClientApp 은 Composable 루트라 이름을 분리했다.)
 */
class MoqClientApplication : Application() {

    override fun onCreate() {
        super.onCreate()
        // 다른 어떤 초기화보다 먼저 시작 — 이른 크래시도 크래시 핸들러/다음 실행의 startupDump 로
        // 회수되어야 하므로 onCreate 최상단에 둔다.
        ServiceLocator.fieldLogCapture(this).start()
    }
}
