package dev.jsketi.moqclient.service

import android.content.Context
import dev.jsketi.moqclient.BuildConfig
import dev.jsketi.moqclient.data.camera.CameraEncoderImpl
import dev.jsketi.moqclient.data.location.LocationProviderImpl
import dev.jsketi.moqclient.data.moq.MoqPublisherImpl
import dev.jsketi.moqclient.data.moq.MultipathSockets
import dev.jsketi.moqclient.data.network.CellularWarmup
import dev.jsketi.moqclient.data.network.NetworkManagerImpl
import dev.jsketi.moqclient.data.rest.DeviceIdentityStore
import dev.jsketi.moqclient.data.rest.NetworkModule
import dev.jsketi.moqclient.data.rest.TelemetryReporter
import dev.jsketi.moqclient.domain.model.NetworkPath
import dev.jsketi.moqclient.domain.usecase.ConnectUseCase
import dev.jsketi.moqclient.domain.usecase.StreamToggleUseCase
import dev.jsketi.moqclient.domain.usecase.SwitchNetworkUseCase
import dev.jsketi.moqclient.util.log.FieldLogCapture
import dev.jsketi.moqclient.util.log.LogExporter
import dev.jsketi.moqclient.vm.PublisherViewModel

object ServiceLocator {

    @Volatile
    private var runtimeInstance: PublisherRuntime? = null

    @Volatile
    private var fieldLogCaptureInstance: FieldLogCapture? = null

    fun runtime(context: Context): PublisherRuntime {
        return runtimeInstance ?: synchronized(this) {
            runtimeInstance ?: createRuntime(context.applicationContext).also { runtimeInstance = it }
        }
    }

    fun fieldLogCapture(context: Context): FieldLogCapture {
        return fieldLogCaptureInstance ?: synchronized(this) {
            fieldLogCaptureInstance ?: FieldLogCapture(context.applicationContext)
                .also { fieldLogCaptureInstance = it }
        }
    }

    fun publisherViewModelFactory(context: Context): PublisherViewModel.Factory {
        val appContext = context.applicationContext
        val runtime = runtime(appContext)
        return PublisherViewModel.Factory(
            connectUseCase = ConnectUseCase(
                identityStore = DeviceIdentityStore(appContext),
                moqPublisher = runtime.moqPublisher
            ),
            streamToggleUseCase = StreamToggleUseCase(runtime),
            switchNetworkUseCase = SwitchNetworkUseCase(
                networkManager = runtime.networkManager,
                moqPublisher = runtime.moqPublisher
            ),
            runtime = runtime,
            networkManager = runtime.networkManager,
            logExporter = LogExporter(appContext, fieldLogCapture(appContext))
        )
    }

    private fun createRuntime(appContext: Context): PublisherRuntime {
        val networkManager = NetworkManagerImpl(appContext)
        val moqPublisher = MoqPublisherImpl(quicBackend = BuildConfig.MOQ_QUIC_BACKEND)
        val identityStore = DeviceIdentityStore(appContext)
        val locationProvider = LocationProviderImpl(appContext)
        val switchNetworkUseCase = SwitchNetworkUseCase(networkManager, moqPublisher)

        // 멀티패스(#38): noq 백엔드일 때만 무장. connect 시도마다 Wi-Fi/LTE 소켓 fd 를 새로 만든다
        // (fd 는 네이티브가 소비). quinn 기본값이면 provider 미장착 → 완전히 기존 단일 경로 동작.
        if (BuildConfig.MOQ_QUIC_BACKEND.equals("noq", ignoreCase = true)) {
            moqPublisher.setMultipathProvider {
                MultipathSockets(
                    wifiFd = switchNetworkUseCase.createPathSocketFd(NetworkPath.WIFI),
                    cellFd = switchNetworkUseCase.createPathSocketFd(NetworkPath.CELLULAR),
                )
            }
            // #44: 멀티패스 arm 실패(한쪽 망 부재) 시 단일 폴백을 현재 OS default 네트워크에
            // 고정 바인딩 — 다른 망 등장에 따른 소스 변경(고전 마이그레이션)을 원천 차단.
            moqPublisher.setFallbackSocketProvider {
                runCatching {
                    switchNetworkUseCase.createPathSocketFd(networkManager.activePath.value)
                }.getOrNull()
            }
        }

        return PublisherRuntime(
            networkManager = networkManager,
            cellularWarmupFactory = { CellularWarmup(networkManager.cellularNetwork) },
            moqPublisher = moqPublisher,
            cameraEncoder = CameraEncoderImpl(appContext),
            locationProvider = locationProvider,
            deviceRepository = NetworkModule.deviceRepository,
            identityStore = identityStore,
            telemetryReporter = TelemetryReporter(appContext, NetworkModule.deviceRepository, locationProvider, networkManager, moqPublisher),
            migrationControllerFactory = { runtime ->
                AutoNetworkMigrationController(
                    networkManager = networkManager,
                    moqPublisher = moqPublisher,
                    switchNetworkUseCase = switchNetworkUseCase,
                    runtime = runtime,
                    // noq(멀티패스) 모드에서는 rebind 전환이 DualSocket 을 파괴 → 비활성(#38 E2E 회귀)
                    enabled = !BuildConfig.MOQ_QUIC_BACKEND.equals("noq", ignoreCase = true)
                )
            },
            multipathFailoverFactory = { runtime ->
                MultipathFailoverController(
                    networkManager = networkManager,
                    moqPublisher = moqPublisher,
                    runtime = runtime,
                    // Wi-Fi 재합류(#46)용 — 복귀한 Wi-Fi Network 에 바인딩된 새 fd
                    wifiSocketFdFactory = { switchNetworkUseCase.createPathSocketFd(NetworkPath.WIFI) },
                    // P3(#40): noq(멀티패스) 모드 전용 — Wi-Fi 사망/송신 정체 시 백업 승격 후 주경로 폐기
                    enabled = BuildConfig.MOQ_QUIC_BACKEND.equals("noq", ignoreCase = true)
                )
            }
        )
    }
}
