package dev.jsketi.moqclient.service

import android.content.Context
import dev.jsketi.moqclient.data.camera.CameraEncoderImpl
import dev.jsketi.moqclient.data.moq.MoqPublisherImpl
import dev.jsketi.moqclient.data.network.CellularWarmup
import dev.jsketi.moqclient.data.network.NetworkManagerImpl
import dev.jsketi.moqclient.data.rest.DeviceIdentityStore
import dev.jsketi.moqclient.data.rest.NetworkModule
import dev.jsketi.moqclient.data.rest.TelemetryReporter
import dev.jsketi.moqclient.domain.usecase.ConnectUseCase
import dev.jsketi.moqclient.domain.usecase.StreamToggleUseCase
import dev.jsketi.moqclient.domain.usecase.SwitchNetworkUseCase
import dev.jsketi.moqclient.vm.PublisherViewModel

object ServiceLocator {

    @Volatile
    private var runtimeInstance: PublisherRuntime? = null

    fun runtime(context: Context): PublisherRuntime {
        return runtimeInstance ?: synchronized(this) {
            runtimeInstance ?: createRuntime(context.applicationContext).also { runtimeInstance = it }
        }
    }

    fun publisherViewModelFactory(context: Context): PublisherViewModel.Factory {
        val appContext = context.applicationContext
        val runtime = runtime(appContext)
        return PublisherViewModel.Factory(
            connectUseCase = ConnectUseCase(
                deviceRepository = NetworkModule.deviceRepository,
                identityStore = DeviceIdentityStore(appContext),
                moqPublisher = runtime.moqPublisher
            ),
            streamToggleUseCase = StreamToggleUseCase(runtime),
            switchNetworkUseCase = SwitchNetworkUseCase(
                networkManager = runtime.networkManager,
                moqPublisher = runtime.moqPublisher
            ),
            runtime = runtime,
            networkManager = runtime.networkManager
        )
    }

    private fun createRuntime(appContext: Context): PublisherRuntime {
        val networkManager = NetworkManagerImpl(appContext)
        return PublisherRuntime(
            networkManager = networkManager,
            cellularWarmupFactory = { CellularWarmup(networkManager.cellularNetwork) },
            moqPublisher = MoqPublisherImpl(),
            cameraEncoder = CameraEncoderImpl(appContext),
            telemetryReporter = TelemetryReporter(appContext, NetworkModule.deviceRepository)
        )
    }
}
