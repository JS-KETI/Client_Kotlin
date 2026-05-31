package dev.jsketi.moqclient.domain.usecase

import android.net.Network
import android.util.Log
import dev.jsketi.moqclient.data.moq.MoqPublisher
import dev.jsketi.moqclient.data.network.NetworkManager
import dev.jsketi.moqclient.domain.model.NetworkPath

/**
 * Migrates the active MoQ session to the opposite network path.
 *
 * Network.bindSocket() cannot affect the UDP socket created inside moq-ffi.
 * The process network must be switched before rebind() so the new native socket
 * is created on the target Android Network.
 */
class SwitchNetworkUseCase(
    private val networkManager: NetworkManager,
    private val moqPublisher: MoqPublisher
) {

    suspend operator fun invoke(): Result<NetworkPath> = runCatching {
        val active = networkManager.activePath.value
        val target = when (active) {
            NetworkPath.WIFI -> NetworkPath.CELLULAR
            NetworkPath.CELLULAR -> NetworkPath.WIFI
        }
        val targetNetwork: Network = when (target) {
            NetworkPath.WIFI -> networkManager.wifiNetwork.value
            NetworkPath.CELLULAR -> networkManager.cellularNetwork.value
        } ?: error("cannot switch to $target; Network handle is not available")

        try {
            networkManager.selectPath(target)
            Log.i(
                TAG,
                "switch $active -> $target; network=$targetNetwork rebindAddress=$REBIND_ADDRESS"
            )
            moqPublisher.rebind(REBIND_ADDRESS).getOrThrow()
            target
        } catch (t: Throwable) {
            Log.w(TAG, "switch $active -> $target failed; rolling back active path", t)
            runCatching { networkManager.selectPath(active) }
                .onFailure { rollbackError ->
                    Log.e(TAG, "rollback to $active failed", rollbackError)
                }
            throw t
        }
    }

    private companion object {
        private const val TAG = "SwitchNetworkUseCase"
        private const val REBIND_ADDRESS = "[::]:0"
    }
}
