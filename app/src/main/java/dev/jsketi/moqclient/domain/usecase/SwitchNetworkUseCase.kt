package dev.jsketi.moqclient.domain.usecase

import android.util.Log
import dev.jsketi.moqclient.data.moq.MoqPublisher
import dev.jsketi.moqclient.data.network.NetworkManager
import dev.jsketi.moqclient.domain.model.NetworkPath

/**
 * Migrates the active MoQ session onto a target network path.
 *
 * Network.bindSocket() cannot reach the UDP socket created inside moq-ffi, so the **process** must be
 * bound to the target Android Network ([NetworkManager.selectPath]) before [MoqPublisher.rebind], so
 * the new native socket is created on that network and QUIC migrates the connection (same Connection
 * ID, new path).
 */
class SwitchNetworkUseCase(
    private val networkManager: NetworkManager,
    private val moqPublisher: MoqPublisher
) {

    /** Switches to the opposite of the current OS default path. */
    suspend operator fun invoke(): Result<NetworkPath> {
        val target = when (networkManager.activePath.value) {
            NetworkPath.WIFI -> NetworkPath.CELLULAR
            NetworkPath.CELLULAR -> NetworkPath.WIFI
        }
        return invoke(target)
    }

    /**
     * Switches to an explicit [target] path.
     *
     * Does NOT trust activePath / publishingPath — it verifies the target [android.net.Network] handle
     * exists, binds the process to it, then rebinds the QUIC endpoint. On failure it best-effort drops
     * the process binding (back to OS default) and propagates the error.
     */
    suspend operator fun invoke(target: NetworkPath): Result<NetworkPath> = runCatching {
        val targetNetwork = when (target) {
            NetworkPath.WIFI -> networkManager.wifiNetwork.value
            NetworkPath.CELLULAR -> networkManager.cellularNetwork.value
        } ?: error("cannot switch to $target; Network handle is not available")

        try {
            networkManager.selectPath(target)
            Log.i(TAG, "switch -> $target; network=$targetNetwork rebindAddress=$REBIND_ADDRESS")
            moqPublisher.rebind(REBIND_ADDRESS).getOrThrow()
            target
        } catch (t: Throwable) {
            Log.w(TAG, "switch -> $target failed; rolling back process binding", t)
            // Best-effort rollback: drop the binding so the process returns to OS-default routing.
            runCatching { networkManager.clearProcessBinding() }
                .onFailure { rollbackError -> Log.e(TAG, "rollback (clearProcessBinding) failed", rollbackError) }
            throw t
        }
    }

    private companion object {
        private const val TAG = "SwitchNetworkUseCase"
        private const val REBIND_ADDRESS = "[::]:0"
    }
}
