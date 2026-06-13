package dev.jsketi.moqclient.domain.usecase

import android.os.ParcelFileDescriptor
import android.os.SystemClock
import android.util.Log
import dev.jsketi.moqclient.data.moq.MoqPublisher
import dev.jsketi.moqclient.data.network.NetworkManager
import dev.jsketi.moqclient.domain.model.NetworkPath
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.InetSocketAddress

/**
 * Migrates the active MoQ session onto a target Android Network path.
 *
 * The primary path creates a DatagramSocket in Kotlin, binds it directly to the target Android
 * Network, detaches a dup fd, then passes that fd into moq-ffi/quinn. This avoids relying on OS
 * default routing or process binding to affect a native-created UDP socket.
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

    suspend operator fun invoke(target: NetworkPath): Result<NetworkPath> = runCatching {
        val attemptId = nextAttemptId()
        val startedAt = SystemClock.elapsedRealtime()
        val targetNetwork = networkManager.networkFor(target)
            ?: error("cannot switch to $target; Network handle is not available")

        Log.i(
            TAG,
            "[switch#$attemptId] ENTER target=$target targetNetwork=$targetNetwork " +
                "osDefault=${networkManager.activePath.value}"
        )

        try {
            Log.i(TAG, "[switch#$attemptId] step=process-bind target=$target")
            networkManager.selectPath(target)
            Log.i(TAG, "[switch#$attemptId] step=process-bind OK target=$target")

            val fd = createBoundDatagramSocketFd(attemptId, target)
            val fdResult = moqPublisher.rebindFd(fd)
            if (fdResult.isSuccess) {
                Log.i(TAG, "[switch#$attemptId] step=rebind-fd OK target=$target fd=$fd")
                logDone(attemptId, target, startedAt, "fd")
                return@runCatching target
            }

            val fdError = fdResult.exceptionOrNull()
            Log.w(
                TAG,
                "[switch#$attemptId] step=rebind-fd FAIL target=$target fd=$fd; " +
                    "fallback=legacy-address error=${fdError?.javaClass?.simpleName}: ${fdError?.message}",
                fdError
            )

            moqPublisher.rebind(REBIND_ADDRESS).getOrThrow()
            Log.i(TAG, "[switch#$attemptId] step=rebind-legacy OK target=$target addr=$REBIND_ADDRESS")
            logDone(attemptId, target, startedAt, "legacy")
            target
        } catch (t: Throwable) {
            Log.w(
                TAG,
                "[switch#$attemptId] FAIL target=$target after=${SystemClock.elapsedRealtime() - startedAt}ms; " +
                    "rolling back process binding: ${t.javaClass.simpleName}: ${t.message}",
                t
            )
            runCatching { networkManager.clearProcessBinding() }
                .onFailure { rollbackError ->
                    Log.e(TAG, "[switch#$attemptId] rollback clearProcessBinding failed", rollbackError)
                }
            throw t
        }
    }

    private fun createBoundDatagramSocketFd(attemptId: Long, target: NetworkPath): Int {
        val socket = DatagramSocket(null)
        try {
            val bindAddress = InetSocketAddress(InetAddress.getByName(IPV6_UNSPECIFIED), 0)
            Log.i(TAG, "[switch#$attemptId] step=socket-create localBefore=${socket.localSocketAddress}")
            socket.bind(bindAddress)
            Log.i(TAG, "[switch#$attemptId] step=socket-local-bind OK local=${socket.localSocketAddress}")

            val network = networkManager.bindDatagramSocket(target, socket)
            Log.i(
                TAG,
                "[switch#$attemptId] step=network-bind OK target=$target network=$network " +
                    "local=${socket.localSocketAddress}"
            )

            val sourcePfd = checkNotNull(ParcelFileDescriptor.fromDatagramSocket(socket)) {
                "ParcelFileDescriptor.fromDatagramSocket returned null"
            }
            val dupPfd = sourcePfd.dup()
            sourcePfd.close()
            val fd = dupPfd.detachFd()
            Log.i(TAG, "[switch#$attemptId] step=fd-detach OK fd=$fd target=$target")
            return fd
        } catch (t: Throwable) {
            Log.e(
                TAG,
                "[switch#$attemptId] step=socket-fd FAIL target=$target " +
                    "local=${socket.localSocketAddress}: ${t.javaClass.simpleName}: ${t.message}",
                t
            )
            throw t
        } finally {
            runCatching { socket.close() }
                .onFailure { Log.w(TAG, "[switch#$attemptId] Java DatagramSocket close failed: ${it.message}", it) }
        }
    }

    private fun logDone(attemptId: Long, target: NetworkPath, startedAt: Long, mode: String) {
        Log.i(
            TAG,
            "[switch#$attemptId] DONE target=$target mode=$mode " +
                "elapsed=${SystemClock.elapsedRealtime() - startedAt}ms osDefault=${networkManager.activePath.value}"
        )
    }

    private companion object {
        private const val TAG = "SwitchNetworkUseCase"
        private const val REBIND_ADDRESS = "[::]:0"
        private const val IPV6_UNSPECIFIED = "::"

        @Volatile private var attemptSequence: Long = 0

        @Synchronized
        private fun nextAttemptId(): Long {
            attemptSequence += 1
            return attemptSequence
        }
    }
}
