package dev.jsketi.moqclient.data.network

import android.net.Network
import dev.jsketi.moqclient.domain.model.NetworkHealth
import dev.jsketi.moqclient.domain.model.NetworkPath
import java.net.DatagramSocket
import kotlinx.coroutines.flow.StateFlow

/**
 * Tracks retained Wi-Fi and cellular Android Network handles and owns Android-level binding.
 */
interface NetworkManager {

    val wifiNetwork: StateFlow<Network?>

    val cellularNetwork: StateFlow<Network?>

    /** OS default network path, not necessarily the active QUIC publishing path. */
    val activePath: StateFlow<NetworkPath>

    val wifiSignalDbm: StateFlow<Int?>

    val wifiHealth: StateFlow<NetworkHealth>

    fun start()

    fun stop()

    /**
     * Binds the whole process to [path]. This affects newly created sockets and REST telemetry.
     */
    fun selectPath(path: NetworkPath)

    /** Returns the currently retained Android Network handle for [path], or null if unavailable. */
    fun networkFor(path: NetworkPath): Network?

    /**
     * Binds [socket] directly to [path]'s Android Network using Network.bindSocket().
     *
     * This socket-level binding is independent of the OS default network and is the preferred path
     * for QUIC rebind sockets. Throws if the target Network handle is unavailable.
     */
    fun bindDatagramSocket(path: NetworkPath, socket: DatagramSocket): Network

    /** Clears process binding so new sockets follow the OS default network again. */
    fun clearProcessBinding()
}
