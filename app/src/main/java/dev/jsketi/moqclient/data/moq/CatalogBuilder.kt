package dev.jsketi.moqclient.data.moq

/**
 * Builds MoQ init-segment bytes and codec strings from raw SPS/PPS NAL units.
 *
 * Wire format used (LOC container, per server-contract.md §7):
 *   [3-byte Annex-B start code 0x000001][SPS bytes][3-byte start code][PPS bytes]
 *
 * This is the simplest format understood by LOC consumers. If the relay later
 * requires a full fMP4 init segment, replace buildInitBytes() with an fMP4 writer.
 */
object CatalogBuilder {

    private val START_CODE = byteArrayOf(0x00, 0x00, 0x01)

    /**
     * Build the init-segment payload to pass to MoqPublisher.publishMedia().
     * @param sps  raw SPS bytes (no start code, as extracted from MediaCodec CODEC_CONFIG output)
     * @param pps  raw PPS bytes (no start code)
     */
    fun buildInitBytes(sps: ByteArray, pps: ByteArray): ByteArray =
        START_CODE + sps + START_CODE + pps

    /**
     * Build the WebCodecs-compatible codec string from the SPS header bytes.
     * Format: avc1.<profile_idc><constraint_flags><level_idc> — each as 2 hex digits.
     *
     * SPS byte layout (after NAL unit type byte at index 0):
     *   index 1: profile_idc
     *   index 2: constraint_set flags
     *   index 3: level_idc
     *
     * @param sps  raw SPS bytes (no start code); must be at least 4 bytes
     * @throws IllegalArgumentException if sps is too short
     */
    fun buildCodecString(sps: ByteArray): String {
        require(sps.size >= 4) { "SPS too short to extract profile/level: ${sps.size} bytes" }
        val profile = sps[1].toInt() and 0xFF
        val constraints = sps[2].toInt() and 0xFF
        val level = sps[3].toInt() and 0xFF
        return "avc1.%02x%02x%02x".format(profile, constraints, level)
    }
}
