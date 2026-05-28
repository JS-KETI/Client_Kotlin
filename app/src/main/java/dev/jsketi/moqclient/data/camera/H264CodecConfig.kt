package dev.jsketi.moqclient.data.camera

/**
 * H.264 SPS NAL 에서 WebCodecs 호환 codec string 을 추출하고
 * Annex-B start code 처리를 담당하는 순수 함수 유틸.
 *
 * 명세: plan/server-contract.md §7. 형식 "avc1.<6 hex>".
 *   - byte[1] : profile_idc
 *   - byte[2] : constraint_set_flags + reserved
 *   - byte[3] : level_idc
 *
 * 예) High Profile + Level 4.0 → "avc1.640028".
 */
object H264CodecConfig {

    private val ANNEX_B_4: ByteArray = byteArrayOf(0x00, 0x00, 0x00, 0x01)
    private val ANNEX_B_3: ByteArray = byteArrayOf(0x00, 0x00, 0x01)

    /**
     * SPS NALU 바이트 배열에서 "avc1.XXXXXX" 코덱 문자열을 추출한다.
     * 입력은 start code 포함/미포함 모두 허용한다.
     *
     * @throws IllegalArgumentException SPS 가 4 byte (NAL header + profile/constraint/level) 미만일 때.
     */
    fun toAvc1CodecString(sps: ByteArray): String {
        val offset = startCodeLength(sps)
        require(sps.size >= offset + 4) {
            "SPS too short: ${sps.size} bytes (need at least ${offset + 4} after ${offset}-byte start code)"
        }
        val profileIdc = sps[offset + 1].toInt() and 0xFF
        val constraintFlags = sps[offset + 2].toInt() and 0xFF
        val levelIdc = sps[offset + 3].toInt() and 0xFF
        return "avc1.%02X%02X%02X".format(profileIdc, constraintFlags, levelIdc)
    }

    /**
     * NALU 앞 start code(`00 00 00 01` 또는 `00 00 01`) 길이를 반환한다.
     * start code 가 없으면 0.
     */
    fun startCodeLength(nalu: ByteArray): Int = when {
        nalu.size >= 4 && nalu.startsWith(ANNEX_B_4) -> 4
        nalu.size >= 3 && nalu.startsWith(ANNEX_B_3) -> 3
        else -> 0
    }

    /**
     * Start code 를 제거한 raw NALU 바이트를 반환한다.
     * start code 가 없으면 원본 그대로 반환.
     */
    fun stripStartCode(nalu: ByteArray): ByteArray {
        val offset = startCodeLength(nalu)
        return if (offset == 0) nalu else nalu.copyOfRange(offset, nalu.size)
    }

    private fun ByteArray.startsWith(prefix: ByteArray): Boolean {
        if (size < prefix.size) return false
        for (i in prefix.indices) if (this[i] != prefix[i]) return false
        return true
    }
}
