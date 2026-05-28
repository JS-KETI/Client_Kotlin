package dev.jsketi.moqclient.data.moq

import java.io.ByteArrayOutputStream

/**
 * Builds MoQ init-segment bytes, codec strings, and Annex-B → AVCC frame conversion.
 *
 * # Why AVCC instead of Annex-B
 *
 * We publish via `MoqBroadcastProducer.publishMedia("avc1", avcC)` (AVCC mode), NOT
 * `"avc3"` (Annex-B inline mode).
 *
 * Upstream moq-mux H.264 importer (`rs/moq-mux/src/codec/h264/import.rs`):
 *
 *   - `initialize_avc1(avcC)` → catalog.video.description = Some(avcC bytes)
 *   - `initialize_avc3(annexB)` → catalog.video.description = None  ← MSE consumer fails
 *
 * The JS consumer (`@moq/watch` 0.2.x, `js/hang/src/container/cmaf/encode.ts:234`)
 * MSE backend requires `description` non-null to build the fMP4 init segment:
 *
 *     const { codedWidth, codedHeight, description } = config;
 *     if (!codedWidth || !codedHeight || !description)
 *         throw new Error("Missing required fields to create video init segment");
 *
 * Chrome/Edge can take the WebCodecs decoder path (which tolerates inline SPS/PPS),
 * but it can fall back to MSE depending on hardware/feature flags. Sending avcC
 * makes both code paths succeed.
 *
 * # AVCC frame format
 *
 * `"avc1"` mode also requires frames to use **length-prefixed NAL units** instead
 * of Annex-B start codes. Each NAL is preceded by a big-endian u32 byte count.
 * MediaCodec on Android emits Annex-B by default, so `annexBToAvcc()` converts
 * each frame before it is handed to `MoqMediaProducer.writeFrame()`.
 *
 * # References
 *
 *   - ISO/IEC 14496-15 §5.3.3.1.2 — AVCDecoderConfigurationRecord
 *   - moq-rs `build_avcc()` in `rs/moq-mux/src/codec/h264/mod.rs:114` — reference impl
 *   - hang::catalog::VideoConfig — `rs/hang/src/catalog/video/mod.rs`
 */
object CatalogBuilder {

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

    /**
     * H.264 profiles requiring the `chroma_format`/`bit_depth_*` trailer in avcC.
     * Per ISO/IEC 14496-15 §5.3.3.1.2 (the "if profile_idc in {…}" branch).
     */
    private val HIGH_PROFILE_IDCS = setOf(
        100, 110, 122, 144,                     // High, High10, High422, High444 Predictive
        44, 86, 118, 128, 138, 139,             // CAVLC4:4:4, ScalableHigh, MVCHigh, MFCHigh, MFCDepthHigh
        134, 135,                                // MFC + Stereo High variants
        83                                       // Scalable Constrained High
    )

    /**
     * Build an `AVCDecoderConfigurationRecord` (= avcC box *content*, no box header)
     * to pass to `MoqPublisher.publishMedia("avc1", ...)`.
     *
     * Base layout per ISO/IEC 14496-15 §5.3.3.1.2:
     * ```
     *   u8  configurationVersion = 1
     *   u8  AVCProfileIndication = SPS[1]   (profile_idc)
     *   u8  profile_compatibility = SPS[2]  (constraint flags)
     *   u8  AVCLevelIndication = SPS[3]     (level_idc)
     *   u8  reserved(6) | lengthSizeMinusOne(2)  = 0xFF (NAL length size = 4 bytes)
     *   u8  reserved(3) | numOfSequenceParameterSets(5) = 0xE1 (= 1 SPS)
     *   u16BE sequenceParameterSetLength
     *   <SPS NAL>
     *   u8  numOfPictureParameterSets = 0x01
     *   u16BE pictureParameterSetLength
     *   <PPS NAL>
     * ```
     *
     * **High Profile trailer** (only when profile_idc ∈ HIGH_PROFILE_IDCS):
     * ```
     *   u8  reserved(6) | chroma_format(2)              = 0xFD (chroma_format=1 → 4:2:0)
     *   u8  reserved(5) | bit_depth_luma_minus8(3)      = 0xF8 (bit_depth_luma=8)
     *   u8  reserved(5) | bit_depth_chroma_minus8(3)    = 0xF8 (bit_depth_chroma=8)
     *   u8  numOfSequenceParameterSetExt                = 0x00
     * ```
     * Default values assume 4:2:0 / 8-bit — true for ~all Android H.264 camera
     * encoders. A more rigorous implementation would parse SPS via Exp-Golomb
     * decoding to extract `chroma_format_idc`, `bit_depth_luma_minus8`, and
     * `bit_depth_chroma_minus8` from `seq_parameter_set_data`.
     *
     * @param sps  raw SPS NAL bytes (no start code); must be at least 4 bytes
     * @param pps  raw PPS NAL bytes (no start code)
     */
    fun buildAvcc(sps: ByteArray, pps: ByteArray): ByteArray {
        require(sps.size >= 4) { "SPS too short to build avcC: ${sps.size} bytes" }
        require(sps.size <= 0xFFFF) { "SPS too long for u16 length: ${sps.size} bytes" }
        require(pps.size <= 0xFFFF) { "PPS too long for u16 length: ${pps.size} bytes" }
        val profileIdc = sps[1].toInt() and 0xFF
        val isHighProfile = false
        val capacity = 11 + sps.size + pps.size
        val out = ByteArrayOutputStream(capacity)
        out.write(0x01)                       // configurationVersion
        out.write(profileIdc)                 // AVCProfileIndication
        out.write(sps[2].toInt() and 0xFF)    // profile_compatibility
        out.write(sps[3].toInt() and 0xFF)    // AVCLevelIndication
        out.write(0xFF)                       // lengthSizeMinusOne = 3 → 4-byte length prefix
        out.write(0xE1)                       // numOfSequenceParameterSets = 1
        out.write((sps.size ushr 8) and 0xFF) // SPS length high byte
        out.write(sps.size and 0xFF)          // SPS length low byte
        out.write(sps)
        out.write(0x01)                       // numOfPictureParameterSets = 1
        out.write((pps.size ushr 8) and 0xFF) // PPS length high byte
        out.write(pps.size and 0xFF)          // PPS length low byte
        out.write(pps)
        if (isHighProfile) {
            out.write(0xFC or 0x01)           // chroma_format = 1 (4:2:0)
            out.write(0xF8 or 0x00)           // bit_depth_luma_minus8 = 0
            out.write(0xF8 or 0x00)           // bit_depth_chroma_minus8 = 0
            out.write(0x00)                   // numOfSequenceParameterSetExt = 0
        }
        return out.toByteArray()
    }

    /**
     * Convert one H.264 Annex-B access unit to length-prefixed AVCC layout, dropping
     * any AUD/SEI noise between NAL units. Each NAL is emitted as:
     *
     *     [u32 BE length][NAL bytes…]
     *
     * Start codes (`00 00 01` or `00 00 00 01`) are stripped. If the input has no
     * start code, it is treated as a single NAL and length-prefixed as-is.
     */
    fun annexBToAvcc(annexB: ByteArray): ByteArray {
        val out = ByteArrayOutputStream(annexB.size + 8)
        val nals = splitAnnexBNals(annexB)
        if (nals.isEmpty()) {
            // No start codes found — treat the whole payload as one NAL (rare; fallback).
            writeLengthPrefixed(out, annexB)
            return out.toByteArray()
        }
        for (nal in nals) {
            if (nal.isNotEmpty()) writeLengthPrefixed(out, nal)
        }
        return out.toByteArray()
    }

    private fun writeLengthPrefixed(out: ByteArrayOutputStream, nal: ByteArray) {
        val len = nal.size
        out.write((len ushr 24) and 0xFF)
        out.write((len ushr 16) and 0xFF)
        out.write((len ushr 8) and 0xFF)
        out.write(len and 0xFF)
        out.write(nal)
    }

    /**
     * Split an Annex-B byte stream into raw NAL units, stripping start codes.
     * Handles both 3-byte (`00 00 01`) and 4-byte (`00 00 00 01`) start codes.
     */
    private fun splitAnnexBNals(payload: ByteArray): List<ByteArray> {
        data class StartCode(val offset: Int, val length: Int)
        val starts = mutableListOf<StartCode>()
        var i = 0
        while (i <= payload.size - 3) {
            if (payload[i] == 0.toByte() && payload[i + 1] == 0.toByte()) {
                if (i + 3 < payload.size &&
                    payload[i + 2] == 0.toByte() && payload[i + 3] == 1.toByte()
                ) {
                    starts += StartCode(i, 4); i += 4; continue
                }
                if (payload[i + 2] == 1.toByte()) {
                    starts += StartCode(i, 3); i += 3; continue
                }
            }
            i++
        }
        if (starts.isEmpty()) return emptyList()
        val result = ArrayList<ByteArray>(starts.size)
        for (idx in starts.indices) {
            val sc = starts[idx]
            val nalStart = sc.offset + sc.length
            val nalEnd = if (idx == starts.lastIndex) payload.size else starts[idx + 1].offset
            if (nalEnd > nalStart) {
                result += payload.copyOfRange(nalStart, nalEnd)
            }
        }
        return result
    }
}
