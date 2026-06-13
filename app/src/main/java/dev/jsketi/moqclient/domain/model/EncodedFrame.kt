package dev.jsketi.moqclient.domain.model

/**
 * 단일 H.264 액세스 유닛(또는 NAL unit 묶음).
 *
 * payload 는 Annex-B 형식 (start code 0x000001 / 0x00000001 + NALU).
 * MediaCodec output buffer 의 BufferInfo.presentationTimeUs 를 그대로 사용.
 * isKeyframe 은 BufferInfo.flags 의 BUFFER_FLAG_KEY_FRAME 비트.
 *
 * MoQ broadcast 의 group 경계는 keyframe 단위이며 본 모델은 그 정보를 유지.
 */
class EncodedFrame(
    val payload: ByteArray,
    val presentationTimeUs: Long,
    val isKeyframe: Boolean,
    val encodedAtElapsedMs: Long
)
