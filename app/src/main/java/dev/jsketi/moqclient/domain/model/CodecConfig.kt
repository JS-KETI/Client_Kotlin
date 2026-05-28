package dev.jsketi.moqclient.domain.model

/**
 * H.264 decoder init segment 정보.
 *
 * codecString : WebCodecs 형식 "avc1.XXXXXX" (SPS profile_idc / constraint_set / level_idc 6자리 hex)
 *               관제 페이지가 MSE/WebCodecs 로 디코딩할 때 이 문자열을 catalog 에 박는다.
 * sps / pps  : NALU raw bytes (start code 미포함). MoQ catalog 의 init segment 합성에 사용.
 * width/height: 인코딩 해상도. 텔레메트리 등록 페이로드에도 동일 값을 전달한다.
 */
class CodecConfig(
    val codecString: String,
    val sps: ByteArray,
    val pps: ByteArray,
    val width: Int,
    val height: Int
)
