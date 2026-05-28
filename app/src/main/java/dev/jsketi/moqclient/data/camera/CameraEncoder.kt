package dev.jsketi.moqclient.data.camera

import androidx.camera.view.PreviewView
import androidx.lifecycle.LifecycleOwner
import dev.jsketi.moqclient.domain.model.CodecConfig
import dev.jsketi.moqclient.domain.model.EncodedFrame
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow

/**
 * 카메라 캡처 + H.264 인코딩 파이프라인 추상화 (Adapter port).
 *
 * 외부 호출자(UseCase / Service / ViewModel)는 본 interface 에만 의존한다.
 * 구현체 교체 가능성: CameraX → Camera2 fallback, 또는 file-replay 테스트 더블 등.
 *
 * 사용 흐름:
 *   1. start(owner, previewView): 카메라 bind + MediaCodec start
 *   2. codecConfig.first { it != null } 로 SPS/PPS init segment 수신 (1회)
 *   3. encodedFrames.collect 로 매 frame NAL bytes 수신
 *   4. stop(): 카메라 unbind + MediaCodec stop. 같은 인스턴스에서 다시 start() 가능.
 *
 * 동시 두 번 start() 는 미지원. 호출자가 라이프사이클을 직렬화해야 한다.
 */
interface CameraEncoder {

    /** 인코더 첫 출력의 BUFFER_FLAG_CODEC_CONFIG 콜백에서 한 번 emit. */
    val codecConfig: StateFlow<CodecConfig?>

    /** 모든 keyframe + delta frame NAL 단위 emit. SharedFlow extraBufferCapacity 권장. */
    val encodedFrames: SharedFlow<EncodedFrame>

    fun start(lifecycleOwner: LifecycleOwner, previewView: PreviewView)

    fun stop()
}
