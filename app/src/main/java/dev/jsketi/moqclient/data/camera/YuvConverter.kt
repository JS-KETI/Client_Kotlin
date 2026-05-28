package dev.jsketi.moqclient.data.camera

import android.graphics.ImageFormat
import androidx.camera.core.ImageProxy
import java.nio.ByteBuffer

/**
 * CameraX [ImageProxy] (YUV_420_888) → I420 planar YUV ByteArray 변환.
 *
 * MediaCodec 에 `COLOR_FormatYUV420Flexible` 로 input 을 넣을 때 표준 레이아웃.
 * 메모리 레이아웃:
 *   [Y 평면 (W*H)] [U 평면 (W/2 × H/2)] [V 평면 (W/2 × H/2)]
 *
 * YUV_420_888 의 plane 별 stride 처리:
 *   - U/V plane 의 pixelStride 는 1(planar) 또는 2(semi-planar, NV21/NV12) 일 수 있다.
 *   - rowStride 가 width 보다 큰 경우(정렬 padding) row 별 복사가 필요하다.
 */
object YuvConverter {

    fun toI420(image: ImageProxy): ByteArray {
        require(image.format == ImageFormat.YUV_420_888) {
            "expected YUV_420_888 but got format=${image.format}"
        }
        val width = image.width
        val height = image.height
        require(width % 2 == 0 && height % 2 == 0) {
            "YUV_420_888 dimensions must be even, got ${width}x$height"
        }

        val ySize = width * height
        val uvWidth = width / 2
        val uvHeight = height / 2
        val uvSize = uvWidth * uvHeight
        val out = ByteArray(ySize + uvSize * 2)

        val planes = image.planes
        check(planes.size == 3) { "YUV_420_888 expected 3 planes, got ${planes.size}" }

        // Y
        copyPlane(planes[0].buffer, planes[0].rowStride, planes[0].pixelStride, width, height, out, 0)
        // U → out[ySize ..]
        copyPlane(planes[1].buffer, planes[1].rowStride, planes[1].pixelStride, uvWidth, uvHeight, out, ySize)
        // V → out[ySize + uvSize ..]
        copyPlane(planes[2].buffer, planes[2].rowStride, planes[2].pixelStride, uvWidth, uvHeight, out, ySize + uvSize)

        return out
    }

    private fun copyPlane(
        src: ByteBuffer,
        rowStride: Int,
        pixelStride: Int,
        width: Int,
        height: Int,
        dst: ByteArray,
        dstOffset: Int
    ) {
        // 빠른 경로: 패딩 없고 packed → 통째 복사
        if (pixelStride == 1 && rowStride == width) {
            val saved = src.position()
            src.get(dst, dstOffset, width * height)
            src.position(saved)
            return
        }

        val rowBuf = ByteArray(rowStride)
        var dstPos = dstOffset
        val savedPos = src.position()
        for (row in 0 until height) {
            src.position(savedPos + row * rowStride)
            val toRead = minOf(rowStride, src.remaining())
            src.get(rowBuf, 0, toRead)
            if (pixelStride == 1) {
                System.arraycopy(rowBuf, 0, dst, dstPos, width)
                dstPos += width
            } else {
                var srcIdx = 0
                var dstIdx = dstPos
                repeat(width) {
                    dst[dstIdx++] = rowBuf[srcIdx]
                    srcIdx += pixelStride
                }
                dstPos += width
            }
        }
        src.position(savedPos)
    }
}
