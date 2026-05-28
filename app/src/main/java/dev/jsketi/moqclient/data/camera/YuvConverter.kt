package dev.jsketi.moqclient.data.camera

import android.graphics.ImageFormat
import android.media.Image
import androidx.camera.core.ImageProxy
import java.nio.ByteBuffer

/**
 * CameraX [ImageProxy] (YUV_420_888) 의 planes 를 MediaCodec input [Image] planes 로 직접 복사.
 *
 * MediaCodec 이 `getInputImage(index)` 로 반환하는 Image 는 codec 이 내부에서 요구하는 실제 형식
 * (I420 / NV12 / NV21 / device-specific) 에 맞는 pixelStride 와 rowStride 를 가진다.
 * → 따라서 plane 단위로 src(ImageProxy) → dst(codec Image) 복사하면 codec 이 알아서 받아준다.
 *
 * 빠른 경로: src/dst 모두 pixelStride==1 + rowStride==width 일 때 plane 전체를 ByteBuffer.put 으로 통째 복사.
 * 일반 경로: row 별로 src 를 row buffer 에 읽고, dst 에는 col*dstPix 위치로 absolute put.
 */
object YuvConverter {

    fun copy(src: ImageProxy, dst: Image) {
        require(src.format == ImageFormat.YUV_420_888) { "src format=${src.format}, expected YUV_420_888" }
        require(dst.format == ImageFormat.YUV_420_888) { "dst format=${dst.format}, expected YUV_420_888" }
        require(src.width == dst.width && src.height == dst.height) {
            "size mismatch src=${src.width}x${src.height} dst=${dst.width}x${dst.height}"
        }
        val srcPlanes = src.planes
        val dstPlanes = dst.planes
        check(srcPlanes.size == 3 && dstPlanes.size == 3) {
            "expected 3 YUV planes (src=${srcPlanes.size} dst=${dstPlanes.size})"
        }

        for (planeIdx in 0..2) {
            val s = srcPlanes[planeIdx]
            val d = dstPlanes[planeIdx]
            val planeWidth = if (planeIdx == 0) src.width else src.width / 2
            val planeHeight = if (planeIdx == 0) src.height else src.height / 2
            copyPlane(
                src = s.buffer, srcRowStride = s.rowStride, srcPixelStride = s.pixelStride,
                dst = d.buffer, dstRowStride = d.rowStride, dstPixelStride = d.pixelStride,
                width = planeWidth, height = planeHeight
            )
        }
    }

    private fun copyPlane(
        src: ByteBuffer, srcRowStride: Int, srcPixelStride: Int,
        dst: ByteBuffer, dstRowStride: Int, dstPixelStride: Int,
        width: Int, height: Int
    ) {
        val srcPos = src.position()
        val dstPos = dst.position()

        // 빠른 경로: 양쪽 다 packed (pixelStride=1) + padding 없음 → 통째 복사
        if (srcPixelStride == 1 && dstPixelStride == 1 &&
            srcRowStride == width && dstRowStride == width
        ) {
            val total = width * height
            val savedLimit = src.limit()
            try {
                src.limit(srcPos + total)
                dst.put(src)
            } finally {
                src.limit(savedLimit)
                src.position(srcPos)
                dst.position(dstPos)
            }
            return
        }

        // 일반 경로: row 단위로 src 읽고 dst 에 col 별 absolute put
        val rowBuf = ByteArray(srcRowStride)
        for (row in 0 until height) {
            src.position(srcPos + row * srcRowStride)
            val toRead = minOf(srcRowStride, src.remaining())
            src.get(rowBuf, 0, toRead)
            val rowDstBase = dstPos + row * dstRowStride
            for (col in 0 until width) {
                dst.put(rowDstBase + col * dstPixelStride, rowBuf[col * srcPixelStride])
            }
        }
        src.position(srcPos)
        dst.position(dstPos)
    }
}
