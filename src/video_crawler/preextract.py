"""Extract the analysis service's media artifacts and ship them as a POSIX tar."""

import asyncio
import hashlib
import json
import logging
import re
import tarfile
from contextlib import suppress
from dataclasses import dataclass
from fractions import Fraction
from io import BytesIO
from pathlib import Path
from shutil import which
from tempfile import TemporaryDirectory
from time import monotonic

from video_crawler.settings import Settings

log = logging.getLogger(__name__)
upload_metrics = logging.getLogger(f"{__name__}.upload")
_AUDIO_MAPPING = re.compile(r"^\s*Stream #0:(\d+) -> #0:0\b", re.MULTILINE)
_SAMPLE_COUNT = re.compile(r"\bn_samples: (\d+)\b")
_PEAK_VOLUME = re.compile(r"\bmax_volume: (-?\d+(?:\.\d+)?) dB")
_MEAN_VOLUME = re.compile(r"\bmean_volume: (-?\d+(?:\.\d+)?) dB")
_MIN_SEGMENT_BYTES = 1024
_MIN_SEGMENT_SECONDS = 2.0
_AUDIO_SAMPLE_RATE = 16_000
_PCM_BYTES_PER_SAMPLE = 2
_SILENT_PEAK_DB = -55.0
_SILENT_MEAN_DB = -65.0
_LOW_MEAN_PEAK_LIMIT_DB = -45.0


_FRAME_RETRY_ERROR = re.compile(
    r"invalid (?:color space|argument)|could not open encoder|error (?:while )?opening encoder",
    re.IGNORECASE,
)
_FAILED_VIDEO_STREAM = re.compile(
    r"input from stream 0:(\d+)\b.*invalid color space", re.IGNORECASE
)
_COLOR_METADATA = {
    "color_space": "matrix_coefficients",
    "color_primaries": "colour_primaries",
    "color_transfer": "transfer_characteristics",
}
_RESERVED_COLOR_VALUES = {"reserved", "reserved0"}


_SCHEMA = "video-preextract-v1"


@dataclass(frozen=True)
class _MediaInfo:
    duration: float
    has_audio: bool


@dataclass(frozen=True)
class Bundle:
    size: int
    sha256: str
    extract_seconds: float


class Preextractor:
    def __init__(self, settings: Settings) -> None:
        self.frame_count = settings.preextract_frame_count
        self.segment_seconds = settings.preextract_audio_segment_seconds
        self._ffmpeg = _require_tool(settings.ffmpeg_path)
        self._ffprobe = _require_tool(settings.ffprobe_path)
        self._slots = asyncio.Semaphore(settings.preextract_concurrency)

    async def create(self, source: Path, target: Path, original_sha256: str) -> Bundle:
        """Write a bundle to a caller-owned path. The scratch directory never survives this call."""
        async with self._slots:
            with TemporaryDirectory(dir=source.parent, prefix=".preextract-") as directory:
                frames = Path(directory) / "frames"
                audio = Path(directory) / "audio"
                frames.mkdir()
                started = monotonic()
                info = await self.probe(source)
                count = await self.extract_frames(source, frames, info)
                if count == 0:
                    raise ValueError("预抽取未生成帧")
                audio.mkdir()
                segments = await self._validated_audio(source, audio) if info.has_audio else []
                extract_seconds = monotonic() - started
                manifest = {
                    "schema": _SCHEMA,
                    "duration": info.duration,
                    "has_audio": info.has_audio,
                    "frame_count": count,
                    "frame_count_config": self.frame_count,
                    "segment_seconds_config": self.segment_seconds,
                    "audio_indices": [int(segment.stem) for segment in segments],
                    "original_size": source.stat().st_size,
                    "original_sha256": original_sha256,
                }
                await _pack(target, manifest, frames, count, segments)
        return await _bundle_digest(target, extract_seconds)

    async def probe(self, source: Path) -> _MediaInfo:
        output, _ = await _run(
            self._ffprobe, "-v", "error",
            "-show_entries", "format=duration:stream=codec_type",
            "-of", "json", str(source),
        )  # fmt: skip
        info = json.loads(output)
        streams = {stream["codec_type"] for stream in info.get("streams", [])}
        duration = float(info.get("format", {}).get("duration", 0))
        if "video" not in streams or duration <= 0:
            raise ValueError("不是有效的视频文件")
        return _MediaInfo(duration=duration, has_audio="audio" in streams)

    async def extract_frames(self, source: Path, target: Path, info: _MediaInfo) -> int:
        """在整段视频上均匀抽取图片，第 k 张约在 k × 时长 ÷ 张数 处。"""
        output_args = (
            "-vf", f"fps={self.frame_count / info.duration},scale=-2:'min(720,ih)'",
            "-frames:v", str(self.frame_count), "-q:v", "4", "-start_number", "0",
        )  # fmt: skip
        try:
            await _run(
                self._ffmpeg,
                "-v",
                "error",
                "-i",
                str(source),
                *output_args,
                str(target / "%03d.jpg"),
            )
        except _ProcessError as error:
            # EINVAL (-22) 在 POSIX 进程退出码中截断为 234。
            if error.returncode not in (-22, 234) and not _FRAME_RETRY_ERROR.search(error.stderr):
                raise
            try:
                count = await self._retry_frames(source, target, output_args, error.stderr)
            except _ProcessError as retry_error:
                raise RuntimeError(f"{error}\n色彩元数据回退失败：{retry_error}") from retry_error
            if count is None:
                raise
            return count
        return len(list(target.glob("*.jpg")))

    async def _retry_frames(
        self, source: Path, target: Path, output_args: tuple[str, ...], stderr: str
    ) -> int | None:
        if (patch := await self._color_patch(source, stderr)) is None:
            return None
        index, bitstream_filter = patch

        # 滤镜图在首帧进入前就会拒绝非法矩阵，因此先在压缩流上改写标签。
        with TemporaryDirectory(dir=target, prefix=".normalize-") as directory:
            scratch = Path(directory)
            normalized = scratch / "source.mp4"
            await _run(
                self._ffmpeg, "-v", "error", "-i", str(source), "-map", f"0:{index}",
                "-c:v", "copy", "-bsf:v", bitstream_filter, str(normalized),
            )  # fmt: skip
            output = str(scratch / "%03d.jpg")
            await _run(self._ffmpeg, "-v", "error", "-i", str(normalized), *output_args, output)
            frames = list(scratch.glob("*.jpg"))
            if not frames:
                return None
            for frame in frames:
                frame.replace(target / frame.name)
            # target 由调度层清空。首次失败可能留下比回退输出更多的帧。
            for stale in range(len(frames), self.frame_count):
                (target / f"{stale:03d}.jpg").unlink(missing_ok=True)
        log.info("抽帧色彩元数据回退成功（%s）", source.parent.name)
        return len(frames)

    async def _validated_audio(self, source: Path, scratch: Path) -> list[Path]:
        try:
            peak, _, stream = await self._audio_volume(source)
            if peak <= _SILENT_PEAK_DB:
                log.info("音轨峰值 %.1f dBFS，跳过听音（%s）", peak, source.parent.name)
                return []
            limit = await self._audio_sample_limit(source, stream) if stream is not None else None
        except (_ProcessError, ValueError) as error:
            log.warning("音轨预检失败，跳过听音（%s）：%s", source.parent.name, error)
            return []

        min_samples = _MIN_SEGMENT_SECONDS * _AUDIO_SAMPLE_RATE
        if limit is not None and limit < min_samples:
            log.info("原音轨不足 %.1f 秒，跳过听音（%s）", _MIN_SEGMENT_SECONDS, source.parent.name)
            return []

        # 先按 PCM 采样切片，再独立编码 MP3，避免编解码填充改变片段的有效时长。
        pcm = scratch / "source.pcm"
        sample_rate = str(_AUDIO_SAMPLE_RATE)
        mapping = ("-map", f"0:{stream}") if stream is not None else ()
        await _run(
            self._ffmpeg, "-v", "error", "-i", str(source), "-vn", *mapping,
            "-ac", "1", "-ar", sample_rate, "-f", "s16le", str(pcm),
        )  # fmt: skip
        total = pcm.stat().st_size // _PCM_BYTES_PER_SAMPLE
        if limit is not None:
            total = min(total, limit)
        segment_samples = self.segment_seconds * _AUDIO_SAMPLE_RATE
        segments = []
        for index, start in enumerate(range(0, total, segment_samples)):
            count = min(segment_samples, total - start)
            segment = scratch / f"{index:03d}.mp3"
            if count < min_samples:
                log.warning(
                    "丢弃音频段（%s/%s）：音频不足 %.1f 秒",
                    source.parent.name, segment.name, _MIN_SEGMENT_SECONDS,
                )  # fmt: skip
                break
            await _run(
                self._ffmpeg, "-v", "error", "-f", "s16le", "-ar", sample_rate, "-ac", "1",
                "-ss", str(index * self.segment_seconds), "-i", str(pcm),
                "-t", str(count / _AUDIO_SAMPLE_RATE), "-b:a", "48k", str(segment),
            )  # fmt: skip
            try:
                issue = await self._audio_segment_issue(segment)
            except (_ProcessError, ValueError, FileNotFoundError) as error:
                issue = str(error)
            if issue is None:
                segments.append(segment)
            else:
                log.warning("丢弃音频段（%s/%s）：%s", source.parent.name, segment.name, issue)
        return segments

    async def _audio_volume(self, source: Path) -> tuple[float, float | None, str | None]:
        _, stats = await _run(
            self._ffmpeg, "-hide_banner", "-nostats", "-v", "info", "-xerror", "-i", str(source),
            "-vn", "-af", "volumedetect", "-f", "null", "-",
        )  # fmt: skip
        peaks = _PEAK_VOLUME.findall(stats)
        if not peaks or not any(int(count) > 0 for count in _SAMPLE_COUNT.findall(stats)):
            raise ValueError("未检测到有效的音频采样或峰值音量")
        mean = max(map(float, _MEAN_VOLUME.findall(stats)), default=None)
        stream = _AUDIO_MAPPING.search(stats)
        return max(map(float, peaks)), mean, stream[1] if stream else None

    async def _audio_sample_limit(self, source: Path, stream: str) -> int | None:
        """用容器声明的音轨时长去掉 AAC 等输入编码可能留下的尾部填充。"""
        output, _ = await _run(
            self._ffprobe, "-v", "error", "-select_streams", stream,
            "-show_entries", "stream=duration_ts,time_base", "-of", "json", str(source),
        )  # fmt: skip
        streams = json.loads(output).get("streams", [])
        if not streams or "duration_ts" not in streams[0]:
            return None
        track = streams[0]
        duration = int(track["duration_ts"]) * Fraction(track["time_base"])
        return int(duration * _AUDIO_SAMPLE_RATE) if duration > 0 else None

    async def _audio_segment_issue(self, segment: Path) -> str | None:
        if segment.stat().st_size < _MIN_SEGMENT_BYTES:
            return f"文件小于 {_MIN_SEGMENT_BYTES} 字节"
        output, errors = await _run(
            self._ffprobe, "-v", "error", "-select_streams", "a:0", "-show_frames",
            "-show_entries", "stream=codec_name,sample_rate:frame=nb_samples", "-of", "json",
            str(segment),
        )  # fmt: skip
        if errors.strip():
            return f"MP3 解码失败：{errors.strip()[-500:]}"
        info = json.loads(output)
        streams = info.get("streams", [])
        if not streams or streams[0].get("codec_name") != "mp3":
            return "没有有效的 MP3 音频流"
        rate = int(streams[0].get("sample_rate", 0))
        samples = sum(int(frame.get("nb_samples", 0)) for frame in info.get("frames", []))
        if rate <= 0 or samples < _MIN_SEGMENT_SECONDS * rate:
            return f"解码后音频不足 {_MIN_SEGMENT_SECONDS} 秒"

        peak, mean, _ = await self._audio_volume(segment)
        if peak <= _SILENT_PEAK_DB:
            return f"音频近静音（峰值 {peak:.1f} dBFS）"
        # 峰值较高时保留片段，即使较长的停顿将整段平均音量拉低。
        if mean is not None and mean <= _SILENT_MEAN_DB and peak <= _LOW_MEAN_PEAK_LIMIT_DB:
            return f"音频近静音（峰值 {peak:.1f} dBFS，平均 {mean:.1f} dBFS）"
        return None

    async def _color_patch(self, source: Path, stderr: str) -> tuple[int, str] | None:
        output, _ = await _run(
            self._ffprobe, "-v", "error", "-select_streams", "V", "-show_entries",
            "stream=index,codec_name,color_space,color_primaries,color_transfer", "-of", "json",
            str(source),
        )  # fmt: skip
        streams = json.loads(output).get("streams", [])
        if match := _FAILED_VIDEO_STREAM.search(stderr):
            index = int(match[1])
            stream = next((stream for stream in streams if stream["index"] == index), None)
        else:
            # stderr 未标出输入流时，仅在能确定视频流的情况下回退。
            stream = streams[0] if len(streams) == 1 else None
        if stream is None or stream.get("codec_name") not in {"h264", "hevc"}:
            return None

        # H.264/HEVC 中 2 表示 unspecified。只改写保留值，保留有效的色彩标签和范围。
        options = [
            f"{option}=2"
            for tag, option in _COLOR_METADATA.items()
            if stream.get(tag) in _RESERVED_COLOR_VALUES
        ]
        if not options:
            return None
        return stream["index"], f"{stream['codec_name']}_metadata={':'.join(options)}"


async def _pack(
    target: Path, manifest: dict, frames: Path, count: int, segments: list[Path]
) -> None:
    encoded = json.dumps(manifest, separators=(",", ":"), allow_nan=False).encode()
    entry = tarfile.TarInfo("manifest.json")
    entry.size = len(encoded)
    with tarfile.open(target, "w", format=tarfile.USTAR_FORMAT) as archive:
        archive.addfile(entry, BytesIO(encoded))
        for index in range(count):
            name = f"{index:03d}.jpg"
            await _pack_file(archive, frames / name, f"frames/{name}")
        # Publish continuous file names while the manifest retains the original indices.
        for index, segment in enumerate(segments):
            await _pack_file(archive, segment, f"audio/{index:03d}.mp3")


async def _pack_file(archive: tarfile.TarFile, path: Path, name: str) -> None:
    entry = tarfile.TarInfo(name)
    entry.size = path.stat().st_size
    if not entry.size:
        raise ValueError(f"预抽取文件为空：{name}")
    with path.open("rb") as payload:
        archive.addfile(entry, payload)
    await asyncio.sleep(0)


async def _bundle_digest(target: Path, extract_seconds: float) -> Bundle:
    digest = hashlib.sha256()
    with target.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
            await asyncio.sleep(0)
        return Bundle(file.tell(), digest.hexdigest(), extract_seconds)


def _require_tool(tool: str) -> str:
    path = which(tool)
    if path is None:
        raise RuntimeError(f"未找到 {tool}，请安装 FFmpeg 或配置 VC_FFMPEG_PATH/VC_FFPROBE_PATH")
    return path


class _ProcessError(RuntimeError):
    def __init__(self, tool: str, returncode: int, stderr: str) -> None:
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(f"{tool} 退出码 {returncode}：{stderr.strip()[-500:]}")


async def _run(*args: str) -> tuple[str, str]:
    # A cancelled spawn can still create a process, so acquire its handle before unwinding.
    starting = asyncio.create_task(
        asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
    )
    try:
        process = await asyncio.shield(starting)
    except asyncio.CancelledError:
        with suppress(OSError):
            await _stop_process(await starting)
        raise
    try:
        stdout, stderr = await process.communicate()
    except asyncio.CancelledError:
        await _stop_process(process)
        raise
    output, errors = stdout.decode(), stderr.decode(errors="replace")
    if process.returncode:
        raise _ProcessError(Path(args[0]).name, process.returncode, errors)
    return output, errors


async def _stop_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is None:
        with suppress(ProcessLookupError):
            process.kill()
    # Drain the pipes as well as waiting for the process to release its files on Windows.
    await process.communicate()
