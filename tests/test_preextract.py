import asyncio
import hashlib
import json
import logging
import shutil
import sys
import tarfile
from contextlib import nullcontext
from pathlib import Path

import pytest

from video_crawler import main, preextract
from video_crawler.preextract import Preextractor
from video_crawler.settings import Settings


def settings(**fields):
    return Settings(_env_file=None, service_token="test", **fields)


@pytest.mark.parametrize(("has_audio", "indices"), [(True, (0, 2)), (False, ()), (True, ())])
async def test_bundle_names_and_original_indices(tmp_path, monkeypatch, has_audio, indices):
    raw = tmp_path / "input.mp4"
    raw.write_bytes(b"original MP4")
    bundle_path = tmp_path / "bundle.tar"
    original_sha256 = hashlib.sha256(raw.read_bytes()).hexdigest()
    extractor = Preextractor(settings(ffmpeg_path=sys.executable, ffprobe_path=sys.executable))

    async def fake_run(tool, *args):
        if "-show_entries" in args:
            info = {"format": {"duration": 140}, "streams": [{"codec_type": "video"}]}
            if has_audio:
                info["streams"].append({"codec_type": "audio"})
            return json.dumps(info), ""
        assert args[args.index("-q:v") + 1] == "4"
        assert args[args.index("-vf") + 1] == "fps=0.04285714285714286,scale=-2:'min(720,ih)'"
        for index in range(6):
            Path(args[-1].replace("%03d", f"{index:03d}")).write_bytes(b"jpeg")
        return "", ""

    async def kept_segments(source, scratch):
        assert source == raw and has_audio
        result = []
        for index in indices:
            segment = scratch / f"{index:03d}.mp3"
            segment.write_bytes(f"audio {index}".encode())
            result.append(segment)
        return result

    monkeypatch.setattr(preextract, "_run", fake_run)
    monkeypatch.setattr(extractor, "_validated_audio", kept_segments)
    bundle = await extractor.create(raw, bundle_path, original_sha256)
    assert set(tmp_path.iterdir()) == {raw, bundle_path}
    assert bundle.size == bundle_path.stat().st_size
    assert bundle.sha256 == hashlib.sha256(bundle_path.read_bytes()).hexdigest()
    with tarfile.open(bundle_path, "r:") as archive:
        assert archive.getnames() == [
            "manifest.json",
            *(f"frames/{i:03d}.jpg" for i in range(6)),
            *(f"audio/{i:03d}.mp3" for i in range(len(indices))),
        ]
        with archive.extractfile("manifest.json") as payload:
            assert json.load(payload) == {
                "schema": "video-preextract-v1",
                "duration": 140,
                "has_audio": has_audio,
                "frame_count": 6,
                "frame_count_config": 6,
                "segment_seconds_config": 60,
                "audio_indices": list(indices),
                "original_size": raw.stat().st_size,
                "original_sha256": original_sha256,
            }
        for published, original in enumerate(indices):
            with archive.extractfile(f"audio/{published:03d}.mp3") as payload:
                assert payload.read() == f"audio {original}".encode()


async def test_cancellation_is_safe_during_subprocess_launch(monkeypatch):
    started, release = asyncio.Event(), asyncio.Event()
    spawn = asyncio.create_subprocess_exec
    spawned = []

    async def slow_spawn(*args, **options):
        process = await spawn(*args, **options)
        spawned.append(process)
        started.set()
        await release.wait()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", slow_spawn)
    job = asyncio.create_task(preextract._run(sys.executable, "-c", "import time; time.sleep(60)"))
    await asyncio.wait_for(started.wait(), timeout=5)
    job.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(job, timeout=5)
    assert spawned[0].returncode is not None


async def test_ffmpeg_concurrency_is_bounded(tmp_path, monkeypatch):
    extractor = Preextractor(
        settings(
            ffmpeg_path=sys.executable,
            ffprobe_path=sys.executable,
            preextract_concurrency=2,
        )
    )
    entered = 0
    ready, release = asyncio.Event(), asyncio.Event()

    async def probe(source):
        nonlocal entered
        entered += 1
        if entered == 2:
            ready.set()
        await release.wait()
        return preextract._MediaInfo(1, False)

    async def make_frame(source, frames, info):
        (frames / "000.jpg").write_bytes(b"image")
        return 1

    monkeypatch.setattr(extractor, "probe", probe)
    monkeypatch.setattr(extractor, "extract_frames", make_frame)
    raw = tmp_path / "input.mp4"
    raw.write_bytes(b"video")
    pending = [
        asyncio.create_task(extractor.create(raw, tmp_path / f"{i}.tar", "a" * 64))
        for i in range(4)
    ]
    try:
        await asyncio.wait_for(ready.wait(), timeout=2)
        assert entered == 2
    finally:
        release.set()
        await asyncio.gather(*pending)
    assert entered == 4


@pytest.mark.skipif(
    not (shutil.which("ffmpeg") and shutil.which("ffprobe")), reason="FFmpeg required"
)
@pytest.mark.parametrize("track", ["speechlike", "silent", "none"])
async def test_static_ffmpeg_produces_playable_bundle(tmp_path, track):
    raw, target = tmp_path / "input.mp4", tmp_path / "output.tar"
    source = "color=c=blue:s=128x96:r=24:d=3"
    args = ["ffmpeg", "-v", "error", "-f", "lavfi", "-i", source]
    if track != "none":
        audio = "anullsrc=r=16000" if track == "silent" else "sine=f=700:r=16000"
        args += ["-f", "lavfi", "-i", audio, "-c:a", "aac", "-shortest"]
    await preextract._run(*args, "-c:v", "libx264", str(raw))
    await Preextractor(settings()).create(raw, target, hashlib.sha256(raw.read_bytes()).hexdigest())
    with tarfile.open(target, "r:") as bundle:
        with bundle.extractfile("manifest.json") as payload:
            manifest = json.load(payload)
        assert manifest["has_audio"] is (track != "none")
        assert manifest["audio_indices"] == ([0] if track == "speechlike" else [])
        with bundle.extractfile("frames/000.jpg") as frame:
            assert frame.read(2) == b"\xff\xd8"
        if track == "speechlike":
            with bundle.extractfile("audio/000.mp3") as payload:
                audio = tmp_path / "test.mp3"
                audio.write_bytes(payload.read())
            output, _ = await preextract._run(
                "ffprobe", "-v", "error", "-show_streams", "-of", "json", str(audio)
            )
            [stream] = json.loads(output)["streams"]
            assert (
                stream["codec_name"],
                stream["sample_rate"],
                stream["channels"],
                stream["bit_rate"],
            ) == ("mp3", "16000", 1, "48000")


@pytest.mark.parametrize("enabled", [True, False])
async def test_serve_writes_preextract_metrics_without_the_cli(tmp_path, monkeypatch, enabled):
    monkeypatch.setattr(main.Redis, "from_url", lambda *args, **kwargs: nullcontext())
    monkeypatch.setattr(logging.getLogger(), "level", logging.WARNING)
    logger = preextract.upload_metrics
    before = logger.handlers.copy(), logger.level

    async def emit(_settings, _redis):
        logger.info("pre-extract upload finished")

    monkeypatch.setattr(main, "_serve", emit)
    await main.serve(settings(data_dir=tmp_path, preextract_media=enabled))
    assert (logger.handlers, logger.level) == before
    log_path = tmp_path / "preextract.log"
    if enabled:
        assert "pre-extract upload finished" in log_path.read_text(encoding="utf-8")
    else:
        assert not log_path.exists()
