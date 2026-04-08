"""
Text-to-Speech — TTS Inference with Binary Result Handling

Demonstrates:
    - Text to audio bytes conversion (simulated)
    - Binary result handling through the adapter
    - Base64 encoding for binary data in JSON results
    - Simulated TTS without any ML dependencies

Usage:
    python examples/ml_patterns/text_to_speech.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import base64
import hashlib
import os
import time
import uuid

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")

SAMPLE_RATE = 22050  # Standard TTS sample rate


def synthesize_speech(
    text: str,
    voice: str = "default",
    speed: float = 1.0,
) -> dict:
    """Simulated text-to-speech synthesis returning audio bytes."""
    # In production: model = TTS(model_name="tts_models/en/ljspeech/tacotron2-DDC")
    # In production: audio_array = model.tts(text, speaker=voice, speed=speed)
    # In production: audio_bytes = audio_array_to_wav(audio_array, sample_rate=22050)

    # Simulate synthesis time proportional to text length
    estimated_duration = len(text.split()) * 0.3  # ~0.3s per word
    time.sleep(min(0.2, estimated_duration * 0.05))  # Capped simulation delay

    # Generate deterministic pseudo-audio bytes from text hash
    text_hash = hashlib.sha256(f"{text}{voice}{speed}".encode()).digest()
    # Simulate WAV-like binary data (header + samples)
    # Real WAV: 44-byte header + PCM samples
    num_samples = int(estimated_duration * SAMPLE_RATE)
    # Generate a small representative chunk (not full audio in example)
    audio_chunk_size = min(num_samples * 2, 4096)  # 16-bit samples, capped
    audio_bytes = bytes(
        (text_hash[i % len(text_hash)] ^ (i & 0xFF)) & 0xFF for i in range(audio_chunk_size)
    )

    # Encode as base64 for JSON-safe transport
    audio_b64 = base64.b64encode(audio_bytes).decode("ascii")

    return {
        "text": text,
        "voice": voice,
        "speed": speed,
        "sample_rate": SAMPLE_RATE,
        "duration_seconds": round(estimated_duration, 2),
        "audio_format": "wav",
        "audio_size_bytes": len(audio_bytes),
        "audio_base64": audio_b64,
    }


def poll_result(
    adapter: KubeMQTaskProcessorAdapter, task_id: str, timeout: int = 15
) -> dict | None:
    """Poll until task completes or timeout."""
    start = time.perf_counter()
    while (time.perf_counter() - start) < timeout:
        status = adapter.get_task_status_sync(task_id)
        if status.status in ("SUCCESS", "FAILURE"):
            return {"status": status.status, "result": status.result}
        time.sleep(0.5)
    return None


def main():
    channel = f"example-ml-tts-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 1
        failed_task_queue_name = f"{channel}.dlq"
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=2, task_processor_config=_Cfg())
    adapter.register_task_handler(synthesize_speech, name="synthesize")
    adapter.start_consumer()

    try:
        requests = [
            {
                "text": "Hello and welcome to KubeMQ Ray Serve integration.",
                "voice": "default",
                "speed": 1.0,
            },
            {
                "text": "This is a demonstration of text to speech synthesis.",
                "voice": "female-1",
                "speed": 1.2,
            },
            {
                "text": "Batch processing enables high throughput audio generation.",
                "voice": "male-1",
                "speed": 0.9,
            },
        ]

        print("Text-to-Speech Pipeline")
        print(f"  Sample rate: {SAMPLE_RATE} Hz")
        print(f"  Queue: {channel}")

        for i, req in enumerate(requests):
            print(f"\n--- TTS Request {i + 1} ---")
            print(f'  Text: "{req["text"]}"')
            print(f"  Voice: {req['voice']}, Speed: {req['speed']}x")

            result = adapter.enqueue_task_sync(
                "synthesize",
                kwargs={
                    "text": req["text"],
                    "voice": req["voice"],
                    "speed": req["speed"],
                },
            )
            print(f"  Enqueued: task_id={result.id}")

            outcome = poll_result(adapter, result.id)
            if outcome and outcome["status"] == "SUCCESS":
                r = outcome["result"]
                print("  Result:")
                print(f"    Duration: {r['duration_seconds']}s")
                print(f"    Format: {r['audio_format']}")
                print(f"    Audio size: {r['audio_size_bytes']} bytes")
                # Verify base64 is decodable
                decoded = base64.b64decode(r["audio_base64"])
                print(f"    Base64 decoded: {len(decoded)} bytes (verified)")
                print(f"    Base64 preview: {r['audio_base64'][:60]}...")
            elif outcome:
                print(f"  Failed: {outcome}")
            else:
                print("  Timed out waiting for result.")
    finally:
        adapter.stop_consumer()

    print("\nExample complete.")


if __name__ == "__main__":
    main()
