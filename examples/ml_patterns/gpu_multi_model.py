"""
GPU Multi-Model — Different Queues for Different GPU Models

Demonstrates:
    - Multiple adapters, each serving a different model on its own queue
    - Separate consumer per model (simulating GPU isolation)
    - Dispatching requests to the correct model queue
    - Collecting and comparing results across models

Usage:
    python examples/ml_patterns/gpu_multi_model.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import os
import time
import uuid

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")


def resnet_predict(image_data: str) -> dict:
    """Simulated ResNet image classification (GPU 0)."""
    # In production: model = torchvision.models.resnet50(pretrained=True)
    time.sleep(0.1)  # Simulate 100ms GPU inference
    categories = ["cat", "dog", "bird", "car", "plane"]
    idx = hash(image_data) % len(categories)
    return {
        "model": "resnet50",
        "gpu": 0,
        "prediction": categories[idx],
        "confidence": round(0.80 + (hash(image_data) % 20) / 100, 3),
    }


def bert_predict(text: str) -> dict:
    """Simulated BERT text classification (GPU 1)."""
    # In production: model = transformers.BertForSequenceClassification.from_pretrained(...)
    time.sleep(0.12)  # Simulate 120ms GPU inference
    labels = ["positive", "negative", "neutral"]
    idx = hash(text) % len(labels)
    return {
        "model": "bert-base",
        "gpu": 1,
        "sentiment": labels[idx],
        "confidence": round(0.75 + (hash(text) % 25) / 100, 3),
    }


def whisper_predict(audio_ref: str) -> dict:
    """Simulated Whisper speech-to-text (GPU 2)."""
    # In production: model = whisper.load_model("base")
    time.sleep(0.2)  # Simulate 200ms GPU inference
    return {
        "model": "whisper-base",
        "gpu": 2,
        "transcript": f"Transcribed text from {audio_ref}",
        "language": "en",
        "confidence": round(0.88 + (hash(audio_ref) % 12) / 100, 3),
    }


def main():
    uid = uuid.uuid4().hex[:8]
    channel_resnet = f"example-ml-gpu-resnet-{uid}"
    channel_bert = f"example-ml-gpu-bert-{uid}"
    channel_whisper = f"example-ml-gpu-whisper-{uid}"

    # --- Adapter for ResNet (GPU 0) ---
    config_resnet = KubeMQAdapterConfig(address=BROKER)
    adapter_resnet = KubeMQTaskProcessorAdapter(config_resnet)

    class _CfgResnet:
        queue_name = channel_resnet
        max_retries = 1
        failed_task_queue_name = f"{channel_resnet}.dlq"
        unprocessable_task_queue_name = ""

    adapter_resnet.initialize(consumer_concurrency=2, task_processor_config=_CfgResnet())
    adapter_resnet.register_task_handler(resnet_predict, name="predict")
    adapter_resnet.start_consumer()

    # --- Adapter for BERT (GPU 1) ---
    config_bert = KubeMQAdapterConfig(address=BROKER)
    adapter_bert = KubeMQTaskProcessorAdapter(config_bert)

    class _CfgBert:
        queue_name = channel_bert
        max_retries = 1
        failed_task_queue_name = f"{channel_bert}.dlq"
        unprocessable_task_queue_name = ""

    adapter_bert.initialize(consumer_concurrency=2, task_processor_config=_CfgBert())
    adapter_bert.register_task_handler(bert_predict, name="predict")
    adapter_bert.start_consumer()

    # --- Adapter for Whisper (GPU 2) ---
    config_whisper = KubeMQAdapterConfig(address=BROKER)
    adapter_whisper = KubeMQTaskProcessorAdapter(config_whisper)

    class _CfgWhisper:
        queue_name = channel_whisper
        max_retries = 1
        failed_task_queue_name = f"{channel_whisper}.dlq"
        unprocessable_task_queue_name = ""

    adapter_whisper.initialize(consumer_concurrency=1, task_processor_config=_CfgWhisper())
    adapter_whisper.register_task_handler(whisper_predict, name="predict")
    adapter_whisper.start_consumer()

    try:
        print("GPU Multi-Model Setup:")
        print(f"  GPU 0 (ResNet):  {channel_resnet}")
        print(f"  GPU 1 (BERT):    {channel_bert}")
        print(f"  GPU 2 (Whisper): {channel_whisper}")

        # Dispatch requests to appropriate model queues
        print("\nDispatching requests...")

        # Image classification requests -> ResNet
        resnet_tasks = []
        for i in range(3):
            result = adapter_resnet.enqueue_task_sync("predict", args=[f"image_{i}.jpg"])
            resnet_tasks.append(result.id)
            print(f"  Image {i} -> ResNet (GPU 0)")

        # Text classification requests -> BERT
        bert_tasks = []
        texts = ["Great product!", "Terrible service.", "It was okay."]
        for text in texts:
            result = adapter_bert.enqueue_task_sync("predict", args=[text])
            bert_tasks.append(result.id)
            print(f"  Text '{text}' -> BERT (GPU 1)")

        # Audio transcription requests -> Whisper
        whisper_tasks = []
        for i in range(2):
            result = adapter_whisper.enqueue_task_sync("predict", args=[f"audio_clip_{i}.wav"])
            whisper_tasks.append(result.id)
            print(f"  Audio {i} -> Whisper (GPU 2)")

        # Collect all results
        print("\nCollecting results...")
        all_tasks = {
            "ResNet": (adapter_resnet, resnet_tasks),
            "BERT": (adapter_bert, bert_tasks),
            "Whisper": (adapter_whisper, whisper_tasks),
        }

        timeout = 30
        start = time.perf_counter()
        results: dict[str, list] = {"ResNet": [], "BERT": [], "Whisper": []}

        for model_name, (adapter, task_ids) in all_tasks.items():
            for tid in task_ids:
                while (time.perf_counter() - start) < timeout:
                    status = adapter.get_task_status_sync(tid)
                    if status.status in ("SUCCESS", "FAILURE"):
                        results[model_name].append(
                            {
                                "status": status.status,
                                "result": status.result,
                            }
                        )
                        break
                    time.sleep(0.5)
                else:
                    results[model_name].append({"status": "TIMEOUT", "result": None})

        # Display results
        print("\n--- Multi-Model Results ---")
        for model_name, model_results in results.items():
            print(f"\n{model_name}:")
            for i, r in enumerate(model_results):
                print(f"  Task {i + 1}: {r['status']} -> {r['result']}")
    finally:
        adapter_resnet.stop_consumer()
        adapter_bert.stop_consumer()
        adapter_whisper.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
