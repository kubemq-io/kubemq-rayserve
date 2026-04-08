"""
Image Classification Pipeline — 3-Stage Preprocess/Classify/Postprocess

Demonstrates:
    - Multi-stage ML pipeline via chained enqueue calls
    - Preprocess -> classify -> postprocess pipeline
    - Passing intermediate results between stages
    - Simulated image processing without ML dependencies

Usage:
    python examples/ml_patterns/image_classification_pipeline.py

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


def preprocess_image(image_path: str, target_size: int = 224) -> dict:
    """Stage 1: Simulated image preprocessing (resize, normalize)."""
    # In production: img = PIL.Image.open(image_path).resize((target_size, target_size))
    # In production: tensor = torchvision.transforms.ToTensor()(img)
    time.sleep(0.05)  # Simulate preprocessing
    return {
        "stage": "preprocess",
        "image_path": image_path,
        "tensor_shape": [3, target_size, target_size],
        "normalized": True,
        "pixel_mean": [0.485, 0.456, 0.406],
    }


def classify_image(preprocessed: dict) -> dict:
    """Stage 2: Simulated image classification."""
    # In production: model = torchvision.models.resnet50(pretrained=True)
    # In production: output = model(tensor.unsqueeze(0))
    time.sleep(0.1)  # Simulate model inference
    categories = {
        0: ("tabby cat", 0.92),
        1: ("golden retriever", 0.88),
        2: ("sports car", 0.95),
        3: ("airplane", 0.91),
        4: ("pizza", 0.87),
    }
    idx = hash(preprocessed["image_path"]) % len(categories)
    label, confidence = categories[idx]
    return {
        "stage": "classify",
        "raw_label": label,
        "raw_confidence": confidence,
        "top_5": [
            {"label": label, "score": confidence},
            {"label": "other_class_1", "score": round(confidence * 0.3, 3)},
            {"label": "other_class_2", "score": round(confidence * 0.15, 3)},
            {"label": "other_class_3", "score": round(confidence * 0.08, 3)},
            {"label": "other_class_4", "score": round(confidence * 0.04, 3)},
        ],
        "input_shape": preprocessed["tensor_shape"],
    }


def postprocess_result(classification: dict) -> dict:
    """Stage 3: Simulated postprocessing (filtering, formatting)."""
    # In production: apply thresholds, map to display names, add metadata
    time.sleep(0.02)  # Simulate postprocessing
    top_result = classification["top_5"][0]
    return {
        "stage": "postprocess",
        "final_label": top_result["label"].replace("_", " ").title(),
        "confidence": top_result["score"],
        "confidence_level": (
            "high"
            if top_result["score"] > 0.9
            else "medium"
            if top_result["score"] > 0.7
            else "low"
        ),
        "all_predictions": [
            {"label": p["label"], "score": p["score"]}
            for p in classification["top_5"]
            if p["score"] > 0.05
        ],
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
    uid = uuid.uuid4().hex[:8]
    channel_preprocess = f"example-ml-imgpipe-pre-{uid}"
    channel_classify = f"example-ml-imgpipe-cls-{uid}"
    channel_postprocess = f"example-ml-imgpipe-post-{uid}"

    # --- Stage 1 adapter: Preprocessing ---
    config_pre = KubeMQAdapterConfig(address=BROKER)
    adapter_pre = KubeMQTaskProcessorAdapter(config_pre)

    class _CfgPre:
        queue_name = channel_preprocess
        max_retries = 1
        failed_task_queue_name = f"{channel_preprocess}.dlq"
        unprocessable_task_queue_name = ""

    adapter_pre.initialize(consumer_concurrency=2, task_processor_config=_CfgPre())
    adapter_pre.register_task_handler(preprocess_image, name="preprocess")
    adapter_pre.start_consumer()

    # --- Stage 2 adapter: Classification ---
    config_cls = KubeMQAdapterConfig(address=BROKER)
    adapter_cls = KubeMQTaskProcessorAdapter(config_cls)

    class _CfgCls:
        queue_name = channel_classify
        max_retries = 1
        failed_task_queue_name = f"{channel_classify}.dlq"
        unprocessable_task_queue_name = ""

    adapter_cls.initialize(consumer_concurrency=1, task_processor_config=_CfgCls())
    adapter_cls.register_task_handler(classify_image, name="classify")
    adapter_cls.start_consumer()

    # --- Stage 3 adapter: Postprocessing ---
    config_post = KubeMQAdapterConfig(address=BROKER)
    adapter_post = KubeMQTaskProcessorAdapter(config_post)

    class _CfgPost:
        queue_name = channel_postprocess
        max_retries = 1
        failed_task_queue_name = f"{channel_postprocess}.dlq"
        unprocessable_task_queue_name = ""

    adapter_post.initialize(consumer_concurrency=2, task_processor_config=_CfgPost())
    adapter_post.register_task_handler(postprocess_result, name="postprocess")
    adapter_post.start_consumer()

    try:
        images = ["cat_photo.jpg", "dog_portrait.png", "car_image.jpg"]
        print("Image Classification Pipeline (3 stages)")
        print(f"  Stage 1 (preprocess):  {channel_preprocess}")
        print(f"  Stage 2 (classify):    {channel_classify}")
        print(f"  Stage 3 (postprocess): {channel_postprocess}")

        for image_path in images:
            print(f"\n--- Processing: {image_path} ---")

            # Stage 1: Preprocess
            print("  Stage 1: Preprocessing...")
            pre_task = adapter_pre.enqueue_task_sync(
                "preprocess", args=[image_path], kwargs={"target_size": 224}
            )
            pre_result = poll_result(adapter_pre, pre_task.id)
            if not pre_result or pre_result["status"] != "SUCCESS":
                print(f"  Preprocessing failed: {pre_result}")
                continue
            print(f"  -> tensor_shape={pre_result['result']['tensor_shape']}")

            # Stage 2: Classify (pass preprocessed data)
            print("  Stage 2: Classifying...")
            cls_task = adapter_cls.enqueue_task_sync("classify", args=[pre_result["result"]])
            cls_result = poll_result(adapter_cls, cls_task.id)
            if not cls_result or cls_result["status"] != "SUCCESS":
                print(f"  Classification failed: {cls_result}")
                continue
            print(f"  -> raw_label={cls_result['result']['raw_label']}")

            # Stage 3: Postprocess (format final output)
            print("  Stage 3: Postprocessing...")
            post_task = adapter_post.enqueue_task_sync("postprocess", args=[cls_result["result"]])
            post_result = poll_result(adapter_post, post_task.id)
            if not post_result or post_result["status"] != "SUCCESS":
                print(f"  Postprocessing failed: {post_result}")
                continue

            final = post_result["result"]
            print(
                f"  -> Final: {final['final_label']} "
                f"(confidence={final['confidence']}, level={final['confidence_level']})"
            )
    finally:
        adapter_pre.stop_consumer()
        adapter_cls.stop_consumer()
        adapter_post.stop_consumer()

    print("\nExample complete.")


if __name__ == "__main__":
    main()
