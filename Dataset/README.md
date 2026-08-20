# Dataset — Industrial Water Pump Anomaly Detection (Audio)

> **Source**: [Kaggle — Anomaly Detection in Water Pump Using Audio Data](https://www.kaggle.com/datasets/vuppalaadithyasairam/anomaly-detection-in-water-pump-using-audio-data)

---

## Overview

This dataset contains raw `.wav` audio recordings of an industrial water pump captured under two conditions:

| Condition | Description |
|-----------|-------------|
| **Normal** | Pump operating under healthy, standard conditions |
| **Anomaly** | Pump operating with induced mechanical faults |

The audio files are used to train and evaluate the **SW-WaveNet** anomalous sound detection model, which learns a discriminative embedding of normal pump acoustics and flags deviations as anomalies at inference time.

---

## Directory Structure

```
Dataset/
├── train-normal/       # Normal audio used for model training (~1,000 files)
│   └── normal_id_00_*.wav
├── test-normal/        # Normal audio held out for evaluation
│   └── normal_id_00_*.wav
├── anomaly/            # Anomalous audio for evaluation only (never seen during training)
│   └── anomaly_id_00_*.wav
├── download_dataset.py # Script to fetch the dataset from Kaggle
└── README.md
```

> **Note**: Audio subfolders are excluded from git (`.gitignore`). Only this README and the download script are tracked.

---

## Audio Specifications

| Property | Value |
|----------|-------|
| **Format** | WAV (PCM) |
| **Sample Rate** | 16,000 Hz |
| **Duration per file** | ~10 seconds |
| **Channels** | Mono |
| **File size** | ~320 KB each |

Each 10-second clip is the model's native input window — two consecutive 5-second prediction steps are processed together by the SW-WaveNet dual-branch encoder.

---

## How to Download

### Option 1 — Automated Script (Recommended)

```bash
pip install kagglehub
python Dataset/download_dataset.py
```

The script:
1. Downloads the dataset to the kagglehub local cache
2. Copies `train-normal/`, `test-normal/`, and `anomaly/` directly into `./Dataset/`

### Option 2 — Kaggle CLI

```bash
pip install kaggle
kaggle datasets download -d vuppalaadithyasairam/anomaly-detection-in-water-pump-using-audio-data
unzip anomaly-detection-in-water-pump-using-audio-data.zip -d Dataset/
```

### Option 3 — Manual

Download directly from the [Kaggle dataset page](https://www.kaggle.com/datasets/vuppalaadithyasairam/anomaly-detection-in-water-pump-using-audio-data) and extract into `Dataset/`.

---

## How the Dataset Is Used

| Split | Role |
|-------|------|
| `train-normal/` | Training: the model learns embeddings of normal pump audio exclusively |
| `test-normal/` | Evaluation: used to compute the per-machine anomaly score threshold (τ) and False Alarm Rate |
| `anomaly/` | Evaluation: used to compute the True Detection Rate (TPR) and Miss Rate (FNR) |

> The model is trained **only on normal audio** — anomaly detection is purely one-class. The `anomaly/` folder is never used during training.

---

## Kaggle Acknowledgement

Dataset published by **Vuppala Adithya Sairam** on Kaggle.  
License: refer to the [dataset page](https://www.kaggle.com/datasets/vuppalaadithyasairam/anomaly-detection-in-water-pump-using-audio-data) for usage terms.
