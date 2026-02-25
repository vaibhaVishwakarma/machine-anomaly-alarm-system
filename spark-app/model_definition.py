# ==========================================
# model_definition.py
# XGBoost Industrial Audio Model (10s clips)
# ==========================================

import io
import joblib
import numpy as np
import torch
import torchaudio
import librosa


# ==========================================
# 1. Industrial Audio Processor
# ==========================================

class IndustrialAudioProcessor:
    def __init__(self, sample_rate=16000, duration=10):
        self.sr = sample_rate
        self.target_length = sample_rate * duration

        self.mel_extractor = torchaudio.transforms.MelSpectrogram(
            sample_rate=self.sr,
            n_fft=1024,
            hop_length=512,
            n_mels=128
        )

        self.mfcc_extractor = torchaudio.transforms.MFCC(
            sample_rate=self.sr,
            n_mfcc=12,
            melkwargs={
                "n_fft": 1024,
                "hop_length": 512,
                "n_mels": 128
            }
        )

        self.amplitude_to_db = torchaudio.transforms.AmplitudeToDB()

    # --------------------------------------

    def preprocess_signal(self, waveform):

        # Pre-emphasis
        waveform_np = librosa.effects.preemphasis(
            waveform.numpy()[0]
        )
        waveform = torch.from_numpy(waveform_np).unsqueeze(0)

        # RMS normalization to -20 dBFS
        rms = torch.sqrt(torch.mean(waveform ** 2))
        if rms > 0:
            target_rms = 10 ** (-20 / 20.0)
            waveform = waveform * (target_rms / rms)

        # Strict 10s framing
        if waveform.shape[1] < self.target_length:
            waveform = torch.nn.functional.pad(
                waveform,
                (0, self.target_length - waveform.shape[1])
            )
        elif waveform.shape[1] > self.target_length:
            waveform = waveform[:, :self.target_length]

        return waveform

    # --------------------------------------

    def extract_features(self, waveform):

        waveform = self.preprocess_signal(waveform)

        mel = self.amplitude_to_db(
            self.mel_extractor(waveform)
        ).squeeze(0).T

        mfcc = self.mfcc_extractor(waveform).squeeze(0).T

        # Combine → (Time, 140)
        combined = torch.cat((mel, mfcc), dim=1)

        # Mean over time → (140,)
        combined_flat = torch.mean(combined, dim=0)

        return combined_flat.numpy()


# ==========================================
# 2. Load XGBoost Model
# ==========================================

MODEL_PATH = "/spark-app/model/xgb_audio_model.pkl"

xgb_model = joblib.load(MODEL_PATH)

processor = IndustrialAudioProcessor()


# ==========================================
# 3. Prediction Function
# ==========================================

def predict_from_bytes(audio_bytes):

    waveform, sr = torchaudio.load(io.BytesIO(audio_bytes))

    features = processor.extract_features(waveform)

    features = features.reshape(1, -1)

    prob = xgb_model.predict_proba(features)[0][1]

    classification = "anomalous" if prob > 0.5 else "normal"

    return float(prob), classification