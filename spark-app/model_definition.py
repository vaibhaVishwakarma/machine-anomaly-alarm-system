import torch
import torch.nn as nn
import torch.nn.functional as F
import torchaudio
import math

# ==========================================
# 1. ArcFace Layer
# ==========================================

class ArcFaceLayer(nn.Module):
    def __init__(self, in_features, out_classes, s=30.0, m=0.7):
        super().__init__()
        self.s = s
        self.m = m

        self.weight = nn.Parameter(
            torch.FloatTensor(out_classes, in_features)
        )
        nn.init.xavier_uniform_(self.weight)

        self.cos_m = math.cos(m)
        self.sin_m = math.sin(m)
        self.th = math.cos(math.pi - m)
        self.mm = math.sin(math.pi - m) * m

    def forward(self, features, labels=None):

        cosine = F.linear(
            F.normalize(features),
            F.normalize(self.weight)
        )

        if labels is None:
            return cosine * self.s

        sine = torch.sqrt(1.0 - torch.pow(cosine, 2))
        phi = cosine * self.cos_m - sine * self.sin_m
        phi = torch.where(cosine > self.th, phi, cosine - self.mm)

        one_hot = torch.zeros_like(cosine)
        one_hot.scatter_(1, labels.view(-1, 1).long(), 1)

        output = (one_hot * phi) + ((1.0 - one_hot) * cosine)
        output *= self.s

        return output

# ==========================================
# 2. Wavegram Network
# ==========================================

class WavegramNet(nn.Module):
    def __init__(self):
        super().__init__()
        self.conv = nn.Conv1d(
            1, 128,
            kernel_size=1024,
            stride=512,
            padding=512
        )

    def forward(self, x):
        return self.conv(x)

# ==========================================
# 3. Dilated Residual Block
# ==========================================

class DilatedResidualBlock(nn.Module):
    def __init__(self, channels, dilation):
        super().__init__()

        self.dilated_conv = nn.Conv1d(
            channels,
            channels,
            kernel_size=2,
            dilation=dilation
        )

        self.batch_norm = nn.BatchNorm1d(channels)
        self.conv_1x1_res = nn.Conv1d(channels, channels, 1)
        self.conv_1x1_skip = nn.Conv1d(channels, channels, 1)

    def forward(self, x):

        residual = x

        pad_size = self.dilated_conv.dilation[0] * (
            self.dilated_conv.kernel_size[0] - 1
        )

        padded_x = F.pad(x, (pad_size, 0))
        out = self.dilated_conv(padded_x)
        out = self.batch_norm(out)

        out = torch.tanh(out) * torch.sigmoid(out)

        res_out = self.conv_1x1_res(out) + residual
        skip_out = self.conv_1x1_skip(out)

        return res_out, skip_out

# ==========================================
# 4. Modified WaveNet Encoder
# ==========================================

class ModifiedWaveNet(nn.Module):
    def __init__(self, in_channels=128, out_channels=512, time_frames=313):
        super().__init__()

        self.causal_conv = nn.Conv1d(in_channels, out_channels, 2)

        self.blocks = nn.ModuleList()
        for _ in range(3):
            for dilation in [1, 2, 4, 8]:
                self.blocks.append(
                    DilatedResidualBlock(out_channels, dilation)
                )

        self.bn1 = nn.BatchNorm1d(out_channels)
        self.relu = nn.ReLU()

        self.depthwise_conv = nn.Conv1d(
            out_channels,
            out_channels,
            kernel_size=time_frames,
            groups=out_channels
        )

        self.bn2 = nn.BatchNorm1d(out_channels)
        self.final_conv = nn.Conv1d(out_channels, 128, 1)

    def forward(self, x):

        padded_x = F.pad(x, (1, 0))
        x = self.causal_conv(padded_x)

        skip_connections = []

        for block in self.blocks:
            x, skip = block(x)
            skip_connections.append(skip)

        out = sum(skip_connections)

        out = self.bn1(out)
        out = self.relu(out)
        out = self.depthwise_conv(out)
        out = self.bn2(out)
        out = self.final_conv(out)

        return out.squeeze(2)

# ==========================================
# 5. Full Pipeline (Inference Ready)
# ==========================================

class SW_WaveNet_Pipeline(nn.Module):

    def __init__(self, sample_rate=16000, num_classes=41):
        super().__init__()

        self.mel_extractor = torchaudio.transforms.MelSpectrogram(
            sample_rate=sample_rate,
            n_fft=1024,
            hop_length=512,
            n_mels=128
        )

        self.amplitude_to_db = torchaudio.transforms.AmplitudeToDB()

        self.wavegram_net = WavegramNet()
        self.wavenet_spectrogram = ModifiedWaveNet()
        self.wavenet_wavegram = ModifiedWaveNet()

        self.arcface = ArcFaceLayer(
            in_features=256,
            out_classes=num_classes
        )

    def forward(self, raw_audio, labels=None):

        mel_spec = self.mel_extractor(raw_audio).squeeze(1)
        log_mel = self.amplitude_to_db(mel_spec)

        wavegram = self.wavegram_net(raw_audio)

        vec_spec = self.wavenet_spectrogram(log_mel)
        vec_wave = self.wavenet_wavegram(wavegram)

        combined = torch.cat((vec_spec, vec_wave), dim=1)

        logits = self.arcface(combined, labels)

        return logits

    def get_anomaly_score(self, raw_audio, target_machine_id):

        self.eval()

        with torch.no_grad():
            logits = self.forward(raw_audio, labels=None)
            probs = F.softmax(logits, dim=1)

            target_prob = probs[0, target_machine_id].item()

            anomaly_score = -math.log(target_prob + 1e-9)

        return anomaly_score