"""
Enterprise Voice-to-Text System
--------------------------------
Real-time speech transcription using Whisper
Saves text line-by-line into Excel
Audio is NOT stored permanently
"""

import whisper
import sounddevice as sd
import numpy as np
import pandas as pd
import scipy.io.wavfile as wav
from datetime import datetime
from pathlib import Path
import logging
import tempfile
import os


# ============================================================
# Configuration
# ============================================================

MODEL_SIZE = "base"
SAMPLE_RATE = 16000
CHANNELS = 1
RECORD_SECONDS = 5
OUTPUT_FILE = r"C:\Users\user\voice_transcription_log.xlsx"


# ============================================================
# Logging Setup
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("EnterpriseVoiceSystem")


# ============================================================
# Audio Recorder Class
# ============================================================

class AudioRecorder:

    def __init__(self, sample_rate, channels, duration):
        self.sample_rate = sample_rate
        self.channels = channels
        self.duration = duration

    def record(self):
        logger.info("Recording audio...")

        audio = sd.rec(
            int(self.duration * self.sample_rate),
            samplerate=self.sample_rate,
            channels=self.channels,
            dtype="int16",
        )

        sd.wait()
        return audio


# ============================================================
# Speech Transcriber Class
# ============================================================

class SpeechTranscriber:

    def __init__(self, model_size):
        logger.info("Loading Whisper model...")
        self.model = whisper.load_model(model_size)
        logger.info("Model loaded successfully")

    def transcribe(self, audio_data, sample_rate):

        # Use temporary file (not permanently saved)
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as temp_audio:
            wav.write(temp_audio.name, sample_rate, audio_data)

            result = self.model.transcribe(temp_audio.name)
            text = result["text"].strip()

        # Remove temp file
        os.remove(temp_audio.name)

        return text


# ============================================================
# Excel Logger Class
# ============================================================

class ExcelLogger:

    def __init__(self, file_path):
        self.file_path = Path(file_path)

    def log(self, text):

        if not text:
            return

        row = {
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Text": text,
        }

        df_new = pd.DataFrame([row])

        if self.file_path.exists():
            df_existing = pd.read_excel(self.file_path)
            df_updated = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_updated = df_new

        df_updated.to_excel(self.file_path, index=False)
        logger.info(f"Saved text to Excel: {text}")


# ============================================================
# Main Voice-to-Text System
# ============================================================

class VoiceToTextSystem:

    def __init__(self):
        self.recorder = AudioRecorder(SAMPLE_RATE, CHANNELS, RECORD_SECONDS)
        self.transcriber = SpeechTranscriber(MODEL_SIZE)
        self.logger = ExcelLogger(OUTPUT_FILE)

    def run(self):
        logger.info("Voice-to-Text system started")
        logger.info("Press CTRL + C to stop")

        while True:
            try:
                audio_data = self.recorder.record()

                text = self.transcriber.transcribe(audio_data, SAMPLE_RATE)

                if text:
                    logger.info(f"Recognized: {text}")
                    self.logger.log(text)

            except KeyboardInterrupt:
                logger.info("System stopped by user")
                break

            except Exception as e:
                logger.error(f"Error: {e}")


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    system = VoiceToTextSystem()
    system.run()
  