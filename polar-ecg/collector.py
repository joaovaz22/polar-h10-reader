"""
Polar H10 ECG Data Collector
============================

This script connects to a Polar H10 heart rate sensor using BLE,
streams raw ECG data, and stores it into CSV files for later analysis.

It was developed as part of a thesis project on:
"Wearable Heartbeat Vibrations: Exploring Balance and Arousal in Smartwatch-Based Feedback".

Author: João Vaz
"""

import sys
import os
import csv
import asyncio
import datetime
from bleak import BleakClient
from bleakheart import PolarMeasurementData


# -------------------------
# Configuration
# -------------------------

# Default BLE MAC address of the Polar H10 (replace with your own device if needed)
ADDRESS = "DD:F1:A1:75:D8:BD"


# -------------------------
# Utility Functions
# -------------------------

def ensure_participant_folder(participant: str) -> str:
    """
    Ensures a data folder exists for the given participant.
    Returns the full path to the folder.
    """
    base_dir = os.getcwd()
    participant_folder = os.path.join(base_dir, "data", participant)

    if not os.path.exists(participant_folder):
        os.makedirs(participant_folder)

    return participant_folder


# -------------------------
# Polar H10 Streaming
# -------------------------

async def read_ecg_task(device, queue: asyncio.Queue, task_type: str):
    """
    Connects to Polar H10, starts ECG streaming,
    and controls duration depending on the type of task.
    """

    print(f"Connecting to {device}...")
    async with BleakClient(device) as client:
        print(f"Connected: {client.is_connected}", flush=True)

        # Initialize Polar measurement object
        pmd = PolarMeasurementData(client, ecg_queue=queue)
        err_code, err_msg, _ = await pmd.start_streaming("ECG")
        if err_code != 0:
            print(f"PMD error: {err_msg}")
            return

        # Allow sensor to stabilize before recording
        print("Waiting 3s for sensor stabilization...")
        await asyncio.sleep(3)

        # Start ECG collection depending on task type
        queue.put_nowait(("START", None, None, None))
        if task_type == "simple":
            print("Recording 60 seconds of ECG data.", flush=True)
            await asyncio.sleep(60)
        else:
            print("Recording 40 seconds of ECG data.", flush=True)
            await asyncio.sleep(40)

        queue.put_nowait(("QUIT", None, None, None))
        await pmd.stop_streaming("ECG")


async def store_ecg_task(queue: asyncio.Queue, participant: str, task_type: str):
    """
    Retrieves ECG data from the queue and writes it into a CSV file.
    """
    folder = ensure_participant_folder(participant)
    filename = os.path.join(folder, f"{participant}_{task_type}_ecg_data.csv")

    started = False

    with open(filename, "a", newline="") as file:
        writer = csv.writer(file)

        while not queue.empty():
            frame = await queue.get()

            if frame[0] == "START":
                writer.writerow(["Real Timestamp", "Type", "Timestamp (ns)", "ECG Samples (uV)"])
                print("Start signal received — recording ECG.")
                started = True
                continue

            if frame[0] == "QUIT":
                print("Quit signal received — stopping recording.")
                started = False
                continue

            if started and frame[0] == "ECG":
                _, timestamp, samples = frame
                timestamp_s = timestamp / 1_000_000_000
                real_time = datetime.datetime.fromtimestamp(timestamp_s).strftime("%Y-%m-%d %H:%M:%S.%f")

                writer.writerow([real_time, "ECG", timestamp, samples])


# -------------------------
# Main Entry Point
# -------------------------

async def main():
    if len(sys.argv) < 3:
        print("Usage: python polar_ecg.py <participant_id> <task_type>", flush=True)
        sys.exit(1)

    participant = sys.argv[1]
    task_type = sys.argv[2]

    print(f"Participant: {participant}")
    print(f"Task type: {task_type}")

    device = ADDRESS
    if device is None:
        print("Polar device not found.")
        sys.exit(1)

    ecg_queue = asyncio.Queue()

    while True:
        await read_ecg_task(device, ecg_queue, task_type)
        await store_ecg_task(ecg_queue, participant, task_type)


if __name__ == "__main__":
    asyncio.run(main())
