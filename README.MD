# Polar H10 ECG Data Collector

This project is a **Python-based tool** for connecting to the **Polar H10 heart rate sensor** via Bluetooth, 
streaming raw **ECG (electrocardiogram) data**, and storing it into structured CSV files for later analysis.

It is part of my Master's thesis setup, but the code can be reused in other research or biofeedback projects.

---

## Features
- Connects automatically to the Polar H10 device via BLE.
- Streams **ECG samples** in real time.
- Saves data to CSV with:
  - Real timestamp (human-readable)
  - Sensor timestamp (nanoseconds)
  - Raw ECG samples (µV)
- Simple structure: one script, easy to adapt.

---

## Project Structure
```
polar-h10-ecg/
│
├── polar_ecg/
│   ├── __init__.py       # Package metadata
│   ├── collector.py      # Main script (connects, streams, saves ECG)
│
├── data/                 # Folder where CSV files will be stored
│
├── requirements.txt      # Python dependencies
└── README.md             # This file
```

---

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/YOUR_USERNAME/polar-h10-ecg.git
   cd polar-h10-ecg
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

---

## Usage

Run the collector with:

```bash
python polar_ecg/collector.py <participant_id> <task_type>
```

Example:
```bash
python polar_ecg/collector.py P01 simple
```

This will create a CSV file at:
```
data/P01/P01_simple_ecg_data.csv
```

---

## Example Output (CSV)

| Real Timestamp              | Type | Timestamp (ns)  | ECG Samples (µV) |
|-----------------------------|------|-----------------|------------------|
| 2025-10-02 19:25:01.123456  | ECG  | 169626990112345 | -123             |
| 2025-10-02 19:25:01.123789  | ECG  | 169626990123789 | -120             |

---

## Notes
- Make sure Bluetooth is enabled on your machine.
- If you want to change the device address, update `ADDRESS` in `collector.py`.
- Tested with **Polar H10** and Python 3.10+.

---

## License
MIT License © 2025 João Vaz
