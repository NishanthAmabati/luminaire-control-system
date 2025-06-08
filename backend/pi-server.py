import asyncio
import websockets
import json
import os
import socket
import threading
import logging
import time
import re
from concurrent.futures import ThreadPoolExecutor
import csv
import datetime
import psutil
import yaml
import resource
import logging.handlers
import pickle  

# Load configuration from config.yaml
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Define path for state persistence
STATE_FILE = "state.pkl"

# Configure Logging with rotation
timestamp = datetime.datetime.now().strftime(config["logging"]["filename_template"])
handler = logging.handlers.TimedRotatingFileHandler(
    timestamp,
    when=config["logging"]["rotation_when"],
    interval=config["logging"]["rotation_interval"],
    backupCount=config["logging"]["rotation_backup_count"]
)
logging.basicConfig(
    level=getattr(logging, config["logging"]["level"]),
    format="%(asctime)s [%(levelname)s] - %(message)s",
    handlers=[handler]
)

# Initial state for WebSocket clients
state = {
    "auto_mode": False,
    "available_scenes": [],
    "current_scene": None,
    "loaded_scene": None,
    "cw": 50.0,
    "ww": 50.0,
    "scheduler": {
        "current_cct": 3500,
        "current_interval": 0,
        "total_intervals": config["luminaire_operations"]["total_intervals"],
        "status": "idle",
        "interval_progress": 0
    },
    "connected_devices": {},
    "basicLogs": [],
    "advancedLogs": [],
    "scene_data": {"cct": [], "intensity": []},
    "current_cct": 3500,
    "current_intensity": 250,
    "is_manual_override": False,
    "cpu_percent": 0.0,
    "mem_percent": 0.0,
    "temperature": None,
    "activationTime": None,
    "isSystemOn": True,
    "last_state": {  # Store last state for restoration
        "auto_mode": False,
        "current_scene": None,
        "cw": 50.0,
        "ww": 50.0,
        "current_intensity": 250
    }
}

scene_data = {}
clients = set()

class LuminaireOperations:
    def __init__(self):
        self._devices_lock = threading.RLock()
        self._state_lock = threading.Lock()
        self._send_lock = threading.Lock()
        self.min_cct = config["luminaire_operations"]["min_cct"]
        self.max_cct = config["luminaire_operations"]["max_cct"]
        self.min_intensity = config["luminaire_operations"]["min_intensity"]
        self.max_intensity = config["luminaire_operations"]["max_intensity"]
        self.INACTIVITY_THRESHOLD = config["luminaire_operations"]["inactivity_threshold"]
        self.devices = {}
        self.current_interval_index = 0
        self.total_intervals = 0
        self.start_time = None
        self.stop_event = threading.Event()
        self.paused = False
        logging.debug("LuminaireOperations initialized")

    def stop_scheduler(self):
        """Stop the current scheduler task and reset state."""
        self.stop_event.set()
        if self.current_scheduler_task is not None:
            self.current_scheduler_task.cancel()  # Cancel the running task
            self.current_scheduler_task = None
            logging.debug("Current scheduler task canceled")
        with self._state_lock:
            state["scene_data"] = {"cct": [], "intensity": []}
            state["current_scene"] = None
            state["loaded_scene"] = None
            state["scheduler"]["status"] = "idle"
        self.log_basic("Scheduler stopped")
        logging.debug("Scheduler stopped and state reset")

    def get_system_stats(self):
        logging.debug("Fetching system stats")
        cpu_percent, mem_percent = psutil.cpu_percent(interval=None), psutil.virtual_memory().percent
       # Fetch temperature data
        temperature = None
        try:
            temps = psutil.sensors_temperatures()
            # Look for common temperature sensors (e.g., 'coretemp' for Intel CPUs, 'k10temp' for AMD)
            for sensor in ['coretemp', 'k10temp', 'cpu_thermal']:
                if sensor in temps and temps[sensor]:
                    # Use the first available reading (highest priority)
                    temperature = temps[sensor][0].current
                    break
            if temperature is None:
                logging.debug("No temperature sensor data available")
            else:
                logging.debug(f"Temperature: {temperature}°C")
        except (AttributeError, NotImplementedError):
            logging.warning("Temperature monitoring not supported on this platform")
        logging.debug(f"System stats - CPU: {cpu_percent}%, Mem: {mem_percent}%, Temp: {temperature}°C")
        return cpu_percent, mem_percent, temperature

    def add(self, ip: str, writer):
        with self._devices_lock:
            self.devices[ip] = {"writer": writer, "last_seen": time.time(), "cw": 50.0, "ww": 50.0}
            if ip not in state["connected_devices"]:
                state["connected_devices"][ip] = {"cw": 50.0, "ww": 50.0}
            self.log_advanced(f"Luminaire connected: {ip}")
            logging.info(f"Added luminaire {ip}")
            logging.debug(f"Device list updated: {list(self.devices.keys())}")

    def disconnect(self, ip: str):
        with self._devices_lock:
            if ip in self.devices:
                writer = self.devices[ip].get("writer")
                if writer:
                    writer.close()
                del self.devices[ip]
                if ip in state["connected_devices"]:
                    del state["connected_devices"][ip]
                self.log_advanced(f"Luminaire disconnected: {ip}")
            logging.info(f"Disconnected {ip}")
            logging.debug(f"Device list after disconnect: {list(self.devices.keys())}")

    def clearALL(self):
        with self._devices_lock:
            for ip in list(self.devices.keys()):
                self.disconnect(ip)
            state["connected_devices"] = {}
        self.log_advanced("All luminaires disconnected.")
        logging.info("All luminaires disconnected.")
        logging.debug("Device list cleared")

    def processACK(self, ip: str, response: str) -> bool:
        logging.debug(f"Processing ACK from {ip}: {response}")
        try:
            if isinstance(response, bytes):
                response = response.decode('utf-8', errors='ignore')
            match = re.match(r"\*001(\d{3})(\d{3})ACK(\d{3})(\d{3})#", response)
            if not match:
                logging.warning(f"Invalid ACK format from {ip}: {response}")
                return False
            cw_raw, ww_raw = match.group(3), match.group(4)
            cw, ww = int(cw_raw) / 10, int(ww_raw) / 10
            with self._devices_lock:
                if ip in self.devices:
                    self.update_cw_ww_intensity(ip, cw, ww)  # Updates devices[ip] and state["connected_devices"]
                    self.log_advanced(f"Received [{ip}]: {response}")
                    # Remove state updates that affect controls
                    # with self._state_lock:
                    #     state["cw"] = cw
                    #     state["ww"] = ww
                    #     state["current_cct"] = self.calculate_cct_from_cw_ww(cw, ww)
                    logging.debug(f"Updated device {ip} - CW: {cw}%, WW: {ww}%")
                    return True
            return False
        except Exception as e:
            self.log_advanced(f"Error processing ACK for {ip}: {e}")
            logging.error(f"Error processing ACK for {ip}: {e}", exc_info=True)
            return False

    def log_basic(self, message: str):
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        with self._state_lock:
            state["basicLogs"].append(f"[{timestamp}] {message}")
            state["basicLogs"] = state["basicLogs"][-config["luminaire_operations"]["log_basic_max_entries"]:]
        logging.info(f"Basic Log: {message}")

    def log_advanced(self, message: str):
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        with self._state_lock:
            state["advancedLogs"].append(f"[{timestamp}] {message}")
            state["advancedLogs"] = state["advancedLogs"][-config["luminaire_operations"]["log_advanced_max_entries"]:]
        logging.debug(f"Advanced Log: {message}")

    def list(self) -> dict:
        logging.debug("Listing connected devices")
        with self._devices_lock:
            now = time.time()
            devices = {ip: {"cw": data.get("cw"), "ww": data.get("ww")} for ip, data in self.devices.items() if now - data["last_seen"] < self.INACTIVITY_THRESHOLD}
            logging.debug(f"Active devices: {list(devices.keys())}")
            return devices

    def update_cw_ww_intensity(self, ip: str, cw: float, ww: float):
        with self._devices_lock:
            if ip in self.devices:
                self.devices[ip].update({"cw": cw, "ww": ww, "last_seen": time.time()})
                state["connected_devices"][ip] = {"cw": cw, "ww": ww}
                logging.debug(f"Updated {ip} - CW: {cw}%, WW: {ww}%")

    async def send(self, ip: str, cw: float, ww: float) -> bool:
        logging.debug(f"Sending to {ip} - CW: {cw}%, WW: {ww}%")
        retries = 0
        with self._devices_lock:
            if ip not in self.devices:
                logging.warning(f"Device {ip} not found for sending")
                return False
            writer = self.devices[ip]["writer"]
        while retries < config["luminaire_operations"]["max_retries"]:
            try:
                command = self.buildCommand(ip, cw, ww)
                writer.write(command.encode())
                await writer.drain()
                self.log_advanced(f"Sent [{ip}]: {command}")
                logging.debug(f"Successfully sent to {ip}")
                return True
            except (ConnectionError, OSError) as e:
                retries += 1
                self.log_advanced(f"Error sending to {ip} (retry {retries}/{config['luminaire_operations']['max_retries']}): {e}")
                logging.warning(f"Error sending to {ip} (retry {retries}/{config['luminaire_operations']['max_retries']}): {e}")
                if retries >= config["luminaire_operations"]["max_retries"]:
                    self.disconnect(ip)
            except Exception as e:
                retries += 1
                self.log_advanced(f"Unexpected error sending to {ip} (retry {retries}/{config['luminaire_operations']['max_retries']}): {e}")
                logging.warning(f"Unexpected error sending to {ip} (retry {retries}/{config['luminaire_operations']['max_retries']}): {e}")
                if retries >= config["luminaire_operations"]["max_retries"]:
                    self.disconnect(ip)
            await asyncio.sleep(0.5)
        logging.error(f"Failed to send to {ip} after {config['luminaire_operations']['max_retries']} retries")
        return False

    async def sendAll(self, cw: float, ww: float) -> tuple[bool, list]:
        logging.debug(f"Sending to all devices - CW: {cw}%, WW: {ww}%")
        failed_ips = []
        tasks = []
        with self._devices_lock:
            if not self.devices:
                logging.warning("No devices available to send to")
                return False, []
            for ip, device in self.devices.items():
                command = self.buildCommand(ip, cw, ww)
                tasks.append(self.async_send(ip, device["writer"], command))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for ip, result in zip(list(self.devices.keys()), results):
            if isinstance(result, Exception):
                failed_ips.append(ip)
                self.log_advanced(f"Error sending to {ip}: {result}")
                logging.warning(f"Error sending to {ip}: {result}")
        success = len(failed_ips) == 0
        if not success:
            self.log_advanced(f"Failed to send to luminaires: {', '.join(failed_ips)}")
        state["current_cct"] = self.calculate_cct_from_cw_ww(cw, ww)
        logging.debug(f"SendAll completed - Success: {success}, Failed IPs: {failed_ips}")
        return success, failed_ips

    async def async_send(self, ip: str, writer, command: str) -> None:
        try:
            writer.write(command.encode())
            await writer.drain()
            self.log_advanced(f"Sent [{ip}]: {command}")
            logging.debug(f"Successfully sent to {ip}")
        except Exception as e:
            self.log_advanced(f"Error sending to {ip}: {e}")
            logging.warning(f"Error sending to {ip}: {e}")
            raise

    def calculate_cw_ww_from_cct_intensity(self, cct: float, intensity: float) -> tuple[float, float]:
        logging.debug(f"Calculating CW/WW from CCT: {cct}, Intensity: {intensity}")
        cct = max(self.min_cct, min(self.max_cct, cct))
        intensity = max(self.min_intensity, min(self.max_intensity, intensity))
        intensity_percent = intensity / self.max_intensity
        cw_base = (cct - self.min_cct) / ((self.max_cct - self.min_cct) / 100.0)
        ww_base = 100.0 - cw_base
        cw = max(0.0, min(99.99, cw_base * intensity_percent))
        ww = max(0.0, min(99.99, ww_base * intensity_percent))
        logging.debug(f"Calculated - CW: {cw}%, WW: {ww}%")
        return cw, ww

    def calculate_cct_from_cw_ww(self, cw: float, ww: float) -> float:
        logging.debug(f"Calculating CCT from CW: {cw}%, WW: {ww}%")
        total = cw + ww
        cct = 3500 if total == 0 else self.min_cct + ((cw / total) * 100 * ((self.max_cct - self.min_cct) / 100.0))
        logging.debug(f"Calculated CCT: {cct}K")
        return cct

    def buildCommand(self, ip: str, cw: float, ww: float) -> str:
        logging.debug(f"Building command for {ip} - CW: {cw}%, WW: {ww}%")
        try:
            ip_parts = ip.split(".")
            ip3, ip4 = f"{int(ip_parts[2]):03}", f"{int(ip_parts[3]):03}"
            command = f"*{ip3}{ip4}{int(cw*10):03}{int(ww*10):03}##"
            logging.debug(f"Built command: {command}")
            return command
        except (ValueError, IndexError) as e:
            self.log_advanced(f"Error building command for {ip}: {e}")
            logging.error(f"Error building command for {ip}: {e}", exc_info=True)
            raise ValueError(f"Invalid IP: {ip}")

    def get_nearest_interval(self, current_time):
        logging.debug(f"Getting nearest interval for time: {current_time}")
        seconds_since_midnight = (current_time.hour * 3600) + (current_time.minute * 60) + current_time.second
        logging.debug(f"Nearest interval: {seconds_since_midnight}")
        return seconds_since_midnight

    async def run_smooth_scheduler(self, csv_path: str):
        logging.debug(f"Starting scheduler for {csv_path}")
        self.stop_event.clear()
        self.paused = False
        self.current_scheduler_task = asyncio.current_task()
        state["scheduler"]["status"] = "running"
        now = datetime.datetime.now().strftime("%H:%M:%S")
        self.log_basic(f"Activated scene: {os.path.basename(csv_path)}")
        try:
            scene_name = os.path.basename(csv_path)
            if scene_name not in scene_data:
                raise FileNotFoundError(f"Scene {scene_name} not found in scene_data")
            
            # Load scene_data_list from CSV
            with open(csv_path, newline='') as csvfile:
                reader = csv.reader(csvfile)
                next(reader)
                scene_data_list = [(int(row[0].split(':')[0]) * 60 + int(row[0].split(':')[1]), float(row[1]), float(row[2])) for row in reader]
            
            self.total_intervals = len(scene_data_list)  # Number of CSV segments
            state["scheduler"]["total_intervals"] = 8640  # Downsampled points for frontend (86,400 / 10)

            # Initialize index based on time of day
            start_time = datetime.datetime.now()
            seconds_since_midnight = (start_time.hour * 3600) + (start_time.minute * 60) + start_time.second
            self.current_interval_index = seconds_since_midnight
            self.start_time = time.time()
            logging.debug(f"Scheduler started at index: {self.current_interval_index}, start_time: {self.start_time}")

            last_interval_update = self.current_interval_index
            update_interval = config.get("luminaire_operations", {}).get("scheduler_update_interval", 2.0)

            while not self.stop_event.is_set() and self.current_interval_index < 86400:
                loop_start = time.time()
                if self.paused:
                    await asyncio.sleep(0.1)
                    logging.debug("Scheduler paused")
                    continue

                # Update index based on elapsed time
                elapsed_time = time.time() - self.start_time
                current_idx = int(seconds_since_midnight + elapsed_time) % 86400
                self.current_interval_index = current_idx

                current_interval = (current_idx // 1800) % self.total_intervals
                next_interval = (current_interval + 1) % self.total_intervals
                interval_progress = (current_idx % 1800) / 1799

                start_min, start_cct, start_intensity = scene_data_list[current_interval]
                end_min, end_cct, end_intensity = scene_data_list[next_interval]
                time_diff = ((end_min - start_min + 1440) % 1440) * 60
                cct_diff = end_cct - start_cct
                intensity_diff = end_intensity - start_intensity

                with self._state_lock:
                    state["current_cct"] = start_cct + (cct_diff * interval_progress)
                    state["current_intensity"] = start_intensity + (intensity_diff * interval_progress)
                    cw, ww = self.calculate_cw_ww_from_cct_intensity(state["current_cct"], state["current_intensity"])
                    state["cw"], state["ww"] = cw, ww
                    state["scheduler"]["interval_progress"] = (current_idx / 86400) * 100  # Progress as % of 24 hours
                    state["scheduler"]["current_interval"] = current_idx // 10  # Downsampled interval for frontend

                success, failed_ips = await self.sendAll(cw, ww)
                if not success:
                    with self._state_lock:
                        state["alert"] = f"Failed to send to luminaires: {', '.join(failed_ips)}"
                    logging.warning(f"SendAll failed for IPs: {failed_ips}")
                else:
                    logging.debug(f"Successfully sent CW: {cw}, WW: {ww} to luminaires")

                if current_idx // 10 != last_interval_update // 10:
                    last_interval_update = current_idx
                    logging.info(f"Interval update - Index: {current_idx}, Progress: {state['scheduler']['interval_progress']}%")

                logging.info(f"Index: {current_idx}, Interval: {current_interval}, "
                            f"Progress: {interval_progress:.2f}, CCT: {state['current_cct']:.1f}K, "
                            f"Intensity: {state['current_intensity']:.1f}lux, CW: {cw:.1f}%, WW: {ww:.1f}%")

                elapsed = time.time() - loop_start
                sleep_time = max(0, update_interval - elapsed)
                await asyncio.sleep(sleep_time)

            if not self.stop_event.is_set():
                with self._state_lock:
                    state["scheduler"]["status"] = "completed"
                now = datetime.datetime.now().strftime("%H:%M:%S")
                self.log_basic(f"Scene completed: {os.path.basename(csv_path)}")
                logging.info("Scene execution completed successfully!")
                logging.debug("Scheduler loop completed")
        except FileNotFoundError:
            self.log_advanced(f"CSV file not found: {csv_path}")
            logging.error(f"CSV file not found: {csv_path}", exc_info=True)
            state["scheduler"]["status"] = "failed"
        except Exception as e:
            self.log_advanced(f"Error running scheduler: {e}")
            logging.error(f"Error running scheduler: {e}", exc_info=True)
            state["scheduler"]["status"] = "failed"
        finally:
            logging.debug(f"Scheduler for {csv_path} terminated")
            with self._state_lock:
                state["scheduler"]["status"] = "idle" if state["scheduler"]["status"] != "failed" else "failed"

    
    async def cleanup_stale_devices(self):
        while True:
            with self._devices_lock:
                now = time.time()
                stale_ips = [ip for ip, data in self.devices.items() if now - data["last_seen"] > self.INACTIVITY_THRESHOLD]
                for ip in stale_ips:
                    await asyncio.sleep(0.1)
                    self.disconnect(ip)
            await asyncio.sleep(config["luminaire_operations"]["cleanup_interval"])

    def pause_scheduler(self):
        self.paused = True
        self.log_basic("Scheduler paused")
        logging.info("Scheduler paused")

    def resume_scheduler(self):
        self.paused = False
        self.start_time = time.time() - (self.current_interval_index * config["luminaire_operations"]["scheduler_update_interval"])
        self.log_basic("Scheduler resumed")
        logging.info("Scheduler resumed")
        logging.debug(f"Resumed at index: {self.current_interval_index}, start_time: {self.start_time}")

    def adjust_cw(self, delta: float, ip: str = None) -> bool:
        logging.debug(f"Adjusting CW by {delta} for IP: {ip}")
        if ip:
            with self._devices_lock:
                if ip not in self.devices or self.devices[ip].get("cw") is None:
                    logging.warning(f"Device {ip} not found or no CW data")
                    return False
                current_cw = self.devices[ip]["cw"]
                new_cw = max(0.0, min(100.0, current_cw + delta))
                self.log_basic(f"Adjusted CW to {new_cw}%")
                return asyncio.run_coroutine_threadsafe(self.send(ip, new_cw, 100 - new_cw), asyncio.get_event_loop()).result()
        else:
            with self._devices_lock:
                device_with_cw = next((ip for ip, data in self.devices.items() if data.get("cw") is not None), None)
                if not device_with_cw:
                    logging.warning("No device with CW data found")
                    return False
                current_cw = self.devices[device_with_cw]["cw"]
            new_cw = max(0.0, min(100.0, current_cw + delta))
            self.log_basic(f"Adjusted CW to {new_cw}%")
            return asyncio.run_coroutine_threadsafe(self.sendAll(new_cw, 100 - new_cw), asyncio.get_event_loop()).result()[0]

def save_state():
    """Save critical state to a pickle file."""
    try:
        persistent_state = {
            "auto_mode": state["auto_mode"],
            "current_scene": state["current_scene"],
            "cw": state["cw"],
            "ww": state["ww"],
            "current_intensity": state["current_intensity"],
            "isSystemOn": state["isSystemOn"]
        }
        with open(STATE_FILE, "wb") as f:
            pickle.dump(persistent_state, f)
        logging.debug(f"State saved to {STATE_FILE}")
    except Exception as e:
        logging.error(f"Failed to save state to {STATE_FILE}: {e}")

class LuminaireServer:
    def __init__(self, luminaire_ops=None):
        self.host = config["server"]["host"]
        self.port = config["server"]["port"]
        self.luminaire_ops = luminaire_ops
        self.running = False
        self.server = None
        logging.debug("LuminaireServer initialized")

    async def emit_status_update(self, websocket):
        logging.debug("Emitting status update")
        state_update = {
            "auto_mode": state["auto_mode"],
            "available_scenes": state["available_scenes"],
            "current_scene": state["current_scene"],
            "loaded_scene": state["loaded_scene"],
            "cw": state["cw"],
            "ww": state["ww"],
            "scheduler": state["scheduler"],
            "connected_devices": state["connected_devices"],
            "basicLogs": state["basicLogs"],
            "advancedLogs": state["advancedLogs"],
            "current_cct": state["current_cct"],
            "current_intensity": state["current_intensity"],
            "is_manual_override": state["is_manual_override"],
            "cpu_percent": state["cpu_percent"],
            "mem_percent": state["mem_percent"],
            "temperature": state["temperature"],
            "activationTime": state["activationTime"],
            "isSystemOn": state["isSystemOn"],
        }
        # Include scene_data if a scene is loaded or active
        if state["loaded_scene"] is not None or state["current_scene"] is not None:
            state_update["scene_data"] = state["scene_data"]
        await websocket.send(json.dumps(state_update))

    async def stream_logs(self, websocket):
        logging.debug("Starting log streaming")
        while True:
            if websocket.open and (state["basicLogs"] or state["advancedLogs"]):
                await websocket.send(json.dumps({
                    "type": "log_update",
                    "basicLogs": state["basicLogs"],
                    "advancedLogs": state["advancedLogs"]
                }))
                logging.debug("Logs streamed to client")
            await asyncio.sleep(1.0)

    async def broadcast_system_stats(self):
        logging.debug("Starting system stats broadcast")
        while True:
            cpu_percent, mem_percent, temperature = self.luminaire_ops.get_system_stats()
            with self.luminaire_ops._state_lock:
                state["cpu_percent"] = cpu_percent
                state["mem_percent"] = mem_percent
                state["temperature"] = temperature
            update = {
                "type": "system_stats",
                "cpu_percent": cpu_percent,
                "mem_percent": mem_percent,
                "temperature": temperature,
            }
            for client in list(clients):
                if not client.close:
                    await client.send(json.dumps(update))
                    logging.debug(f"Broadcasted stats to {client.remote_address}")
            await asyncio.sleep(1)

    async def broadcast_live_updates(self):
        logging.debug("Starting live updates broadcast")
        while True:
            if state["scheduler"]["status"] == "running":
                with self.luminaire_ops._state_lock:
                    live_update = {
                        "type": "live_update",
                        "current_cct": state["current_cct"],
                        "current_intensity": state["current_intensity"],
                        "cw": state["cw"],
                        "ww": state["ww"],
                        '''"cpu_percent": state["cpu_percent"],
                        "mem_percent": state["mem_percent"],
                        "temperature": state["temperature"],'''
                        "interval_progress": state["scheduler"]["interval_progress"]
                    }
                if clients:
                    for client in list(clients):
                        if not client.close:
                            await client.send(json.dumps(live_update))
                            logging.debug(f"Sent live update to {client.remote_address}")
            await asyncio.sleep(1)

    async def send_manual_updates(self):
        logging.debug("Starting manual updates broadcast")
        while True:
            if not state["isSystemOn"]:
                # System is off, send CW=0, WW=0 to all luminaires
                success, failed_ips = await self.luminaire_ops.sendAll(0, 0)
                if not success:
                    self.luminaire_ops.log_advanced(f"Failed to send zero values to IPs: {', '.join(failed_ips)}")
                    logging.warning(f"Failed to send zero values to IPs: {failed_ips}")
                else:
                    logging.debug("Sent CW=0, WW=0 to all luminaires (system off)")
            elif not state["auto_mode"]:
                # System is on and in manual mode, send current CW, WW, intensity
                cw, ww, intensity = state["cw"], state["ww"], state["current_intensity"]
                success, failed_ips = await self.luminaire_ops.sendAll(cw, ww)
                if not success:
                    self.luminaire_ops.log_advanced(f"Manual update failed for IPs: {', '.join(failed_ips)}")
                    logging.warning(f"Manual update failed for IPs: {failed_ips}")
                else:
                    logging.debug(f"Manual update sent - CW: {cw}, WW: {ww}")
            await asyncio.sleep(1.0)

    async def start(self):
        logging.debug(f"Starting server on {self.host}:{self.port}")
        try:
            self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
            self.running = True
            logging.info(f"Luminaire Server started on {self.host}:{self.port}")
            async with self.server:
                await self.server.serve_forever()
        except Exception as e:
            logging.error(f"Failed to start server: {e}", exc_info=True)
            await self._cleanup_server()
            raise

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info('peername')
        client_ip = addr[0] if addr else "unknown"
        logging.info(f"New luminaire connected: {client_ip}")
        with self.luminaire_ops._devices_lock:
            self.luminaire_ops.add(client_ip, writer)
        try:
            while self.running:
                data = await reader.read(1024)
                if not data:
                    break
                logging.info(f"Received from {client_ip}: {data.decode()}")
                with self.luminaire_ops._devices_lock:
                    self.luminaire_ops.processACK(client_ip, data)
        except (ConnectionError, OSError) as e:
            logging.warning(f"Luminaire {client_ip} disconnected unexpectedly: {e}")
        except Exception as e:
            logging.error(f"Unexpected error handling {client_ip}: {e}", exc_info=True)
        finally:
            with self.luminaire_ops._devices_lock:
                self.luminaire_ops.disconnect(client_ip)
            logging.debug(f"Client {client_ip} handling terminated")

    async def shutdown(self):
        logging.debug("Shutting down server")
        self.running = False
        with self.luminaire_ops._devices_lock:
            self.luminaire_ops.clearALL()
        await self._cleanup_server()
        logging.info("Luminaire Server shut down.")

    async def _cleanup_server(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        logging.debug("Server cleaned up")

    async def status_loop(self):
        logging.debug("Starting status loop")
        while True:
            with self.luminaire_ops._state_lock:
                state["connected_devices"] = self.luminaire_ops.list()
            logging.debug(f"Updated connected devices: {list(state['connected_devices'].keys())}")
            await asyncio.sleep(10.0)

def downsample_data(data, points_per_hour=6):
    """Downsample data to a specified number of points per hour, matching frontend logic."""
    logging.debug(f"Downsampling data: {len(data)} points to ~{points_per_hour * 24} points")
    downsampled = []
    step = max(1, 3600 // 10 // points_per_hour)  # Same as frontend: 3600/10/6 = 600 seconds per point
    for i in range(0, len(data), step):
        downsampled.append(data[i])
    logging.debug(f"Downsampled to {len(downsampled)} points")
    return downsampled

def load_scenes(luminaire_ops):
    logging.debug("Loading scenes")
    scene_dir = config["luminaire_operations"]["scene_directory"]
    if not os.path.exists(scene_dir):
        os.makedirs(scene_dir)
    state["available_scenes"] = [f for f in os.listdir(scene_dir) if f.endswith('.csv')]
    for scene in state["available_scenes"]:
        try:
            with open(os.path.join(scene_dir, scene), newline='') as csvfile:
                reader = csv.reader(csvfile)
                next(reader)  # Skip header
                scene_data_list = [(int(row[0].split(':')[0]) * 60 + int(row[0].split(':')[1]), float(row[1]), float(row[2])) for row in reader]
                full_cct_data = []
                full_intensity_data = []
                for i in range(len(scene_data_list)):
                    start_min, start_cct, start_intensity = scene_data_list[i]
                    end_min, end_cct, end_intensity = scene_data_list[(i + 1) % len(scene_data_list)]
                    time_diff = ((end_min - start_min + 1440) % 1440) * 60
                    cct_diff = end_cct - start_cct
                    intensity_diff = end_intensity - start_intensity
                    for j in range(1800):
                        t = j / 1799
                        interpolated_cct = start_cct + (cct_diff * t)
                        interpolated_intensity = start_intensity + (intensity_diff * t)
                        full_cct_data.append(interpolated_cct)
                        full_intensity_data.append(interpolated_intensity)
                # Downsample the data
                downsampled_cct = downsample_data(full_cct_data, points_per_hour=6)
                downsampled_intensity = downsample_data(full_intensity_data, points_per_hour=6)
                logging.debug(f"Interpolated and downsampled {scene}: {len(downsampled_cct)} CCT points, {len(downsampled_intensity)} intensity points")
                scene_data[scene] = {
                    "cct": downsampled_cct,
                    "intensity": downsampled_intensity
                }
            logging.info(f"Loaded scene {scene}.")
            logging.debug(f"Scene data for {scene}: {len(scene_data[scene]['cct'])} CCT points, {len(scene_data[scene]['intensity'])} intensity points")
        except Exception as e:
            logging.error(f"Error loading scene {scene}: {e}", exc_info=True)

async def websocket_handler(websocket, path=None):
    logging.debug(f"New WebSocket connection from {websocket.remote_address}")
    clients.add(websocket)
    logging.info(f"New WebSocket client connected from {websocket.remote_address}")
    ops.log_advanced("WebSocket connected")
    try:
        await server.emit_status_update(websocket)
        async for message in websocket:
            data = json.loads(message)
            logging.debug(f"Received message: {data}")
            action = data.get("type")
            now = datetime.datetime.now().strftime("%H:%M:%S")
            include_scene_data = False  # Flag to control when to send scene_data
            if action == "ping":
                await websocket.send(json.dumps({"type": "pong"}))
            elif action == "set_mode":
                logging.debug("Processing set_mode action: auto=%s", data["auto"])
                state_update = {}
                previous_scene = state["current_scene"]  # Preserve current scene
                state["auto_mode"] = data["auto"]
                ops.stop_event.set() if not data["auto"] else ops.stop_event.clear()
                ops.log_basic(f"Switched to {'Auto' if data['auto'] else 'Manual'} mode")
                if not data["auto"]:
                    state["scene_data"] = {"cct": [], "intensity": []}
                    state["loaded_scene"] = None
                    state["scheduler"]["status"] = "idle"
                    state_update = {
                        "auto_mode": state["auto_mode"],
                        "scene_data": state["scene_data"],
                        "current_scene": state["current_scene"],
                        "loaded_scene": state["loaded_scene"],
                        "scheduler": state["scheduler"],
                    }
                elif data["auto"] and previous_scene:
                    if previous_scene in scene_data:
                        state["current_scene"] = previous_scene
                        state["scene_data"] = scene_data[previous_scene]
                        state["activationTime"] = datetime.datetime.now().strftime("%H:%M:%S")
                        state["loaded_scene"] = previous_scene
                        state["scheduler"]["status"] = "running"
                        state_update = {
                            "auto_mode": state["auto_mode"],
                            "scene_data": state["scene_data"],
                            "current_scene": state["current_scene"],
                            "loaded_scene": state["loaded_scene"],
                            "scheduler": state["scheduler"],
                            "activationTime": state["activationTime"],
                        }
                        logging.debug("Prepared state_update with scene_data: cct_length=%s, intensity_length=%s",
                                      len(state["scene_data"]["cct"]), len(state["scene_data"]["intensity"]))
                    else:
                        logging.warning("Previous scene %s not found in scene_data", previous_scene)
                        ops.log_basic(f"Failed to reactivate scene {previous_scene}: not found")
                        state["current_scene"] = None
                        state["scene_data"] = {"cct": [], "intensity": []}
                        state_update = {
                            "auto_mode": state["auto_mode"],
                            "scene_data": state["scene_data"],
                            "current_scene": state["current_scene"],
                            "loaded_scene": state["loaded_scene"],
                            "scheduler": state["scheduler"],
                        }
                else:
                    state_update = {"auto_mode": state["auto_mode"]}
                if data["auto"] and previous_scene and previous_scene in scene_data:
                    scene_path = os.path.join(config["luminaire_operations"]["scene_directory"], previous_scene)
                    logging.debug("Reactivating scene: %s at path %s", previous_scene, scene_path)
                    try:
                        asyncio.create_task(ops.run_smooth_scheduler(scene_path))
                        ops.log_basic(f"Reactivated scene: {previous_scene}")
                    except Exception as e:
                        logging.error("Failed to reactivate scene %s: %s", previous_scene, e, exc_info=True)
                        ops.log_basic(f"Failed to reactivate scene {previous_scene}: {str(e)}")
                        state["scheduler"]["status"] = "idle"
                        state["current_scene"] = None
                        state_update = {
                            "auto_mode": state["auto_mode"],
                            "scene_data": {"cct": [], "intensity": []},
                            "current_scene": None,
                            "loaded_scene": None,
                            "scheduler": state["scheduler"],
                        }
                if state_update:
                    logging.debug("Sending state_update: %s", state_update)
                    await websocket.send(json.dumps({"state_update": state_update}))
                load_scenes(ops)
                save_state()
                logging.debug("Completed set_mode action")
            elif action == "load_scene":
                state["loaded_scene"] = data["scene"]
                if data["scene"] in scene_data:
                    state["scene_data"] = scene_data[data["scene"]]
                    include_scene_data = True  # Send scene_data for load_scene
                ops.log_basic(f"Loaded scene: {data['scene']}")
                save_state()
                logging.debug(f"Loaded scene data: cct_length=%s, intensity_length=%s",
                              len(state["scene_data"]["cct"]), len(state["scene_data"]["intensity"]))
            elif action == "activate_scene":
                    # Stop any running scene
                    if state["scheduler"]["status"] == "running" and state["current_scene"]:
                        ops.stop_scheduler()
                        await asyncio.sleep(0.1)  # Brief pause to ensure task termination
                        logging.debug(f"Stopped running scene: {state['current_scene']}")
                    
                    # Activate the new scene
                    state["current_scene"] = data["scene"]
                    state["loaded_scene"] = data["scene"]
                    if state["auto_mode"] and data["scene"] in scene_data:
                        state["scene_data"] = scene_data[data["scene"]]
                        state["activationTime"] = now
                        state["scheduler"]["status"] = "running"
                        include_scene_data = True
                        asyncio.create_task(ops.run_smooth_scheduler(os.path.join(config["luminaire_operations"]["scene_directory"], data["scene"])))
                    ops.log_basic(f"Activated scene: {data['scene']}")
                    save_state()
                    logging.debug(f"Activated scene: {data['scene']}")
            elif action == "stop_scheduler":
                ops.stop_scheduler()
                save_state()
                logging.debug("Scheduler stopped")
            elif action == "manual_override":
                state["is_manual_override"] = data.get("override", False)
                if not state["is_manual_override"] and state["auto_mode"] and state["current_scene"]:
                    ops.start_time = None
                    asyncio.create_task(ops.run_smooth_scheduler(os.path.join(config["luminaire_operations"]["scene_directory"], state["current_scene"])))
                ops.log_basic(f"Manual override {'enabled' if data.get('override', False) else 'disabled'}")
                logging.debug(f"Manual override set to {state['is_manual_override']}")
            elif action == "pause_resume":
                if data.get("pause", False):
                    ops.pause_scheduler()
                else:
                    ops.resume_scheduler()
                logging.debug(f"Scheduler {'paused' if data.get('pause', False) else 'resumed'}")
            elif action == "adjust_light":
                light_type, delta = data["light_type"], data["delta"]
                if light_type == "cw":
                    ops.adjust_cw(delta * 1.0)
                    state["cw"] = min(100, max(0, (state["cw"] or 50) + delta))
                    state["ww"] = 100 - state["cw"]
                elif light_type == "ww":
                    ops.adjust_cw(-delta * 1.0)
                    state["ww"] = min(100, max(0, (state["ww"] or 50) + delta))
                    state["cw"] = 100 - state["ww"]
                state["current_cct"] = ops.calculate_cct_from_cw_ww(state["cw"], state["ww"])
                ops.log_basic(f"Adjusted {light_type.upper()} by {delta}%")
                save_state()
                logging.debug(f"Adjusted {light_type} by {delta}%, New CW: {state['cw']}, WW: {state['ww']}")
            elif action == "sendAll":
                cw, ww, intensity = data["cw"], data["ww"], data["intensity"]
                state["cw"], state["ww"], state["current_intensity"] = cw, ww, intensity
                state["current_cct"] = ops.calculate_cct_from_cw_ww(cw, ww)
                success, failed_ips = await ops.sendAll(cw, ww)
                save_state()
                logging.debug(f"SendAll - Success: {success}, Failed IPs: {failed_ips}")
            elif action == "set_cct":
                state["current_cct"] = data["cct"]
                cw, ww = ops.calculate_cw_ww_from_cct_intensity(state["current_cct"], state["current_intensity"])
                state["cw"], state["ww"] = cw, ww
                success, failed_ips = await ops.sendAll(cw, ww)
                ops.log_basic(f"Set CCT to {data['cct']}K")
                save_state()
                logging.debug(f"Set CCT to {data['cct']}K, CW: {cw}, WW: {ww}")
            elif action == "toggle_system":
                state["isSystemOn"] = data["isSystemOn"]
                if not data["isSystemOn"]:
                    # Save current state before turning off
                    state["last_state"] = {
                        "auto_mode": state["auto_mode"],
                        "current_scene": state["current_scene"],
                        "cw": state["cw"],
                        "ww": state["ww"],
                        "current_intensity": state["current_intensity"]
                    }
                    if state["auto_mode"]:
                        ops.stop_event.set()
                        state["scheduler"]["status"] = "stopped"
                        state["current_scene"] = None
                        state["loaded_scene"] = None
                        state["scene_data"] = {"cct": [], "intensity": []}
                        ops.log_basic("Scheduler stopped due to system off")
                    success, failed_ips = await ops.sendAll(0, 0)
                    ops.log_basic("System turned OFF")
                else:
                    # Restore previous state
                    if state["last_state"]["auto_mode"] and state["last_state"]["current_scene"]:
                        state["auto_mode"] = True
                        state["current_scene"] = state["last_state"]["current_scene"]
                        state["loaded_scene"] = state["last_state"]["current_scene"]
                        state["scene_data"] = scene_data[state["current_scene"]]
                        state["activationTime"] = datetime.datetime.now().strftime("%H:%M:%S")
                        asyncio.create_task(ops.run_smooth_scheduler(os.path.join(config["luminaire_operations"]["scene_directory"], state["current_scene"])))
                        ops.log_basic(f"Restored and activated scene: {state['current_scene']}")
                    else:
                        state["cw"] = state["last_state"]["cw"]
                        state["ww"] = state["last_state"]["ww"]
                        state["current_intensity"] = state["last_state"]["current_intensity"]
                        state["current_cct"] = ops.calculate_cct_from_cw_ww(state["cw"], state["ww"])
                        success, failed_ips = await ops.sendAll(state["cw"], state["ww"])
                    ops.log_basic("System turned ON")
                save_state()  # Add this line
                logging.debug(f"System toggled to {'ON' if data['isSystemOn'] else 'OFF'}")
            # Send tailored state update
            state_update = {
                "auto_mode": state["auto_mode"],
                "available_scenes": state["available_scenes"],
                "current_scene": state["current_scene"],
                "loaded_scene": state["loaded_scene"],
                "cw": state["cw"],
                "ww": state["ww"],
                "scheduler": state["scheduler"],
                "connected_devices": state["connected_devices"],
                "basicLogs": state["basicLogs"],
                "advancedLogs": state["advancedLogs"],
                "current_cct": state["current_cct"],
                "current_intensity": state["current_intensity"],
                "is_manual_override": state["is_manual_override"],
                "cpu_percent": state["cpu_percent"],
                "mem_percent": state["mem_percent"],
                "temperature": state["temperature"],
                "activationTime": state["activationTime"],
                "isSystemOn": state["isSystemOn"],
            }
            if include_scene_data:
                state_update["scene_data"] = state["scene_data"]
                logging.debug("Sending state_update with scene_data: cct_length=%s, intensity_length=%s",
                              len(state_update["scene_data"]["cct"]), len(state_update["scene_data"]["intensity"]))
            for client in clients:
                await client.send(json.dumps(state_update))
            logging.debug("State updated and broadcasted to clients")
        asyncio.create_task(server.stream_logs(websocket))
    except Exception as e:
        ops.log_advanced(f"WebSocket handler error: {e}")
        logging.error(f"WebSocket handler error: {e}", exc_info=True)
    finally:
        clients.remove(websocket)
        ops.log_advanced("WebSocket disconnected")
        logging.info(f"WebSocket client disconnected from {websocket.remote_address}")
        logging.debug("Client removed from clients set")

async def emit_status_update(websocket):
    await websocket.send(json.dumps(state))
    logging.debug("Status update emitted")

async def main():
    global ops, server
    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft_limit < 1024:
        resource.setrlimit(resource.RLIMIT_NOFILE, (1024, hard_limit))
    ops = LuminaireOperations()
    # Load scenes before state restoration to ensure scene_data is ready
    load_scenes(ops)
    # Load persisted state if available
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "rb") as f:
                persisted_state = pickle.load(f)
                # Restore critical state
                state["auto_mode"] = persisted_state.get("auto_mode", state["auto_mode"])
                state["current_scene"] = persisted_state.get("current_scene", state["current_scene"])
                state["cw"] = persisted_state.get("cw", state["cw"])
                state["ww"] = persisted_state.get("ww", state["ww"])
                state["current_intensity"] = persisted_state.get("current_intensity", state["current_intensity"])
                state["isSystemOn"] = persisted_state.get("isSystemOn", state["isSystemOn"])
                # Update last_state for consistency
                state["last_state"] = {
                    "auto_mode": state["auto_mode"],
                    "current_scene": state["current_scene"],
                    "cw": state["cw"],
                    "ww": state["ww"],
                    "current_intensity": state["current_intensity"]
                }
                logging.debug(f"State loaded from {STATE_FILE}")
                # If system is on, restore settings
                if state["isSystemOn"]:
                    if state["auto_mode"] and state["current_scene"]:
                        # Immediately activate the previous scene
                        scene_path = os.path.join(config["luminaire_operations"]["scene_directory"], state["current_scene"])
                        state["loaded_scene"] = state["current_scene"]
                        # Populate state["scene_data"] from scene_data if scene exists
                        if state["current_scene"] in scene_data:
                            state["scene_data"] = scene_data[state["current_scene"]]
                            logging.debug(f"Restored scene_data for {state['current_scene']}: {len(state['scene_data']['cct'])} CCT points, {len(state['scene_data']['intensity'])} intensity points")
                        else:
                            state["scene_data"] = {"cct": [], "intensity": []}
                            logging.warning(f"Scene {state['current_scene']} not found in scene_data during restoration")
                        state["activationTime"] = datetime.datetime.now().strftime("%H:%M:%S")
                        state["scheduler"]["status"] = "running"
                        asyncio.create_task(ops.run_smooth_scheduler(scene_path))
                        ops.log_basic(f"Restored and activated scene after power-on: {state['current_scene']}")
                        logging.info(f"Restored and activated scene after power-on: {state['current_scene']}")
                    else:
                        # Restore manual settings
                        success, failed_ips = await ops.sendAll(state["cw"], state["ww"])
                        ops.log_basic(f"Restored manual settings after power-on: CW={state['cw']}, WW={state['ww']}, Intensity={state['current_intensity']}")
                        logging.info(f"Restored manual settings after power-on: CW={state['cw']}, WW={state['ww']}, Intensity={state['current_intensity']}")
    except Exception as e:
        logging.error(f"Failed to load state from {STATE_FILE}: {e}")
    server = LuminaireServer(luminaire_ops=ops)
    # load_scenes(ops)  # Moved above state restoration
    websocket_server = await websockets.serve(websocket_handler, config["server"]["websocket_host"], config["server"]["websocket_port"])
    print(f"WebSocket server running on ws://{config['server']['websocket_host']}:{config['server']['websocket_port']}")
    logging.info("WebSocket server started")
    asyncio.create_task(ops.cleanup_stale_devices())
    asyncio.create_task(server.broadcast_system_stats())
    asyncio.create_task(server.broadcast_live_updates())
    asyncio.create_task(server.status_loop())
    asyncio.create_task(server.send_manual_updates())
    # Broadcast state to all clients after restoration
    for client in clients:
        await server.emit_status_update(client)
    logging.debug("Broadcasted restored state to all WebSocket clients")
    await server.start()

if __name__ == "__main__":
    asyncio.run(main())
